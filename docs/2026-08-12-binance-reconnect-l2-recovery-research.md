# Binance USD-M 重连与 L2 恢复调研（2026-08-12）

Research date: 2026-08-12 (Asia/Shanghai). Scope is limited to Binance first-party documentation,
the public USD-M Futures API, and Binance's official connector source. Repository observations are
against the committed `v0.3.1` tag; they are identified separately from Binance's contract.

## Conclusion

The observed approximately 63-second `public-1` interval is not evidence that opening a replacement
WebSocket takes 63 seconds. In `v0.3.1`, the route waits for all 32 serial REST order-book snapshots
before it becomes ready and closes the connection-wide gap. With a one-second reconnect delay and a
two-second shared snapshot interval, the deterministic part of that interval is approximately
`1 + (32 - 1) * 2 = 63` seconds, plus request latency. The matching observed durations are therefore
explained by the local readiness policy.

Binance's documentation does not state why a TCP connection may disappear without a WebSocket close
frame. It documents planned 24-hour expiry, ping/pong requirements, and control-message limits, but
does not authorize attributing an unframed close to Binance, the network path, or the client. The
error must remain classified as an abnormal transport loss unless additional packet or host evidence
identifies the cause.

The safe optimization is to separate two facts that currently share one timer:

1. **Transport recovery:** the replacement subscription is acknowledged and fresh events are again
   observed. This closes the raw transport-loss interval.
2. **Per-symbol L2 re-anchor:** buffered diff events bridge a successful REST snapshot and continue
   with valid update IDs. Only that symbol's derived L2 may become `VALID`.

Snapshot fetching can be made much faster with a shared, weight-aware limiter and small bounded
concurrency. It must not weaken the snapshot bridge, `pu` sequence check, or explicit gap accounting.

## Official constraints

### WebSocket lifecycle and control traffic

Binance documents the following for USD-M Futures market streams:

- one connection is valid for 24 hours, so clients must expect disconnection at that boundary;
- the server sends a ping frame every three minutes and disconnects a connection if it receives no
  pong within ten minutes;
- unsolicited pong frames are allowed;
- a connection accepts at most ten incoming messages per second, and repeated excess can lead to an
  IP ban;
- one connection can listen to at most 1,024 streams;
- a single JSON `SUBSCRIBE` request can carry multiple stream names and returns an acknowledgement
  with the request `id`.

The official Python connector provides useful first-party implementation precedent, but not an
additional exchange guarantee: it schedules renewal every 23 hours and replies to a server ping with
the same payload. This agrees with the repository's pre-expiry rotation and protocol-level pong
handling.

For the current non-D0 public routes, 28 and 32 symbols require 56 and 64 subscriptions respectively
(`bookTicker` and `depth@100ms` per symbol). Both are far below the 1,024-stream ceiling. A batched
initial `SUBSCRIBE`, one 60-second subscription audit, and a client ping every 20 seconds are also far
below ten client-to-server messages per second. The documented control limits do not explain the two
abnormal closes.

### Authoritative local order-book recovery

Binance's required sequence is:

1. open the diff-depth stream and buffer its events;
2. obtain `GET /fapi/v1/depth?symbol=...&limit=1000`;
3. discard buffered events whose `u` is below the snapshot `lastUpdateId`;
4. require the first retained event to cover the snapshot update ID (`U <= lastUpdateId` and
   `u >= lastUpdateId`);
5. thereafter require each event's `pu` to equal the previous event's `u`;
6. if that sequence condition fails, restart from the REST snapshot step.

Consequently, a reconnect cannot inherit `VALID` merely because a new socket is connected or a
subscription is acknowledged. Events may be captured immediately, but the L2 state remains invalid
for each symbol until its own snapshot bridge succeeds. A failed or timed-out snapshot leaves that
symbol unanchored and must not make the route fully snapshot-ready.

### REST weight and IP semantics

Binance's official connector records these weights for `GET /fapi/v1/depth`:

| `limit` | request weight |
| ---: | ---: |
| 5, 10, 20, 50 | 2 |
| 100 | 5 |
| 500 | 10 |
| 1,000 | 20 |

The USD-M general information page says that response headers expose
`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` for the current IP, limits apply to IP addresses
rather than API keys, `429` requires the client to back off, and repeated violations or failure to
back off can produce a `418` IP ban. It directs clients to the `rateLimits` array in
`GET /fapi/v1/exchangeInfo` for the current limits.

On 2026-08-12, the first-party production endpoint reported
`REQUEST_WEIGHT / MINUTE / 1 / 2400`. This value is dynamic API evidence, not a permanent constant;
the collector must not compile it as an immutable Binance promise.

## Quantitative impact on this repository

The calculations below assume D0 is disabled, as in the formal v0.3.1 deployment. D0 would add RPI
snapshot work and must be budgeted separately.

| scope | symbols | snapshots | weight | last serial start at 2 s | observed-style total with 1 s reconnect wait |
| --- | ---: | ---: | ---: | ---: | ---: |
| `public-0` | 28 | 28 | 560 | 54 s | about 55 s plus HTTP latency |
| `public-1` | 32 | 32 | 640 | 62 s | about 63 s plus HTTP latency |
| both shards | 60 | 60 | 1,200 | 118 s if globally serialized | about 119 s plus HTTP latency |

The current shared two-second lock issues at most 30 limit-1,000 snapshots per minute, or 600 weight
per minute when continuously occupied. That is conservative against the observed 2,400/minute
limit, but it turns rate-limit headroom into avoidable L2 invalid time. Because the REST client is
shared by both public shards, coincident recovery or rotation also makes one shard wait behind the
other.

Setting the interval to 0.5 seconds without a real weight controller would consume the entire
observed 2,400/minute allowance under sustained work and leave no capacity for retries, discovery,
open-interest requests, or a changed exchange limit. A shorter fixed sleep alone is therefore not a
robust solution.

## Recommended recovery design

### 1. Keep transport and L2 readiness separate

- Open one explicit transport gap at the last trustworthy receive time when a route fails.
- Start receiving and durably ingesting replacement WebSocket events immediately.
- Close the transport gap only after the initial subscription acknowledgement and fresh events prove
  the expected route is flowing again. Record socket-open, subscription-ack, first-fresh-event, and
  transport-recovered timestamps separately.
- Do not keep the raw transport gap open merely because later symbols are still waiting for REST
  snapshots. Instead, retain per-symbol L2 invalidity until each snapshot event bridges the buffered
  diff sequence.
- Continue the existing `pu == previous u` check. Any mismatch creates an explicit sequence gap and
  requests another snapshot; it cannot inherit `VALID` across that boundary.

This distinction preserves honesty: some replacement raw events may exist during re-anchoring, while
the derived order book is still not trustworthy.

### 2. Replace fixed global serialization with a shared weighted limiter

Use one limiter for all Binance REST work from the collector, with endpoint weights rather than a
snapshot-only sleep. A conservative repository policy is:

- discover the current request limit from `exchangeInfo` and monitor the used-weight response
  header;
- reserve at least half of the observed minute budget for non-snapshot work and retries, making
  1,200 weight/minute the initial snapshot ceiling while the exchange reports 2,400;
- cap snapshot concurrency at four on the 1-core/1-GiB host;
- count every limit-1,000 snapshot attempt as weight 20 before dispatch;
- stop dispatch and back off on `429`; never spin-retry; treat `418` as an operational incident;
- cancel or supersede obsolete snapshot work after another reconnect so stale work cannot delay the
  current connection.

The 32-symbol shard costs only 640 weight and the full 60-symbol universe costs 1,200, so a bounded
burst fits that repository budget once. The limiter must still prevent repeated reconnects and
retries from spending the same minute's budget again. Four concurrent responses and streaming each
payload directly into the existing ingest path bound transient tasks and payloads; an unbounded
32-request fan-out is unnecessary on 1C1G.

### 3. Recover symbols independently

- Start a bounded task per pending symbol instead of awaiting the shard in symbol order.
- Mark a symbol's snapshot as captured as soon as that observation is durably ingested; do not wait
  for the last symbol in the shard. This edge milestone is not L2 re-anchoring: only the central
  `U/u/pu` bridge may mark that symbol `VALID`.
- Route-level “snapshot ready” remains the conjunction of all required snapshot-captured
  milestones, but it is an operational readiness metric, not the end of the raw transport gap and
  not proof that central reconstruction has bridged every symbol.
- Expose per-symbol snapshot request/response/bridge latency and the pending count. This makes a slow
  or repeatedly failing symbol visible without inflating every other symbol's gap.

## Integrity invariants and acceptance checks

The optimization is acceptable only if all of the following hold in tests and deployment telemetry:

- WebSocket events are buffered before their corresponding snapshot request begins.
- No symbol becomes L2 `VALID` until the official snapshot bridge conditions pass.
- `pu` discontinuity after a bridge immediately invalidates that symbol and triggers re-anchoring.
- Snapshot HTTP failure, cancellation, timeout, `429`, or collector restart cannot close L2
  invalidity.
- A raw transport gap ends from transport evidence, not from the last REST response; derived L2
  validity begins independently per symbol.
- The gap ledger contains no `VALID` overlap with transport or sequence gaps.
- Reconnect tests cover 28 and 32 symbols, coincident shard recovery, a snapshot that repeatedly
  fails, `429` backoff, a second disconnect during recovery, and cancellation of stale snapshots.
- A 1C1G soak records RSS, event-loop lag, ingest queue ratio, snapshot pending count, used IP
  weight, transport-recovery latency, and per-symbol re-anchor latency.

## v0.3.2 implementation decision

v0.3.2 applies the low-risk portion of this design without increasing REST request weight:

- transport-ready now requires the initial subscription acknowledgement plus one fresh event for
  every configured liveness `(stream, symbol)` key;
- the route transport gap closes at that milestone and logs transport recovery latency;
- route snapshot-ready remains separate and logs the remaining re-anchor latency;
- central L2 continues to invalidate on transport gap OPEN and cannot return to `VALID` before that
  symbol's snapshot bridge succeeds;
- the formal public routes increase from two shards `[28, 32]` to four shards
  `[14, 18, 14, 14]`, reducing failure scope while preserving total streams and event volume;
- the two-second shared snapshot interval remains unchanged at no more than 600 snapshot
  weight/minute.

A shared header-aware weighted limiter and bounded concurrent snapshots remain future work. They are
not required to correct the false 63-second transport classification, and deploying them without
the documented `429`/`418` behavior would add unnecessary production risk.

## What the sources do not establish

- Binance does not document a cause for `no close frame received or sent`; the two incidents cannot
  be labelled a Binance outage from this message alone.
- The 2,400/minute exchange-info value is current observed state and may change.
- Binance does not promise a reconnect latency or a snapshot response latency.
- The suggested 50% reserve, concurrency of four, and separation of operational metrics are local
  engineering policies, not Binance requirements.

## First-party sources

All sources were accessed on 2026-08-12.

1. Binance, [USD-M Futures WebSocket Market Streams: Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect).
2. Binance, [Live Subscribing/Unsubscribing to streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams).
3. Binance, [How to manage a local order book correctly](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
4. Binance, [USD-M Futures General Info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info).
5. Binance, [USD-M Futures REST market data: Order Book](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#order-book).
6. Binance production API, [`GET /fapi/v1/exchangeInfo`](https://fapi.binance.com/fapi/v1/exchangeInfo).
7. Binance official Python connector at commit
   [`65ba6aef`: depth weights](https://github.com/binance/binance-connector-python/blob/65ba6aef60f9d6ae4010c173184c856a24c0763b/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/rest_api/api/market_data_api.py#L1079-L1132),
   [23-hour renewal interval](https://github.com/binance/binance-connector-python/blob/65ba6aef60f9d6ae4010c173184c856a24c0763b/common/src/binance_common/constants.py#L16-L20),
   [scheduled reconnect](https://github.com/binance/binance-connector-python/blob/65ba6aef60f9d6ae4010c173184c856a24c0763b/common/src/binance_common/websocket.py#L265-L270), and
   [ping payload pong](https://github.com/binance/binance-connector-python/blob/65ba6aef60f9d6ae4010c173184c856a24c0763b/common/src/binance_common/websocket.py#L377-L380).
