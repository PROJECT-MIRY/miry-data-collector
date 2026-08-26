from miry.orderbook.bridge import (
    BridgeStatus,
    SnapshotBridgeTracker,
    UpdateSpan,
    locate_snapshot_bridge,
)


def _span(sequence: int, first: int, final: int, previous: int) -> UpdateSpan:
    return UpdateSpan(
        receive_seq=sequence,
        first_update_id=first,
        final_update_id=final,
        previous_final_update_id=previous,
    )


def test_captured_apt_snapshot_requires_an_official_overlap() -> None:
    tracker = SnapshotBridgeTracker()

    waiting = tracker.on_snapshot(11379248797922)
    stale = tracker.on_diff(_span(29, 11379248808730, 11379248809261, 11379248797922))

    assert waiting.status is BridgeStatus.WAITING_FOR_EVENTS
    assert stale.status is BridgeStatus.STALE_SNAPSHOT
    assert not tracker.is_bridged

    bridged = tracker.on_snapshot(11379248809000)

    assert bridged.status is BridgeStatus.BRIDGED
    assert bridged.bridge_index == 0
    assert tracker.is_bridged
    assert tracker.previous_update_id == 11379248809261


def test_captured_crv_snapshot_is_older_than_the_entire_diff_buffer() -> None:
    spans = (
        _span(15750, 11380357872292, 11380357884118, 11380357868810),
        _span(16267, 11380357889414, 11380357919477, 11380357884118),
        _span(16632, 11380357933479, 11380357942179, 11380357919477),
    )

    stale = locate_snapshot_bridge(11380356522031, spans)
    bridged = locate_snapshot_bridge(11380357880000, spans)

    assert stale.status is BridgeStatus.STALE_SNAPSHOT
    assert bridged.status is BridgeStatus.BRIDGED
    assert bridged.bridge_index == 0


def test_snapshot_ahead_of_stream_waits_and_then_bridges() -> None:
    tracker = SnapshotBridgeTracker()
    tracker.on_diff(_span(1, 90, 99, 89))

    waiting = tracker.on_snapshot(105)
    bridged = tracker.on_diff(_span(2, 100, 110, 99))

    assert waiting.status is BridgeStatus.WAITING_FOR_EVENTS
    assert bridged.status is BridgeStatus.BRIDGED
    assert tracker.previous_update_id == 110


def test_pu_discontinuity_invalidates_a_bridged_sequence() -> None:
    tracker = SnapshotBridgeTracker()
    tracker.on_diff(_span(1, 100, 105, 99))
    assert tracker.on_snapshot(102).status is BridgeStatus.BRIDGED

    gap = tracker.on_diff(_span(2, 106, 110, 104))

    assert gap.status is BridgeStatus.SEQUENCE_GAP
    assert gap.expected_previous_update_id == 105
    assert gap.received_previous_update_id == 104
    assert not tracker.is_bridged
