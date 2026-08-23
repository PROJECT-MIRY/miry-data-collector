from __future__ import annotations


class TrafficSharder:
    """Balance symbols by message rate while preserving existing route assignments."""

    def __init__(self, count: int, message_rates: dict[str, int]) -> None:
        if count < 1:
            raise ValueError("shard count must be positive")
        self._count = count
        self._message_rates = message_rates
        self._assignments: dict[str, int] = {}

    def shards(self, instruments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
        shard_count = min(self._count, len(instruments))
        if shard_count == 0:
            return ()
        initial_assignment = not self._assignments
        active = set(instruments)
        max_symbols = (len(active) + shard_count - 1) // shard_count + 1
        self._assignments = {
            symbol: shard
            for symbol, shard in self._assignments.items()
            if symbol in active and shard < shard_count
        }
        shards: list[list[str]] = [[] for _ in range(shard_count)]
        loads = [0] * shard_count
        for symbol, shard in sorted(self._assignments.items()):
            shards[shard].append(symbol)
            loads[shard] += self._message_rate(symbol)
        unassigned = sorted(
            active - self._assignments.keys(),
            key=lambda symbol: (-self._message_rate(symbol), symbol),
        )
        for symbol in unassigned:
            available = [
                index for index in range(shard_count) if len(shards[index]) < max_symbols
            ]
            shard = min(available, key=lambda index: (loads[index], index))
            self._assignments[symbol] = shard
            shards[shard].append(symbol)
            loads[shard] += self._message_rate(symbol)
        if initial_assignment:
            self._improve_initial_balance(shards, loads)
            self._assignments = {
                symbol: shard for shard, symbols in enumerate(shards) for symbol in symbols
            }
        return tuple(tuple(sorted(shard)) for shard in shards)

    def _message_rate(self, symbol: str) -> int:
        if symbol in self._message_rates:
            return self._message_rates[symbol]
        if self._message_rates:
            ordered = sorted(self._message_rates.values())
            return ordered[len(ordered) // 2]
        return 1

    def _improve_initial_balance(self, shards: list[list[str]], loads: list[int]) -> None:
        while True:
            current_spread = max(loads) - min(loads)
            best: tuple[int, int, str, str, int, int] | None = None
            best_spread = current_spread
            for left in range(len(shards)):
                for right in range(left + 1, len(shards)):
                    for left_symbol in shards[left]:
                        for right_symbol in shards[right]:
                            next_left = (
                                loads[left]
                                - self._message_rate(left_symbol)
                                + self._message_rate(right_symbol)
                            )
                            next_right = (
                                loads[right]
                                - self._message_rate(right_symbol)
                                + self._message_rate(left_symbol)
                            )
                            next_loads = [*loads]
                            next_loads[left] = next_left
                            next_loads[right] = next_right
                            spread = max(next_loads) - min(next_loads)
                            if spread < best_spread:
                                best_spread = spread
                                best = (
                                    left,
                                    right,
                                    left_symbol,
                                    right_symbol,
                                    next_left,
                                    next_right,
                                )
            if best is None:
                return
            left, right, left_symbol, right_symbol, next_left, next_right = best
            shards[left].remove(left_symbol)
            shards[left].append(right_symbol)
            shards[right].remove(right_symbol)
            shards[right].append(left_symbol)
            loads[left] = next_left
            loads[right] = next_right
