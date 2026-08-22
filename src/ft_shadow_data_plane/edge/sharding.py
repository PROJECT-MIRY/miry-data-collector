from __future__ import annotations


class StableWeightedSharder:
    """Balance symbols by observed load while preserving stable route assignments."""

    def __init__(self, count: int, weights: dict[str, int]) -> None:
        if count < 1:
            raise ValueError("shard count must be positive")
        self._count = count
        self._weights = weights
        self._assignments: dict[str, int] = {}

    def shards(self, instruments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
        shard_count = min(self._count, len(instruments))
        if shard_count == 0:
            return ()
        initial_assignment = not self._assignments
        active = set(instruments)
        self._assignments = {
            symbol: shard
            for symbol, shard in self._assignments.items()
            if symbol in active and shard < shard_count
        }
        shards: list[list[str]] = [[] for _ in range(shard_count)]
        loads = [0] * shard_count
        for symbol, shard in sorted(self._assignments.items()):
            shards[shard].append(symbol)
            loads[shard] += self._weight(symbol)
        unassigned = sorted(
            active - self._assignments.keys(),
            key=lambda symbol: (-self._weight(symbol), symbol),
        )
        for symbol in unassigned:
            shard = min(range(shard_count), key=lambda index: (loads[index], index))
            self._assignments[symbol] = shard
            shards[shard].append(symbol)
            loads[shard] += self._weight(symbol)
        if initial_assignment:
            self._improve_initial_balance(shards, loads)
            self._assignments = {
                symbol: shard for shard, symbols in enumerate(shards) for symbol in symbols
            }
        return tuple(tuple(sorted(shard)) for shard in shards)

    def _weight(self, symbol: str) -> int:
        if symbol in self._weights:
            return self._weights[symbol]
        if self._weights:
            ordered = sorted(self._weights.values())
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
                                - self._weight(left_symbol)
                                + self._weight(right_symbol)
                            )
                            next_right = (
                                loads[right]
                                - self._weight(right_symbol)
                                + self._weight(left_symbol)
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
