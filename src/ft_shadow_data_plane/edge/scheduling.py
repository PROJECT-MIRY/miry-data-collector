from __future__ import annotations


def advance_fixed_deadline(previous: float, interval: float, now: float) -> float:
    deadline = previous + interval
    if deadline <= now:
        missed = int((now - deadline) // interval) + 1
        deadline += missed * interval
    return deadline


def staggered_offsets(count: int, window_seconds: float) -> tuple[float, ...]:
    if count < 0:
        raise ValueError("poll count cannot be negative")
    if window_seconds < 0:
        raise ValueError("poll window cannot be negative")
    if count == 0:
        return ()
    return tuple(index * window_seconds / count for index in range(count))
