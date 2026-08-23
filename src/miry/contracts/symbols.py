from __future__ import annotations

import unicodedata

MAX_SYMBOL_LENGTH = 30


def is_exchange_symbol(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= MAX_SYMBOL_LENGTH
        and value == value.strip()
        and value == unicodedata.normalize("NFC", value)
        and value == value.upper()
        and value.isalnum()
    )


def validate_exchange_symbol(value: str) -> str:
    if not is_exchange_symbol(value):
        raise ValueError(f"invalid canonical exchange symbol: {value!r}")
    return value


def validate_symbols(values: tuple[str, ...], *, count: int | None = None) -> tuple[str, ...]:
    validated = []
    for raw in values:
        if not raw:
            continue
        validated.append(validate_exchange_symbol(raw))
    result = tuple(validated)
    if len(result) != len(set(result)):
        raise ValueError("symbol list contains duplicates")
    if count is not None and len(result) != count:
        raise ValueError(f"symbol list must contain exactly {count} values")
    return result
