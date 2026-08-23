from __future__ import annotations

import argparse
from pathlib import Path

from miry.contracts.symbols import validate_symbols


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate canonical Binance symbols")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--count", type=int)
    args = parser.parse_args()
    symbols = validate_symbols(
        tuple(args.input.read_text(encoding="utf-8").splitlines()), count=args.count
    )
    if symbols:
        print("\n".join(symbols))


if __name__ == "__main__":
    main()
