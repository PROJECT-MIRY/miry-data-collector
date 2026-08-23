#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 YYYY-MM-DD /shared/path/to/symbols.txt" >&2
    exit 2
fi

utc_date=$1
symbols_file=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
processing_env=${MIRY_PROCESSING_ENV:-$script_dir/processing.env}

normalized_date=$(date -u -d "$utc_date" +%F 2>/dev/null) || {
    echo "invalid UTC date: $utc_date" >&2
    exit 2
}
if [ "$normalized_date" != "$utc_date" ]; then
    echo "UTC date must use YYYY-MM-DD: $utc_date" >&2
    exit 2
fi
if [ ! -r "$processing_env" ]; then
    echo "missing processing environment: $processing_env" >&2
    exit 1
fi
if [ ! -r "$symbols_file" ]; then
    echo "symbols file is not readable: $symbols_file" >&2
    exit 1
fi

set -a
. "$processing_env"
set +a
: "${MIRY_APPTAINER:?MIRY_APPTAINER is required}"
: "${MIRY_DATA_IMAGE:?MIRY_DATA_IMAGE is required}"
: "${MIRY_RAW_ROOT:?MIRY_RAW_ROOT is required}"
: "${MIRY_DERIVED_ROOT:?MIRY_DERIVED_ROOT is required}"
: "${MIRY_COLLECTOR:?MIRY_COLLECTOR is required}"
: "${MIRY_L2_CONCURRENCY:?MIRY_L2_CONCURRENCY is required}"

canonical_root=${MIRY_SYMBOLS_ROOT:-$(readlink -f "$script_dir/../..")/symbols}
mkdir -p "$canonical_root"
canonical_symbols="$canonical_root/$utc_date.canonical.txt"
canonical_partial=$(mktemp "$canonical_symbols.XXXXXX")
trap 'rm -f "$canonical_partial"' EXIT HUP INT TERM
"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" \
    miry-data-symbols \
    --input "$symbols_file" \
    --count 60 \
    > "$canonical_partial"
mv "$canonical_partial" "$canonical_symbols"
trap - EXIT HUP INT TERM
symbols_file=$canonical_symbols

previous_date=$(date -u -d "$utc_date -1 day" +%F)
previous_raw="$MIRY_RAW_ROOT/collector=$MIRY_COLLECTOR/day-manifests/date=$previous_date/SEALED.json"
previous_processed="$MIRY_DERIVED_ROOT/quality/collector=$MIRY_COLLECTOR/date=$previous_date/_PROCESSED.json"
previous_rejected="$MIRY_DERIVED_ROOT/quality/collector=$MIRY_COLLECTOR/date=$previous_date/_QUALITY_REJECTED.json"
if [ -e "$previous_raw" ] && [ ! -s "$previous_processed" ] && [ ! -s "$previous_rejected" ]; then
    echo "previous UTC day has no terminal quality result: $previous_date" >&2
    exit 1
fi

case "$MIRY_L2_CONCURRENCY" in
    *[!0-9]*|0|'')
        echo "MIRY_L2_CONCURRENCY must be a positive integer" >&2
        exit 1
        ;;
esac

if ! awk 'NF != 1 || seen[$0]++ { exit 1 } END { if (NR != 60) exit 1 }' \
    "$symbols_file"
then
    echo "symbols must contain exactly 60 unique canonical Binance symbols, one per line" >&2
    exit 1
fi

symbols_file=$(readlink -f "$symbols_file")
symbol_count=$(awk 'END { print NR }' "$symbols_file")
array_max=$((symbol_count - 1))
symbols=$(awk 'BEGIN { separator = "" } { printf "%s%s", separator, $1; separator = "," }' \
    "$symbols_file")

export MIRY_UTC_DATE=$utc_date
export MIRY_SYMBOLS_FILE=$symbols_file
export MIRY_SYMBOLS=$symbols

normalize_result=$(sbatch --parsable "$script_dir/slurm/normalize.sbatch")
normalize_job=${normalize_result%%;*}
l2_result=$(sbatch \
    --parsable \
    --dependency="afterok:$normalize_job" \
    --array="0-$array_max%$MIRY_L2_CONCURRENCY" \
    "$script_dir/slurm/l2-array.sbatch")
l2_job=${l2_result%%;*}
finalize_result=$(sbatch \
    --parsable \
    --dependency="afterok:$l2_job" \
    "$script_dir/slurm/finalize.sbatch")
finalize_job=${finalize_result%%;*}

echo "normalize_job=$normalize_job"
echo "l2_job=$l2_job"
echo "finalize_job=$finalize_job"
