#!/bin/sh
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_RANGE_DERIVED_ROOT:?}"
: "${MIRY_RANGE_COLLECTOR:?}"
: "${MIRY_RANGE_DATE:?}"
: "${MIRY_RANGE_SYMBOLS_FILE:?}"

marker="$MIRY_RANGE_DERIVED_ROOT/typed/collector=$MIRY_RANGE_COLLECTOR/date=$MIRY_RANGE_DATE/_NORMALIZED.json"
partial="$MIRY_RANGE_SYMBOLS_FILE.$SLURM_JOB_ID.partial"
mkdir -p "$(dirname "$MIRY_RANGE_SYMBOLS_FILE")"
jq -er '
  .expected_symbols
  | if type == "array" and length == 60 and (unique | length) == 60
    then .[]
    else error("normalized marker has no authoritative 60-symbol universe")
    end
' "$marker" > "$partial"
mv "$partial" "$MIRY_RANGE_SYMBOLS_FILE"
