#!/bin/sh
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:30:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_RANGE_APPTAINER:?}"
: "${MIRY_RANGE_IMAGE:?}"
: "${MIRY_RANGE_PROCESS_COMMAND:?}"
: "${MIRY_RANGE_RAW_ROOT:?}"
: "${MIRY_RANGE_DERIVED_ROOT:?}"
: "${MIRY_RANGE_COLLECTOR:?}"
: "${MIRY_RANGE_DATE:?}"

marker="$MIRY_RANGE_DERIVED_ROOT/typed/collector=$MIRY_RANGE_COLLECTOR/date=$MIRY_RANGE_DATE/_NORMALIZED.json"
symbols=$(jq -jer \
  '.expected_symbols | if type == "array" and length == 60 and (unique | length) == 60 then join(",") else error("invalid expected_symbols") end' \
  "$marker")

"$MIRY_RANGE_APPTAINER" exec --writable "$MIRY_RANGE_IMAGE" \
  "$MIRY_RANGE_PROCESS_COMMAND" finalize \
  --raw-root "$MIRY_RANGE_RAW_ROOT" \
  --derived-root "$MIRY_RANGE_DERIVED_ROOT" \
  --collector "$MIRY_RANGE_COLLECTOR" \
  --date "$MIRY_RANGE_DATE" \
  --symbols "$symbols"
