#!/bin/sh
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=06:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_RANGE_APPTAINER:?}"
: "${MIRY_RANGE_IMAGE:?}"
: "${MIRY_RANGE_PROCESS_COMMAND:?}"
: "${MIRY_RANGE_RAW_ROOT:?}"
: "${MIRY_RANGE_DERIVED_ROOT:?}"
: "${MIRY_RANGE_COLLECTOR:?}"
: "${MIRY_RANGE_DATE:?}"
: "${SLURM_ARRAY_TASK_ID:?}"

marker="$MIRY_RANGE_DERIVED_ROOT/typed/collector=$MIRY_RANGE_COLLECTOR/date=$MIRY_RANGE_DATE/_NORMALIZED.json"
symbol=$(jq -er --argjson index "$SLURM_ARRAY_TASK_ID" \
  '.expected_symbols | if type == "array" and length == 60 and (unique | length) == 60 then .[$index] else error("invalid expected_symbols") end' \
  "$marker")

if [ -n "${MIRY_RANGE_L2_RUNNER:-}" ]; then
  : "${MIRY_RANGE_L2_MODE:?}"
  exec "$MIRY_RANGE_APPTAINER" exec --writable "$MIRY_RANGE_IMAGE" \
    python "$MIRY_RANGE_L2_RUNNER" \
    --mode "$MIRY_RANGE_L2_MODE" \
    --derived-root "$MIRY_RANGE_DERIVED_ROOT" \
    --collector "$MIRY_RANGE_COLLECTOR" \
    --date "$MIRY_RANGE_DATE" \
    --symbol "$symbol"
fi

"$MIRY_RANGE_APPTAINER" exec --writable "$MIRY_RANGE_IMAGE" \
  "$MIRY_RANGE_PROCESS_COMMAND" l2 \
  --raw-root "$MIRY_RANGE_RAW_ROOT" \
  --derived-root "$MIRY_RANGE_DERIVED_ROOT" \
  --collector "$MIRY_RANGE_COLLECTOR" \
  --date "$MIRY_RANGE_DATE" \
  --symbol "$symbol"
