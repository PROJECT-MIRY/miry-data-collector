#!/bin/sh
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_RAW_ROOT:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_PROCESSING_DATE:?}"
: "${MIRY_PROCESS_COMMAND:?}"
: "${SLURM_ARRAY_TASK_ID:?}"

input_root="$MIRY_DERIVED_ROOT/l2-symbol-projections/collector=$MIRY_COLLECTOR/date=$MIRY_PROCESSING_DATE"
symbol=$(jq -er --argjson index "$SLURM_ARRAY_TASK_ID" \
  '.schedule | if type == "array" and length == 60 and (unique | length) == 60 then .[$index] else error("invalid L2 schedule") end' \
  "$input_root/_L2_SYMBOL_PROJECTION.json")
test -s "$input_root/symbol=$symbol.parquet"
checkpoint="$MIRY_DERIVED_ROOT/quality/collector=$MIRY_COLLECTOR/date=$MIRY_PROCESSING_DATE/symbol=$symbol/l2-checkpoint.json"
if [ -s "$checkpoint" ]; then
  echo "L2 checkpoint already complete date=$MIRY_PROCESSING_DATE symbol=$symbol"
  exit 0
fi

exec "$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" \
  "$MIRY_PROCESS_COMMAND" l2 \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_PROCESSING_DATE" \
  --symbol "$symbol"
