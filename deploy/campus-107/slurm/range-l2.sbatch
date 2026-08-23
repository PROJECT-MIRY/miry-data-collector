#!/bin/sh
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_RANGE_APPTAINER:?}"
: "${MIRY_RANGE_IMAGE:?}"
: "${MIRY_RANGE_PROCESS_COMMAND:?}"
: "${MIRY_RANGE_RAW_ROOT:?}"
: "${MIRY_RANGE_DERIVED_ROOT:?}"
: "${MIRY_RANGE_COLLECTOR:?}"
: "${MIRY_RANGE_DATE:?}"
: "${MIRY_RANGE_SYMBOLS_FILE:?}"
: "${SLURM_ARRAY_TASK_ID:?}"

symbol=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$MIRY_RANGE_SYMBOLS_FILE")
test -n "$symbol"

"$MIRY_RANGE_APPTAINER" exec --writable "$MIRY_RANGE_IMAGE" \
  "$MIRY_RANGE_PROCESS_COMMAND" l2 \
  --raw-root "$MIRY_RANGE_RAW_ROOT" \
  --derived-root "$MIRY_RANGE_DERIVED_ROOT" \
  --collector "$MIRY_RANGE_COLLECTOR" \
  --date "$MIRY_RANGE_DATE" \
  --symbol "$symbol"
