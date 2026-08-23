#!/bin/sh
#SBATCH --job-name=miry-l2
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_RAW_ROOT:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_UTC_DATE:?}"
: "${MIRY_SYMBOLS_FILE:?}"
: "${SLURM_ARRAY_TASK_ID:?}"

symbol=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$MIRY_SYMBOLS_FILE")
test -n "$symbol"

"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" miry-data-process l2 \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_UTC_DATE" \
  --symbol "$symbol"
