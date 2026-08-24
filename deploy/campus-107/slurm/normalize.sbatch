#!/bin/sh
#SBATCH --cpus-per-task=5
#SBATCH --mem=24G
#SBATCH --time=03:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_RAW_ROOT:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_PROCESSING_DATE:?}"
: "${MIRY_PROCESS_COMMAND:?}"
: "${MIRY_NORMALIZE_WORKERS:?}"

"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" \
  "$MIRY_PROCESS_COMMAND" normalize \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_PROCESSING_DATE" \
  --max-workers "$MIRY_NORMALIZE_WORKERS"
