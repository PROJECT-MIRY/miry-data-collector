#!/bin/sh
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=02:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_PROCESSING_DATE:?}"
: "${MIRY_L2_INPUT_BUILDER:?}"

exec "$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" \
  python "$MIRY_L2_INPUT_BUILDER" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_PROCESSING_DATE"
