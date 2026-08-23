#!/bin/sh
#SBATCH --job-name=miry-finalize
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:30:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_RAW_ROOT:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_UTC_DATE:?}"
: "${MIRY_SYMBOLS:?}"

"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" miry-data-process finalize \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_UTC_DATE" \
  --symbols "$MIRY_SYMBOLS"
