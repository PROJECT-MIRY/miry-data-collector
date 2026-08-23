#!/bin/sh
#SBATCH --job-name=miry-normalize
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_APPTAINER:?}"
: "${MIRY_DATA_IMAGE:?}"
: "${MIRY_RAW_ROOT:?}"
: "${MIRY_DERIVED_ROOT:?}"
: "${MIRY_COLLECTOR:?}"
: "${MIRY_UTC_DATE:?}"

"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" miry-data-process normalize \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_UTC_DATE"
