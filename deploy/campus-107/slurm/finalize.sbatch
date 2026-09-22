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
: "${MIRY_PROCESSING_DATE:?}"
: "${MIRY_PROCESS_COMMAND:?}"

marker="$MIRY_DERIVED_ROOT/typed/collector=$MIRY_COLLECTOR/date=$MIRY_PROCESSING_DATE/_NORMALIZED.json"
symbols=$(jq -jer \
  '.expected_symbols | if type == "array" and length == 60 and (unique | length) == 60 then join(",") else error("invalid expected_symbols") end' \
  "$marker")

set +e
"$MIRY_APPTAINER" exec --env PYTHONNOUSERSITE=1 --env PYTHONPATH= --writable "$MIRY_DATA_IMAGE" \
  "$MIRY_PROCESS_COMMAND" finalize \
  --raw-root "$MIRY_RAW_ROOT" \
  --derived-root "$MIRY_DERIVED_ROOT" \
  --collector "$MIRY_COLLECTOR" \
  --date "$MIRY_PROCESSING_DATE" \
  --symbols "$symbols"
status=$?
set -e

quality_root="$MIRY_DERIVED_ROOT/quality/collector=$MIRY_COLLECTOR/date=$MIRY_PROCESSING_DATE"
if [ "$status" -ne 0 ] && [ ! -s "$quality_root/_QUALITY_REJECTED.json" ]; then
  exit "$status"
fi
if [ "$status" -ne 0 ]; then
  echo "QUALITY_REJECTED date=$MIRY_PROCESSING_DATE marker=$quality_root/_QUALITY_REJECTED.json"
fi
