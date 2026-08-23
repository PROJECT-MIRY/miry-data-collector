#!/bin/sh
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --mail-type=FAIL

set -eu
: "${MIRY_RANGE_DERIVED_ROOT:?}"
: "${MIRY_RANGE_COLLECTOR:?}"
: "${MIRY_RANGE_DATE:?}"
: "${MIRY_RANGE_BOUNDARY_CANDIDATE:?}"

actual="$MIRY_RANGE_DERIVED_ROOT/typed/collector=$MIRY_RANGE_COLLECTOR/date=$MIRY_RANGE_DATE/_DEDUP_CHECKPOINT.json"
cmp "$MIRY_RANGE_BOUNDARY_CANDIDATE" "$actual"
sha256sum "$actual"
