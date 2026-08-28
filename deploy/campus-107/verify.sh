#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
processing_env=${MIRY_PROCESSING_ENV:-$script_dir/processing.env}
install_root=${MIRY_CAMPUS_ROOT:-/persistent/miry-data-collector}

for command_name in crontab sbatch flock jq ssh; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing command: $command_name" >&2
        exit 1
    fi
done

for path in "$processing_env" "$install_root/central.yaml"; do
    if [ ! -r "$path" ]; then
        echo "missing or unreadable file: $path" >&2
        exit 1
    fi
done

set -a
. "$processing_env"
set +a
: "${MIRY_APPTAINER:?MIRY_APPTAINER is required}"
: "${MIRY_DATA_IMAGE:?MIRY_DATA_IMAGE is required}"
: "${MIRY_RAW_ROOT:?MIRY_RAW_ROOT is required}"
: "${MIRY_DERIVED_ROOT:?MIRY_DERIVED_ROOT is required}"
: "${MIRY_COLLECTOR:?MIRY_COLLECTOR is required}"
: "${MIRY_L2_CONCURRENCY:?MIRY_L2_CONCURRENCY is required}"
: "${MIRY_NORMALIZE_WORKERS:?MIRY_NORMALIZE_WORKERS is required}"
: "${MIRY_PROCESSING_START_DATE:?MIRY_PROCESSING_START_DATE is required}"
: "${MIRY_SLURM_ACCOUNT:?MIRY_SLURM_ACCOUNT is required}"
: "${MIRY_SLURM_PARTITION:?MIRY_SLURM_PARTITION is required}"
: "${MIRY_SLURM_QOS:?MIRY_SLURM_QOS is required}"

for path in \
    "$script_dir/submit-ready-day.sh" \
    "$script_dir/build-l2-inputs.py" \
    "$script_dir/prune-l2-projections.py" \
    "$script_dir/processing-status.py" \
    "$script_dir/slurm/normalize.sbatch" \
    "$script_dir/slurm/l2-inputs.sbatch" \
    "$script_dir/slurm/l2.sbatch" \
    "$script_dir/slurm/finalize.sbatch"
do
    if [ ! -r "$path" ]; then
        echo "missing processing file: $path" >&2
        exit 1
    fi
done

if [ ! -x "$MIRY_APPTAINER" ]; then
    echo "missing executable Apptainer: $MIRY_APPTAINER" >&2
    exit 1
fi
if [ ! -d "$MIRY_DATA_IMAGE" ]; then
    echo "missing Apptainer sandbox: $MIRY_DATA_IMAGE" >&2
    exit 1
fi
for path in "$MIRY_RAW_ROOT" "$MIRY_DERIVED_ROOT"; do
    if [ ! -d "$path" ] || [ ! -w "$path" ]; then
        echo "directory must exist and be writable: $path" >&2
        exit 1
    fi
done

case "$MIRY_L2_CONCURRENCY" in
    *[!0-9]*|0|'')
        echo "MIRY_L2_CONCURRENCY must be a positive integer" >&2
        exit 1
        ;;
esac
if [ "$MIRY_L2_CONCURRENCY" -gt 32 ]; then
    echo "MIRY_L2_CONCURRENCY cannot exceed the 32 CPU allocation" >&2
    exit 1
fi
case "$MIRY_NORMALIZE_WORKERS" in
    *[!0-9]*|0|'')
        echo "MIRY_NORMALIZE_WORKERS must be a positive integer" >&2
        exit 1
        ;;
esac
if [ "$MIRY_NORMALIZE_WORKERS" -gt 8 ]; then
    echo "MIRY_NORMALIZE_WORKERS cannot exceed the measured 8-worker ceiling" >&2
    exit 1
fi

"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" miry-data-pull --help >/dev/null
"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" miry-data-process --help >/dev/null
"$MIRY_APPTAINER" exec --writable "$MIRY_DATA_IMAGE" rsync --version >/dev/null
sbatch --version
echo "campus-107 deployment checks passed"
