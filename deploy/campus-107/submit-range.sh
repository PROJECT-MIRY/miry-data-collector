#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 START_DATE END_DATE" >&2
    exit 2
fi

start_date=$1
end_date=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

for name in \
    MIRY_RANGE_APPTAINER MIRY_RANGE_IMAGE MIRY_RANGE_PROCESS_COMMAND \
    MIRY_RANGE_RAW_ROOT MIRY_RANGE_DERIVED_ROOT MIRY_RANGE_COLLECTOR \
    MIRY_RANGE_SYMBOLS_ROOT MIRY_RANGE_LOG_ROOT MIRY_RANGE_ACCOUNT \
    MIRY_RANGE_PARTITION MIRY_RANGE_QOS
do
    eval "value=\${$name-}"
    if [ -z "$value" ]; then
        echo "$name is required" >&2
        exit 1
    fi
done

concurrency=${MIRY_RANGE_L2_CONCURRENCY:-16}
case "$concurrency" in
    *[!0-9]*|0|'') echo "MIRY_RANGE_L2_CONCURRENCY must be positive" >&2; exit 2 ;;
esac

normalize_date=$(date -u -d "$start_date" +%F 2>/dev/null) || exit 2
test "$normalize_date" = "$start_date" || exit 2
normalize_date=$(date -u -d "$end_date" +%F 2>/dev/null) || exit 2
test "$normalize_date" = "$end_date" || exit 2
test "$start_date" \< "$end_date" || test "$start_date" = "$end_date" || {
    echo "start date is after end date" >&2
    exit 2
}

mkdir -p "$MIRY_RANGE_SYMBOLS_ROOT" "$MIRY_RANGE_LOG_ROOT"
previous_normalize=
previous_l2=${MIRY_RANGE_PREVIOUS_L2_JOB:-}
date_value=$start_date

submit() {
    sbatch --parsable \
      --account="$MIRY_RANGE_ACCOUNT" \
      --partition="$MIRY_RANGE_PARTITION" \
      --qos="$MIRY_RANGE_QOS" \
      "$@"
}

while :; do
    compact_date=$(printf '%s' "$date_value" | tr -d -)
    symbols_file="$MIRY_RANGE_SYMBOLS_ROOT/$date_value.txt"
    export MIRY_RANGE_DATE=$date_value
    export MIRY_RANGE_SYMBOLS_FILE=$symbols_file

    normalize_dependencies=$previous_normalize
    if [ "${MIRY_RANGE_EXTRA_DEPENDENCY_DATE:-}" = "$date_value" ]; then
        extra=${MIRY_RANGE_EXTRA_DEPENDENCY_JOB:?extra dependency job is required}
        if [ -n "$normalize_dependencies" ]; then
            normalize_dependencies="$normalize_dependencies:$extra"
        else
            normalize_dependencies=$extra
        fi
    fi
    normalize_args=""
    if [ -n "$normalize_dependencies" ]; then
        normalize_args="--dependency=afterok:$normalize_dependencies"
    fi
    # Shell expansion is intentional: normalize_args is either empty or one sbatch option.
    normalize_result=$(submit $normalize_args \
      --job-name="${MIRY_RANGE_JOB_PREFIX:-miry}-norm-$compact_date" \
      --output="$MIRY_RANGE_LOG_ROOT/norm-$date_value-%j.out" \
      --error="$MIRY_RANGE_LOG_ROOT/norm-$date_value-%j.err" \
      "$script_dir/slurm/range-normalize.sbatch")
    normalize_job=${normalize_result%%;*}

    symbols_result=$(submit \
      --dependency="afterok:$normalize_job" \
      --job-name="${MIRY_RANGE_JOB_PREFIX:-miry}-sym-$compact_date" \
      --output="$MIRY_RANGE_LOG_ROOT/symbols-$date_value-%j.out" \
      --error="$MIRY_RANGE_LOG_ROOT/symbols-$date_value-%j.err" \
      "$script_dir/slurm/range-symbols.sbatch")
    symbols_job=${symbols_result%%;*}

    l2_dependencies=$symbols_job
    if [ -n "$previous_l2" ]; then
        l2_dependencies="$l2_dependencies:$previous_l2"
    fi
    l2_result=$(submit \
      --dependency="afterok:$l2_dependencies" \
      --array="0-59%$concurrency" \
      --job-name="${MIRY_RANGE_JOB_PREFIX:-miry}-l2-$compact_date" \
      --output="$MIRY_RANGE_LOG_ROOT/l2-$date_value-%A_%a.out" \
      --error="$MIRY_RANGE_LOG_ROOT/l2-$date_value-%A_%a.err" \
      "$script_dir/slurm/range-l2.sbatch")
    l2_job=${l2_result%%;*}

    finalize_result=$(submit \
      --dependency="afterok:$l2_job" \
      --job-name="${MIRY_RANGE_JOB_PREFIX:-miry}-fin-$compact_date" \
      --output="$MIRY_RANGE_LOG_ROOT/finalize-$date_value-%j.out" \
      --error="$MIRY_RANGE_LOG_ROOT/finalize-$date_value-%j.err" \
      "$script_dir/slurm/range-finalize.sbatch")
    finalize_job=${finalize_result%%;*}

    echo "date=$date_value normalize=$normalize_job symbols=$symbols_job l2=$l2_job finalize=$finalize_job"
    previous_normalize=$normalize_job
    previous_l2=$l2_job
    [ "$date_value" = "$end_date" ] && break
    date_value=$(date -u -d "$date_value +1 day" +%F)
done
