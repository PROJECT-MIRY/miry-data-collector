#!/bin/sh
set -eu

if [ "$#" -gt 1 ]; then
    echo "usage: $0 [UTC_TODAY]" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
processing_env=${MIRY_PROCESSING_ENV:-$script_dir/processing.env}
if [ ! -r "$processing_env" ]; then
    echo "missing processing environment: $processing_env" >&2
    exit 1
fi

set -a
. "$processing_env"
set +a
for name in \
    MIRY_APPTAINER MIRY_DATA_IMAGE MIRY_RAW_ROOT MIRY_DERIVED_ROOT MIRY_COLLECTOR \
    MIRY_L2_CONCURRENCY MIRY_NORMALIZE_WORKERS MIRY_PROCESSING_START_DATE MIRY_SLURM_ACCOUNT \
    MIRY_SLURM_PARTITION MIRY_SLURM_QOS
do
    eval "value=\${$name-}"
    if [ -z "$value" ]; then
        echo "$name is required" >&2
        exit 1
    fi
done

case "$MIRY_L2_CONCURRENCY" in
    *[!0-9]*|0|'') echo "MIRY_L2_CONCURRENCY must be positive" >&2; exit 1 ;;
esac
if [ "$MIRY_L2_CONCURRENCY" -gt 32 ]; then
    echo "MIRY_L2_CONCURRENCY cannot exceed the 32 CPU allocation" >&2
    exit 1
fi
case "$MIRY_NORMALIZE_WORKERS" in
    *[!0-9]*|0|'') echo "MIRY_NORMALIZE_WORKERS must be positive" >&2; exit 1 ;;
esac
if [ "$MIRY_NORMALIZE_WORKERS" -gt 8 ]; then
    echo "MIRY_NORMALIZE_WORKERS cannot exceed the measured 8-worker ceiling" >&2
    exit 1
fi
today=${1:-$(date -u +%F)}
test "$(date -u -d "$today" +%F 2>/dev/null)" = "$today" || {
    echo "invalid UTC today: $today" >&2
    exit 2
}
test "$(date -u -d "$MIRY_PROCESSING_START_DATE" +%F 2>/dev/null)" = \
    "$MIRY_PROCESSING_START_DATE" || {
    echo "invalid processing start date: $MIRY_PROCESSING_START_DATE" >&2
    exit 1
}

state_root=${MIRY_PROCESSING_STATE_ROOT:-$script_dir/../../status/processing}
log_root=${MIRY_PROCESSING_LOG_ROOT:-$script_dir/../../logs/processing}
submissions=$state_root/submissions
mkdir -p "$state_root" "$submissions" "$log_root"
exec 9>"$state_root/submit.lock"
flock -n 9 || exit 0

next_path=$state_root/next-date
if [ -s "$next_path" ]; then
    utc_date=$(sed -n '1p' "$next_path")
else
    utc_date=$MIRY_PROCESSING_START_DATE
fi

write_next_date() {
    next_value=$1
    partial=$(mktemp "$next_path.XXXXXX")
    printf '%s\n' "$next_value" > "$partial"
    mv "$partial" "$next_path"
}

quality_result_exists() {
    quality_root=$MIRY_DERIVED_ROOT/quality/collector=$MIRY_COLLECTOR/date=$1
    [ -s "$quality_root/_PROCESSED.json" ] || [ -s "$quality_root/_QUALITY_REJECTED.json" ]
}

while [ "$utc_date" \< "$today" ] && quality_result_exists "$utc_date"; do
    utc_date=$(date -u -d "$utc_date +1 day" +%F)
    write_next_date "$utc_date"
done
if [ ! "$utc_date" \< "$today" ]; then
    echo "derived scheduler waiting date=$utc_date today=$today"
    exit 0
fi

sealed=$MIRY_RAW_ROOT/collector=$MIRY_COLLECTOR/day-manifests/date=$utc_date/SEALED.json
if [ ! -s "$sealed" ]; then
    echo "derived scheduler waiting for sealed raw date=$utc_date"
    exit 0
fi
previous_date=$(date -u -d "$utc_date -1 day" +%F)
if [ "$utc_date" != "$MIRY_PROCESSING_START_DATE" ] && ! quality_result_exists "$previous_date"; then
    echo "previous UTC day has no terminal quality result: $previous_date" >&2
    exit 1
fi

submission=$submissions/date=$utc_date
submitting=$submission.submitting
if [ -e "$submission" ] || [ -e "$submitting" ]; then
    echo "derived submission already recorded date=$utc_date"
    exit 0
fi
if ! mkdir "$submitting" 2>/dev/null; then
    echo "derived submission already claimed date=$utc_date"
    exit 0
fi

export MIRY_PROCESSING_DATE=$utc_date
export MIRY_PROCESS_COMMAND=${MIRY_PROCESS_COMMAND:-miry-data-process}
export MIRY_L2_INPUT_BUILDER=$script_dir/build-l2-inputs.py

submit() {
    sbatch --parsable \
        --account="$MIRY_SLURM_ACCOUNT" \
        --partition="$MIRY_SLURM_PARTITION" \
        --qos="$MIRY_SLURM_QOS" \
        "$@"
}
job_id() {
    value=${1%%;*}
    case "$value" in *[!0-9]*|'') return 1 ;; esac
    printf '%s\n' "$value"
}

normalized="$MIRY_DERIVED_ROOT/typed/collector=$MIRY_COLLECTOR/date=$utc_date/_NORMALIZED.json"
normalize_job=
if [ ! -s "$normalized" ]; then
    normalize_job=$(job_id "$(submit \
        --job-name="miry-norm-${utc_date}" \
        --output="$log_root/normalize-$utc_date-%j.out" \
        --error="$log_root/normalize-$utc_date-%j.err" \
        "$script_dir/slurm/normalize.sbatch")")
    printf '%s\n' "$normalize_job" > "$submitting/normalize.job"
fi

if [ -n "$normalize_job" ]; then
    inputs_result=$(submit --dependency="afterok:$normalize_job" \
        --job-name="miry-inputs-${utc_date}" \
        --output="$log_root/l2-inputs-$utc_date-%j.out" \
        --error="$log_root/l2-inputs-$utc_date-%j.err" \
        "$script_dir/slurm/l2-inputs.sbatch")
else
    inputs_result=$(submit \
        --job-name="miry-inputs-${utc_date}" \
        --output="$log_root/l2-inputs-$utc_date-%j.out" \
        --error="$log_root/l2-inputs-$utc_date-%j.err" \
        "$script_dir/slurm/l2-inputs.sbatch")
fi
inputs_job=$(job_id "$inputs_result")
printf '%s\n' "$inputs_job" > "$submitting/l2-inputs.job"

l2_job=$(job_id "$(submit \
    --dependency="afterok:$inputs_job" \
    --array="0-59%$MIRY_L2_CONCURRENCY" \
    --job-name="miry-l2-${utc_date}" \
    --output="$log_root/l2-$utc_date-%A_%a.out" \
    --error="$log_root/l2-$utc_date-%A_%a.err" \
    "$script_dir/slurm/l2.sbatch")")
printf '%s\n' "$l2_job" > "$submitting/l2.job"

finalize_job=$(job_id "$(submit \
    --dependency="afterok:$l2_job" \
    --job-name="miry-finalize-${utc_date}" \
    --output="$log_root/finalize-$utc_date-%j.out" \
    --error="$log_root/finalize-$utc_date-%j.err" \
    "$script_dir/slurm/finalize.sbatch")")
printf '%s\n' "$finalize_job" > "$submitting/finalize.job"
mv "$submitting" "$submission"
echo "derived submitted date=$utc_date normalize=${normalize_job:-reused} inputs=$inputs_job l2=$l2_job finalize=$finalize_job"
