#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 /path/to/miry-data-collector.sif" >&2
    exit 2
fi

release_sif=$1
if [ ! -r "$release_sif" ]; then
    echo "release SIF is not readable: $release_sif" >&2
    exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
install_root=${MIRY_CAMPUS_ROOT:-/persistent/miry-data-collector}
data_root=${MIRY_DATA_ROOT:-$install_root/data}
apptainer=${MIRY_APPTAINER:-/public/app/apptainer/1.4.5/bin/apptainer}
deploy_root=$install_root/deploy/campus-107

if [ ! -x "$apptainer" ]; then
    echo "missing executable Apptainer: $apptainer" >&2
    exit 1
fi

install -d -m 750 \
    "$install_root" \
    "$install_root/logs" \
    "$install_root/logs/processing" \
    "$install_root/rsync" \
    "$install_root/status" \
    "$install_root/status/processing" \
    "$data_root/raw" \
    "$data_root/derived" \
    "$data_root/transfer-ledger" \
    "$deploy_root/slurm"
release_hash=$(sha256sum "$release_sif" | cut -d' ' -f1)
release_name=miry-data-collector-$release_hash.sif
release_target=$install_root/$release_name
if [ -e "$release_target" ]; then
    installed_hash=$(sha256sum "$release_target" | cut -d' ' -f1)
    if [ "$installed_hash" != "$release_hash" ]; then
        echo "installed release has an unexpected hash: $release_target" >&2
        exit 1
    fi
else
    install -m 555 "$release_sif" "$release_target"
fi
if [ -e "$install_root/miry-data-collector.sif" ]; then
    exec 9>"$install_root/status/processing/submit.lock"
    if ! flock -n 9; then
        echo "rolling upgrade requires the derived submitter lock" >&2
        exit 1
    fi
    if ! command -v squeue >/dev/null 2>&1; then
        echo "rolling upgrade requires squeue to prove the old pipeline is drained" >&2
        exit 1
    fi
    active_jobs=$(squeue -h -u "${MIRY_SLURM_USER:-$(id -un)}" -o '%i|%j|%T' \
        | awk -F'|' '$2 ~ /^miry-(norm|normalize|inputs|l2-inputs|l2|finalize)(-|$)/')
    if [ -n "$active_jobs" ]; then
        echo "refusing rolling upgrade while old Slurm pipeline jobs remain:" >&2
        echo "$active_jobs" >&2
        exit 1
    fi
fi
ln -sfn "$release_name" "$install_root/miry-data-collector.sif"
sandbox_name=miry-data-collector-$release_hash.sandbox
sandbox_target=$install_root/$sandbox_name
if [ ! -d "$sandbox_target" ]; then
    sandbox_partial=$sandbox_target.partial
    if [ -e "$sandbox_partial" ]; then
        echo "remove incomplete sandbox before retrying: $sandbox_partial" >&2
        exit 1
    fi
    "$apptainer" build --sandbox "$sandbox_partial" "$release_target"
    mv "$sandbox_partial" "$sandbox_target"
fi
ln -sfn "$sandbox_name" "$install_root/miry-data-collector.sandbox"
install -m 555 "$script_dir/pull-once.sh" "$install_root/pull-once.sh"
install -m 555 "$script_dir/submit-ready-day.sh" "$deploy_root/submit-ready-day.sh"
install -m 555 "$script_dir/build-l2-inputs.py" "$deploy_root/build-l2-inputs.py"
install -m 555 "$script_dir/prune-l2-projections.py" "$deploy_root/prune-l2-projections.py"
install -m 555 "$script_dir/processing-status.py" "$deploy_root/processing-status.py"
install -m 555 "$script_dir/verify.sh" "$deploy_root/verify.sh"
install -m 444 "$script_dir/README.md" "$deploy_root/README.md"
install -m 444 "$script_dir/central.yaml.example" "$deploy_root/central.yaml.example"
install -m 444 "$script_dir/processing.env.example" "$deploy_root/processing.env.example"
install -m 444 "$script_dir/crontab.example" "$deploy_root/crontab.example"
install -m 444 "$script_dir/slurm/normalize.sbatch" "$deploy_root/slurm/normalize.sbatch"
install -m 444 "$script_dir/slurm/l2-inputs.sbatch" "$deploy_root/slurm/l2-inputs.sbatch"
install -m 444 "$script_dir/slurm/l2.sbatch" "$deploy_root/slurm/l2.sbatch"
install -m 444 "$script_dir/slurm/finalize.sbatch" "$deploy_root/slurm/finalize.sbatch"

for obsolete in \
    submit-day.sh submit-range.sh submit-symbol-range.py optimized-l2-runner.py \
    build-legacy-dedup-boundary.py finish-legacy-normalize.py \
    slurm/l2-array.sbatch slurm/optimized-normalize.sbatch \
    slurm/optimized-l2-inputs.sbatch slurm/optimized-l2.sbatch \
    slurm/optimized-finalize.sbatch slurm/range-normalize.sbatch \
    slurm/range-symbols.sbatch slurm/range-l2.sbatch slurm/range-finalize.sbatch \
    slurm/verify-dedup-boundary.sbatch
do
    rm -f "$deploy_root/$obsolete"
done

if [ ! -e "$install_root/central.yaml" ]; then
    install -m 600 "$script_dir/central.yaml.example" "$install_root/central.yaml"
fi
if [ ! -e "$deploy_root/processing.env" ]; then
    install -m 600 "$script_dir/processing.env.example" "$deploy_root/processing.env"
fi

echo "installed campus release $release_hash under $install_root"
echo "next: edit $install_root/central.yaml and $deploy_root/processing.env"
echo "then: run $deploy_root/verify.sh before installing cron"
