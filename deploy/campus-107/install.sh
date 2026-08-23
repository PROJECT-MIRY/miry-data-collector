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
    "$install_root/rsync" \
    "$install_root/status" \
    "$install_root/symbols" \
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
install -m 555 "$script_dir/submit-day.sh" "$deploy_root/submit-day.sh"
install -m 555 "$script_dir/verify.sh" "$deploy_root/verify.sh"
install -m 444 "$script_dir/README.md" "$deploy_root/README.md"
install -m 444 "$script_dir/central.yaml.example" "$deploy_root/central.yaml.example"
install -m 444 "$script_dir/processing.env.example" "$deploy_root/processing.env.example"
install -m 444 "$script_dir/crontab.example" "$deploy_root/crontab.example"
install -m 444 "$script_dir/slurm/normalize.sbatch" "$deploy_root/slurm/normalize.sbatch"
install -m 444 "$script_dir/slurm/l2-array.sbatch" "$deploy_root/slurm/l2-array.sbatch"
install -m 444 "$script_dir/slurm/finalize.sbatch" "$deploy_root/slurm/finalize.sbatch"

if [ ! -e "$install_root/central.yaml" ]; then
    install -m 600 "$script_dir/central.yaml.example" "$install_root/central.yaml"
fi
if [ ! -e "$deploy_root/processing.env" ]; then
    install -m 600 "$script_dir/processing.env.example" "$deploy_root/processing.env"
fi

echo "installed campus release $release_hash under $install_root"
echo "next: edit $install_root/central.yaml and $deploy_root/processing.env"
echo "then: run $deploy_root/verify.sh before installing cron"
