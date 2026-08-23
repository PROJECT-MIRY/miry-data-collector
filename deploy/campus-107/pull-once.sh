#!/bin/sh
set -eu

script_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
install_root=${MIRY_CAMPUS_ROOT:-$script_root}
apptainer=${MIRY_APPTAINER:-/public/app/apptainer/1.4.5/bin/apptainer}
flock=${MIRY_FLOCK:-/usr/bin/flock}
image=$install_root/miry-data-collector.sandbox
config=$install_root/central.yaml

exec 9>"$install_root/pull.lock"
"$flock" -n 9

exec "$apptainer" exec --writable "$image" \
    miry-data-pull --config "$config"
