#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run this installer as root" >&2
    exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
collector_user=data-puller
collector_group=data-puller
collector_id=10001
deploy_root=/opt/miry-data-collector/deploy/vultr
config_root=/etc/miry-data-collector
data_root=/srv/miry-data-rsync
diagnostics_root=/var/log/miry-data-collector/diagnostics

for command_name in rsync rrsync; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing command: $command_name" >&2
        exit 1
    fi
done

if getent group "$collector_group" >/dev/null 2>&1; then
    actual_gid=$(getent group "$collector_group" | cut -d: -f3)
    if [ "$actual_gid" != "$collector_id" ]; then
        echo "$collector_group exists with GID $actual_gid, expected $collector_id" >&2
        exit 1
    fi
elif getent group "$collector_id" >/dev/null 2>&1; then
    echo "GID $collector_id is already owned by another group" >&2
    exit 1
else
    groupadd --gid "$collector_id" "$collector_group"
fi

if getent passwd "$collector_user" >/dev/null 2>&1; then
    actual_uid=$(id -u "$collector_user")
    if [ "$actual_uid" != "$collector_id" ]; then
        echo "$collector_user exists with UID $actual_uid, expected $collector_id" >&2
        exit 1
    fi
elif getent passwd "$collector_id" >/dev/null 2>&1; then
    echo "UID $collector_id is already owned by another user" >&2
    exit 1
else
    useradd \
        --uid "$collector_id" \
        --gid "$collector_group" \
        --home-dir / \
        --no-create-home \
        --shell /bin/sh \
        "$collector_user"
fi
usermod --shell /bin/sh "$collector_user"

install -d -o root -g root -m 755 "$data_root"
install -d -o root -g root -m 750 "$diagnostics_root"
for relative_path in \
    ready \
    writing \
    control \
    control/acks \
    control/applying-acks \
    control/rejected-acks \
    control/transfer-ledger \
    control/universe
do
    install -d \
        -o "$collector_id" \
        -g "$collector_id" \
        -m 750 \
        "$data_root/$relative_path"
done

install -d -o root -g root -m 755 "$deploy_root" "$deploy_root/systemd" "$config_root"
install -m 644 "$script_dir/README.md" "$deploy_root/README.md"
install -m 644 "$script_dir/compose.yaml" "$deploy_root/compose.yaml"
install -m 644 "$script_dir/edge.yaml.example" "$deploy_root/edge.yaml.example"
install -m 644 "$script_dir/edge.env.example" "$deploy_root/edge.env.example"
install -m 644 "$script_dir/alert.env.example" "$deploy_root/alert.env.example"
install -m 555 "$script_dir/preflight-upgrade.sh" "$deploy_root/preflight-upgrade.sh"
install -m 555 "$script_dir/verify.sh" "$deploy_root/verify.sh"
install -m 555 "$script_dir/configure-rsync.sh" "$deploy_root/configure-rsync.sh"
install -m 555 "$script_dir/rsync_gateway.py" "$deploy_root/rsync_gateway.py"
install -m 555 "$script_dir/diagnostics.py" "$deploy_root/diagnostics.py"
install -m 644 \
    "$script_dir/systemd/miry-data-collector.service" \
    "$deploy_root/systemd/miry-data-collector.service"
install -m 644 \
    "$script_dir/systemd/miry-data-collector-alert@.service" \
    "$deploy_root/systemd/miry-data-collector-alert@.service"
install -m 644 \
    "$script_dir/systemd/miry-data-diagnostics.service" \
    "$deploy_root/systemd/miry-data-diagnostics.service"
install -m 644 \
    "$script_dir/systemd/miry-data-diagnostics.timer" \
    "$deploy_root/systemd/miry-data-diagnostics.timer"
install -m 644 \
    "$script_dir/systemd/miry-data-collector.service" \
    /etc/systemd/system/miry-data-collector.service
install -m 644 \
    "$script_dir/systemd/miry-data-collector-alert@.service" \
    /etc/systemd/system/miry-data-collector-alert@.service
install -m 644 \
    "$script_dir/systemd/miry-data-diagnostics.service" \
    /etc/systemd/system/miry-data-diagnostics.service
install -m 644 \
    "$script_dir/systemd/miry-data-diagnostics.timer" \
    /etc/systemd/system/miry-data-diagnostics.timer

if [ ! -e "$config_root/edge.yaml" ]; then
    install \
        -o root \
        -g "$collector_id" \
        -m 640 \
        "$script_dir/edge.yaml.example" \
        "$config_root/edge.yaml"
fi
if [ ! -e "$config_root/edge.env" ]; then
    install -m 600 "$script_dir/edge.env.example" "$config_root/edge.env"
fi
if [ ! -e "$config_root/alert.env" ]; then
    install -m 600 "$script_dir/alert.env.example" "$config_root/alert.env"
fi

systemctl daemon-reload

echo "installed Vultr deployment files"
echo "next: edit $config_root/*.yaml and $config_root/*.env"
echo "then: run $deploy_root/configure-rsync.sh /path/to/campus-key.pub"
