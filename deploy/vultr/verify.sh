#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run this verifier as root" >&2
    exit 1
fi

deploy_root=/opt/miry-data-collector/deploy/vultr
config_root=/etc/miry-data-collector

for command_name in curl docker nsenter nstat python3 rsync rrsync runuser ss systemctl sshd; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing command: $command_name" >&2
        exit 1
    fi
done

for path in \
    "$deploy_root/compose.yaml" \
    "$config_root/edge.env" \
    "$config_root/edge.yaml" \
    "$config_root/alert.env"
do
    if [ ! -r "$path" ]; then
        echo "missing or unreadable file: $path" >&2
        exit 1
    fi
done

set -a
. "$config_root/edge.env"
set +a
: "${EDGE_IMAGE:?EDGE_IMAGE is required}"
: "${EDGE_DATA_ROOT:?EDGE_DATA_ROOT is required}"
: "${EDGE_CONFIG:?EDGE_CONFIG is required}"

case "$EDGE_IMAGE" in
    *@sha256:*) ;;
    *)
        echo "EDGE_IMAGE must use an immutable sha256 digest" >&2
        exit 1
        ;;
esac
case "$EDGE_IMAGE" in
    *REPLACE_WITH_RELEASE_DIGEST*)
        echo "replace the placeholder in EDGE_IMAGE" >&2
        exit 1
        ;;
esac

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
    if [ ! -d "$EDGE_DATA_ROOT/$relative_path" ]; then
        echo "missing data directory: $EDGE_DATA_ROOT/$relative_path" >&2
        exit 1
    fi
done
if ! runuser -u data-puller -- test -r /etc/ssh/authorized_keys/data-puller; then
    echo "restricted rsync key is not configured" >&2
    exit 1
fi
if [ ! -x "$deploy_root/rsync_gateway.py" ]; then
    echo "restricted rsync gateway is not installed" >&2
    exit 1
fi
if [ ! -x "$deploy_root/diagnostics.py" ]; then
    echo "host diagnostics sampler is not installed" >&2
    exit 1
fi

docker compose -f "$deploy_root/compose.yaml" config --quiet
sshd -t
systemctl is-active --quiet miry-data-collector.service
systemctl is-active --quiet miry-data-diagnostics.timer
running_services=$(docker compose -f "$deploy_root/compose.yaml" ps --status running --services)
if [ "$running_services" != collector ]; then
    echo "collector container is not running" >&2
    docker compose -f "$deploy_root/compose.yaml" ps >&2
    exit 1
fi
echo "Vultr deployment checks passed"
