#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run this preflight as root" >&2
    exit 1
fi

config_root=/etc/miry-data-collector
env_file=${1:-$config_root/edge.env}

if [ ! -r "$env_file" ]; then
    echo "missing or unreadable environment file: $env_file" >&2
    exit 1
fi

set -a
. "$env_file"
set +a
: "${EDGE_IMAGE:?EDGE_IMAGE is required}"
: "${EDGE_CONFIG:?EDGE_CONFIG is required}"

case "$EDGE_IMAGE" in
    *@sha256:*) ;;
    *)
        echo "EDGE_IMAGE must use an immutable sha256 digest" >&2
        exit 1
        ;;
esac

if [ ! -r "$EDGE_CONFIG" ]; then
    echo "missing or unreadable collector config: $EDGE_CONFIG" >&2
    exit 1
fi
if ! docker image inspect "$EDGE_IMAGE" >/dev/null 2>&1; then
    echo "target image is not present locally: $EDGE_IMAGE" >&2
    exit 1
fi

docker run \
    --rm \
    --network none \
    --read-only \
    --tmpfs /tmp:size=16m,mode=1777 \
    --volume "$EDGE_CONFIG:/etc/miry-data/edge.yaml:ro" \
    --entrypoint python \
    "$EDGE_IMAGE" \
    -c 'from pathlib import Path
from miry.collector.config import load_collector_config
config = load_collector_config(Path("/etc/miry-data/edge.yaml"))
print(
    "collector upgrade preflight passed "
    f"collector_id={config.collector_id} symbols={len(config.universe.members)} "
    f"universe={config.universe.core_generation}.{config.universe.candidate_revision} "
    f"sequence={config.universe.decision_sequence} queue_max_bytes={config.queue_max_bytes}"
)'
