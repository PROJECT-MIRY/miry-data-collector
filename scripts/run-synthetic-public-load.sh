#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
python_bin="$repo_root/.venv/bin/python"
load_script="$repo_root/scripts/synthetic_public_load.py"
rate_per_minute=${1:-700000}
duration_seconds=${2:-10}
port=${3:-18765}
server_cpus=${MIRY_REPLAY_SERVER_CPUS:-1-4}
client_cpu=${MIRY_REPLAY_CLIENT_CPU:-0}
server_log=$(mktemp)

server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid"
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log"
}
trap cleanup EXIT

if [[ ! -x "$python_bin" ]]; then
  echo "missing $python_bin; run: uv sync --all-groups" >&2
  exit 2
fi

taskset -c "$server_cpus" "$python_bin" "$load_script" server \
  --rate "$rate_per_minute" \
  --duration "$duration_seconds" \
  --port "$port" >"$server_log" 2>&1 &
server_pid=$!

for _attempt in {1..100}; do
  if grep -qx 'ready' "$server_log"; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    exit 2
  fi
  sleep 0.05
done

if ! grep -qx 'ready' "$server_log"; then
  cat "$server_log" >&2
  echo "synthetic load server did not become ready" >&2
  exit 2
fi

systemd-run --user --wait --pipe \
  -p CPUQuota=100% \
  -p "AllowedCPUs=$client_cpu" \
  -p MemoryMax=768M \
  -p TasksMax=512 \
  --working-directory="$repo_root" \
  "$python_bin" "$load_script" client \
  --decoder typed \
  --rate "$rate_per_minute" \
  --duration "$duration_seconds" \
  --port "$port"
