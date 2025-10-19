#!/usr/bin/env bash
set -euo pipefail

PY=${PYTHON:-python}
HOST=${TAIKO_SERVER_HOST:-0.0.0.0}
PORT=${TAIKO_SERVER_PORT:-34802}

$PY server.py --host "$HOST" --port "$PORT" &
server_pid=$!

cleanup() {
    if kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi
}

trap cleanup EXIT

exec gunicorn --bind 0.0.0.0:8000 --workers ${GWORKERS:-2} --threads ${GTHREADS:-4} app:app
