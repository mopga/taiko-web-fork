#!/usr/bin/env bash
set -euo pipefail

PY=${PYTHON:-python}
HOST=${TAIKO_SERVER_HOST:-0.0.0.0}
PORT=${TAIKO_SERVER_PORT:-34802}

$PY server.py --host "$HOST" --port "$PORT" &
server_pid=$!

cleanup() {
    if [ -n "${server_pid:-}" ]; then
        if kill -0 "$server_pid" 2>/dev/null; then
            kill "$server_pid" 2>/dev/null || true
        fi
        wait "$server_pid" 2>/dev/null || true
    fi
}

trap cleanup EXIT

if [ "${TAIKO_INIT_INDEXES:-1}" != "0" ]; then
    "$PY" tools/init_db_schema.py
fi

gunicorn \
    --bind 0.0.0.0:8000 \
    --workers ${GWORKERS:-2} \
    --threads ${GTHREADS:-4} \
    --capture-output \
    --error-logfile - \
    --access-logfile - \
    app:app
