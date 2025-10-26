#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="$(pwd)/_data"
mkdir -p "$DATA_DIR"

RUN_PROFILE=desktop DATA_DIR="$DATA_DIR" ./dist/backend/taiko-web-backend/taiko-web-backend --port 8000 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT

for _ in {1..30}; do
  if RESPONSE=$(curl -sf --max-time 2 http://127.0.0.1:8000/healthz); then
    if [[ "$RESPONSE" == *'"status":"ok"'* && "$RESPONSE" == *'"db":"sqlite"'* ]]; then
      echo "healthz OK"
      exit 0
    fi
  fi
  sleep 1
done

echo "healthz failed"
exit 1
