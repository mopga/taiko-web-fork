#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="$(pwd)"
DATA_DIR="$WORK_DIR/_data"
LOG_DIR="$WORK_DIR/_logs"
LOG_FILE="$LOG_DIR/smoke_backend.log"
BASE_URL="http://127.0.0.1:8000"

BACKEND_STAGING="$REPO_ROOT/standalone/dist/backend/taiko-web-backend"

mkdir -p "$DATA_DIR" "$LOG_DIR"

RUN_PROFILE=desktop PROFILE=desktop DATA_DIR="$DATA_DIR" PORT=8000 \
  "$BACKEND_STAGING/taiko-web-backend" --host 127.0.0.1 --port 8000 >"$LOG_FILE" 2>&1 &
PID=$!

cleanup() {
  if [[ -n "${PID:-}" ]]; then
    kill "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_for_health() {
  for _ in $(seq 1 60); do
    if curl -sf --max-time 2 "$BASE_URL/healthz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

check_status() {
  local method="$1" path="$2"; shift 2
  local expected=("$@")
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X "$method" "$BASE_URL$path")
  for target in "${expected[@]}"; do
    if [[ "$code" == "$target" ]]; then
      return 0
    fi
  done
  echo "Unexpected status $code for $path" >&2
  return 1
}

fail() {
  echo "Smoke test failed" >&2
  if [[ -f "$LOG_FILE" ]]; then
    echo "===== backend log tail =====" >&2
    tail -n 200 "$LOG_FILE" >&2
  fi
  exit 1
}

wait_for_health || fail

if ! HEALTH_JSON="$(curl -fsS "$BASE_URL/healthz")"; then
  fail
fi

DATA_DIR_RESOLVED="$(cd "$DATA_DIR" && pwd)"
export HEALTH_JSON DATA_DIR_RESOLVED
python - <<'PY' || fail
import json
import os
from pathlib import Path

health = json.loads(os.environ['HEALTH_JSON'])
if health.get('status') != 'ok':
    raise SystemExit('health status not ok')
if health.get('profile') != 'desktop':
    raise SystemExit(f"unexpected profile: {health.get('profile')!r}")
db_path = health.get('db_path')
if not isinstance(db_path, str) or not db_path:
    raise SystemExit('db_path missing from /healthz')
if not Path(db_path).exists():
    raise SystemExit('db_path does not exist')
data_dir = Path(os.environ['DATA_DIR_RESOLVED']).resolve()
db_real = Path(db_path).resolve()
if db_real != data_dir and data_dir not in db_real.parents:
    raise SystemExit('db_path not under DATA_DIR')
PY

check_status GET / 200 || fail
check_status GET /favicon.ico 200 304 || fail
check_status GET /api/songs 200 || fail
check_status GET /api/modes 200 || fail
check_status GET /api/categories 200 || fail

echo "Desktop backend smoke test passed"
exit 0
