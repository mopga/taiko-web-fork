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
SONGS_DIR="$BACKEND_STAGING/songs"
TEST_TRACK_SRC="$REPO_ROOT/tools/ci-assets/test-track"

mkdir -p "$DATA_DIR" "$SONGS_DIR" "$LOG_DIR"

if [[ "${TAIKO_SMOKE_COPY_TRACK:-1}" != "0" && -d "$TEST_TRACK_SRC" ]]; then
  TARGET_DIR="$SONGS_DIR/TestTrack"
  rm -rf "$TARGET_DIR"
  mkdir -p "$TARGET_DIR"
  cp -a "$TEST_TRACK_SRC/." "$TARGET_DIR/"
fi

RUN_PROFILE=desktop PROFILE=desktop DATA_DIR="$DATA_DIR" PORT=8000 "$BACKEND_STAGING/taiko-web-backend" --host 127.0.0.1 --port 8000 >"$LOG_FILE" 2>&1 &
PID=$!
songs_payload_file=""
cleanup() {
  kill "$PID" 2>/dev/null || true
  if [[ -n "$songs_payload_file" && -f "$songs_payload_file" ]]; then
    rm -f "$songs_payload_file"
  fi
}
trap cleanup EXIT

wait_for_health() {
  for _ in $(seq 1 60); do
    if RESPONSE=$(curl -sf --max-time 2 "$BASE_URL/healthz"); then
      if [[ "$RESPONSE" == *'"status":"ok"'* ]]; then
        return 0
      fi
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

health_payload=$(curl -sf --max-time 5 "$BASE_URL/healthz") || fail
python <<'PY' "${health_payload}" "${DATA_DIR}" || fail
import json
import os
import sys

payload_raw = sys.argv[1]
data_dir = os.path.abspath(sys.argv[2])
try:
    payload = json.loads(payload_raw)
except Exception as exc:
    raise SystemExit(f'healthz JSON parse error: {exc}')

if not isinstance(payload, dict):
    raise SystemExit('healthz payload is not an object')

profile = payload.get('profile')
if profile != 'desktop':
    raise SystemExit(f'Unexpected profile from /healthz: {profile!r}')

db_path = payload.get('db_path')
if not db_path:
    raise SystemExit('healthz payload missing db_path')

db_real = os.path.abspath(db_path)
if not db_real.startswith(data_dir):
    raise SystemExit(f'db_path {db_real!r} not under DATA_DIR {data_dir!r}')
PY

for _ in $(seq 1 30); do
  if grep -q "profile=desktop catalog_source=filesystem" "$LOG_FILE" 2>/dev/null; then
    catalog_logged=1
    break
  fi
  sleep 1
done
if [[ -z "${catalog_logged:-}" ]]; then
  echo "Expected catalog source log not found" >&2
  fail
fi

check_status HEAD / 200 || fail
content_type=$(curl -sI "$BASE_URL/" | awk 'tolower($1)=="content-type:" {print tolower($2)}')
if [[ "$content_type" != text/html* ]]; then
  echo "Unexpected root content type: $content_type" >&2
  fail
fi

check_status HEAD /favicon.ico 200 304 || fail

songs_payload_file=$(mktemp)
songs_status=""
for _ in $(seq 1 45); do
  songs_status=$(curl -s -w '%{http_code}' --max-time 5 "$BASE_URL/api/songs" -o "$songs_payload_file")
  if [[ "$songs_status" == "200" ]]; then
    break
  fi
  sleep 1
done

if [[ "${songs_status:-}" != "200" ]]; then
  echo "Timed out waiting for /api/songs 200" >&2
  fail
fi

python <<'PY' <"$songs_payload_file" || fail
import json
import sys

try:
    data = json.load(sys.stdin)
except Exception as exc:  # pragma: no cover - smoke guard
    raise SystemExit(f'Invalid JSON: {exc}')

if isinstance(data, dict):
    items = data.get('items')
    if items is None:
        items = []
    if not isinstance(items, list):
        raise SystemExit('Songs payload items is not a list')
    payload = items
elif isinstance(data, list):
    payload = data
else:
    raise SystemExit('Songs payload must be a list or dict with items list')

if payload:
    first = payload[0]
    if not isinstance(first, dict):
        raise SystemExit('First song entry is not an object')
    if not first.get('is_playable'):
        raise SystemExit('First song is not playable')
    difficulties = first.get('difficulties')
    if not isinstance(difficulties, dict) or not difficulties:
        raise SystemExit('First song has invalid difficulties payload')
    for key, value in difficulties.items():
        if not isinstance(value, dict):
            raise SystemExit(f'Difficulty {key} is not an object')
PY

openapi_status=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/openapi.json")
if [[ "$openapi_status" == "200" ]]; then
  echo "openapi.json present"
elif [[ "$openapi_status" != "404" ]]; then
  echo "Unexpected status $openapi_status for /openapi.json" >&2
  fail
fi

echo "Desktop backend smoke test passed"
exit 0
