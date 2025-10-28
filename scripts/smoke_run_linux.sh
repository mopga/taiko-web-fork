#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="$(pwd)/_data"
SONGS_DIR="$(pwd)/_songs"
LOG_DIR="$(pwd)/_logs"
LOG_FILE="$LOG_DIR/smoke_backend.log"
BASE_URL="http://127.0.0.1:8000"

mkdir -p "$DATA_DIR" "$SONGS_DIR" "$LOG_DIR"

RUN_PROFILE=desktop PROFILE=desktop DATA_DIR="$DATA_DIR" ./dist/backend/taiko-web-backend/taiko-web-backend --host 127.0.0.1 --port 8000 --songs-dir "$SONGS_DIR" >"$LOG_FILE" 2>&1 &
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

if isinstance(data, list):
    pass
elif isinstance(data, dict):
    items = data.get('items')
    if items is None:
        items = []
    if not isinstance(items, list):
        raise SystemExit('Songs payload items is not a list')
else:
    raise SystemExit('Songs payload must be a list or dict with items list')
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
