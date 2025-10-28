#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="$(pwd)/_data"
SONGS_DIR="$(pwd)/_songs"
LOG_DIR="$(pwd)/_logs"
LOG_FILE="$LOG_DIR/smoke_backend.log"
BASE_URL="http://127.0.0.1:8000"

mkdir -p "$DATA_DIR" "$SONGS_DIR" "$LOG_DIR"

RUN_PROFILE=desktop DATA_DIR="$DATA_DIR" ./dist/backend/taiko-web-backend/taiko-web-backend --host 127.0.0.1 --port 8000 --songs-dir "$SONGS_DIR" >"$LOG_FILE" 2>&1 &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null || true
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

check_status HEAD / 200 || fail
content_type=$(curl -sI "$BASE_URL/" | awk 'tolower($1)=="content-type:" {print tolower($2)}')
if [[ "$content_type" != text/html* ]]; then
  echo "Unexpected root content type: $content_type" >&2
  fail
fi

check_status HEAD /favicon.ico 200 304 || fail

songs_payload=$(curl -sf --max-time 5 "$BASE_URL/api/songs") || fail
printf '%s' "$songs_payload" | python - <<'PY' || fail
import json
import sys

try:
    data = json.load(sys.stdin)
except Exception as exc:  # pragma: no cover - smoke guard
    raise SystemExit(f'Invalid JSON: {exc}')
if not isinstance(data, list):
    raise SystemExit('Songs payload is not a list')
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
