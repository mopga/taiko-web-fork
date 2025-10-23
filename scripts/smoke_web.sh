#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
SONGS_DIR="$ROOT_DIR/songs"
TMP_HEALTH="$(mktemp)"

mkdir -p "$SONGS_DIR"

cleanup() {
  local exit_code="$1"
  if [[ -f "$TMP_HEALTH" ]]; then
    rm -f "$TMP_HEALTH"
  fi
  if [[ "${SMOKE_WEB_DEBUG:-}" != "" || "$exit_code" -ne 0 ]]; then
    echo "\n--- docker compose logs (tail) ---"
    docker compose -f "$COMPOSE_FILE" logs --tail 200 || true
    echo "--- end logs ---\n"
  fi
  docker compose -f "$COMPOSE_FILE" down -v || true
  exit "$exit_code"
}
trap 'cleanup "$?"' EXIT

echo "Starting docker compose stack..."
docker compose -f "$COMPOSE_FILE" up -d --build

health_url="http://localhost:8000/healthz"
echo "Waiting for $health_url"
ready=0
for attempt in $(seq 1 30); do
  if curl --fail --silent --show-error "$health_url" >"$TMP_HEALTH" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 2
  echo "Attempt $attempt failed, retrying..."
done
if [[ "$ready" -ne 1 ]]; then
  echo "Service did not become healthy in time" >&2
  exit 1
fi

python3 - "$TMP_HEALTH" <<'PY'
import json
import sys
from pathlib import Path

payload = Path(sys.argv[1]).read_text(encoding="utf-8")
try:
    data = json.loads(payload)
except json.JSONDecodeError as exc:
    raise SystemExit(f"Health payload is not JSON: {exc}\n{payload}")
assert data.get("status") == "ok", f"Unexpected health status: {data}"
assert data.get("mongo") == "ok", f"Mongo not ready: {data}"
assert data.get("redis") == "ok", f"Redis not ready: {data}"
PY

echo "Checking CSRF token endpoint..."
curl --fail --silent --show-error "http://localhost:8000/api/csrftoken" \
  | python3 - <<'PY'
import json, sys
payload = sys.stdin.read()
try:
    data = json.loads(payload)
except json.JSONDecodeError as exc:
    raise SystemExit(f"csrftoken payload is not JSON: {exc}\n{payload}")
assert data.get("status") == "ok", f"Unexpected csrftoken status: {data}"
assert isinstance(data.get("token"), str) and data["token"], "CSRF token is empty"
PY

echo "Checking songs catalog endpoint..."
curl --fail --silent --show-error "http://localhost:8000/api/songs?limit=5" \
  | python3 - <<'PY'
import json, sys
payload = sys.stdin.read()
try:
    data = json.loads(payload)
except json.JSONDecodeError as exc:
    raise SystemExit(f"songs payload is not JSON: {exc}\n{payload}")
if not isinstance(data, list):
    raise SystemExit(f"Expected list payload, got {type(data)!r}")
PY

echo "Smoke tests passed."
