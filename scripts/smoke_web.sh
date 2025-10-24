#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

need() {
  local bin="$1"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "[smoke] required binary not found: $bin" >&2
    exit 1
  fi
}

need docker
need curl
need jq

[[ "${SMOKE_WEB_DEBUG:-}" == "1" ]] && set -x

COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
SONGS_DIR="$REPO_ROOT/songs"
# Temporary files for payload capture
TMP_HEALTH="$(mktemp)"
TMP_HEALTH_ERR="$(mktemp)"

mkdir -p "$SONGS_DIR"

cleanup() {
  local exit_code="$1"
  if [[ -f "$TMP_HEALTH" ]]; then
    rm -f "$TMP_HEALTH" || true
  fi
  if [[ -f "$TMP_HEALTH_ERR" ]]; then
    rm -f "$TMP_HEALTH_ERR" || true
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
docker compose -f "$COMPOSE_FILE" up -d --build || { echo "[smoke] failed to start docker compose" >&2; exit 1; }

health_url="http://localhost:8000/healthz"
echo "Waiting for $health_url"
ready=0
for attempt in $(seq 1 30); do
  if curl --fail --silent --show-error "$health_url" >"$TMP_HEALTH" 2>"$TMP_HEALTH_ERR"; then
    ready=1
    break
  else
    echo "[smoke] health probe attempt $attempt failed" >&2
    if [[ -s "$TMP_HEALTH_ERR" ]]; then
      cat "$TMP_HEALTH_ERR" >&2
    fi
    fallback_body="$(curl --silent --show-error "$health_url" || true)"
    if [[ -n "$fallback_body" ]]; then
      echo "[smoke] response body:" >&2
      echo "$fallback_body" >&2
    fi
  fi
  sleep 2
  echo "Attempt $attempt failed, retrying..."
done
if [[ "$ready" -ne 1 ]]; then
  echo "[smoke] service did not become healthy in time" >&2
  if [[ -s "$TMP_HEALTH" ]]; then
    echo "[smoke] last response body:" >&2
    cat "$TMP_HEALTH" >&2
  fi
  if [[ -s "$TMP_HEALTH_ERR" ]]; then
    echo "[smoke] curl stderr:" >&2
    cat "$TMP_HEALTH_ERR" >&2
  fi
  exit 1
fi

#
# Validate /healthz JSON: минимальный обязательный набор полей.
# Без heredoc — на jq. Это надёжнее и прозрачнее в CI.
#
if ! jq -e '.status=="ok" and .mongo=="ok" and .redis=="ok"' "$TMP_HEALTH" >/dev/null; then
  echo "[smoke] healthz validation failed" >&2
  echo "---- response body ----" >&2
  cat "$TMP_HEALTH" >&2 || true
  exit 1
fi

echo "Checking CSRF token endpoint..."
csrf_payload="$(curl --fail --silent --show-error "http://localhost:8000/api/csrftoken" 2>"$TMP_HEALTH_ERR")" || {
  echo "[smoke] failed to fetch CSRF token" >&2
  if [[ -s "$TMP_HEALTH_ERR" ]]; then
    cat "$TMP_HEALTH_ERR" >&2
  fi
  fallback_body="$(curl --silent --show-error "http://localhost:8000/api/csrftoken" || true)"
  if [[ -n "$fallback_body" ]]; then
    echo "[smoke] response body:" >&2
    echo "$fallback_body" >&2
  fi
  exit 1
}
if ! printf '%s' "$csrf_payload" | python3 - <<'PY'; then
    echo "[smoke] csrftoken payload validation failed" >&2
    echo "---- response body ----" >&2
    printf '%s' "$csrf_payload" >&2
    exit 1
fi
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
songs_payload="$(curl --fail --silent --show-error "http://localhost:8000/api/songs?limit=5" 2>"$TMP_HEALTH_ERR")" || {
  echo "[smoke] failed to fetch songs catalog" >&2
  if [[ -s "$TMP_HEALTH_ERR" ]]; then
    cat "$TMP_HEALTH_ERR" >&2
  fi
  fallback_body="$(curl --silent --show-error "http://localhost:8000/api/songs?limit=5" || true)"
  if [[ -n "$fallback_body" ]]; then
    echo "[smoke] response body:" >&2
    echo "$fallback_body" >&2
  fi
  exit 1
}
if ! printf '%s' "$songs_payload" | python3 - <<'PY'; then
    echo "[smoke] songs payload validation failed" >&2
    echo "---- response body ----" >&2
    printf '%s' "$songs_payload" >&2
    exit 1
fi
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
