from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
SMOKE_SCRIPT = REPO_ROOT / "scripts" / "smoke_web.sh"
BASE_URL = "http://localhost:8000"


def _run_compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    return subprocess.run(cmd, check=check, cwd=REPO_ROOT, text=True, capture_output=True)


@pytest.fixture(scope="module")
def web_stack():
    REPO_ROOT.joinpath("songs").mkdir(exist_ok=True)
    _run_compose("up", "-d", "--build")
    session = requests.Session()
    try:
        health_url = f"{BASE_URL}/healthz"
        deadline = time.time() + 120
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                response = session.get(health_url, timeout=5)
            except requests.RequestException as exc:  # pragma: no cover - integration wait loop
                last_error = exc
                time.sleep(2)
                continue
            if response.status_code != 200:
                time.sleep(2)
                continue
            try:
                payload = response.json()
            except ValueError as exc:  # pragma: no cover - integration wait loop
                last_error = exc
                time.sleep(2)
                continue
            if payload.get("status") == "ok":
                break
            time.sleep(2)
        else:
            logs = _run_compose("logs", check=False)
            stdout = logs.stdout[-4000:]
            stderr = logs.stderr[-4000:]
            pytest.fail(
                "docker compose stack did not become healthy: "
                f"last_error={last_error!r}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        yield session
    finally:
        session.close()
        _run_compose("down", "-v", check=False)


def test_web_stack_health_and_api(web_stack: requests.Session) -> None:
    session = web_stack

    health = session.get(f"{BASE_URL}/healthz", timeout=5)
    health.raise_for_status()
    health_payload = health.json()
    assert health_payload.get("status") == "ok"
    assert health_payload.get("mongo") == "ok"
    assert health_payload.get("redis") == "ok"

    csrftoken = session.get(f"{BASE_URL}/api/csrftoken", timeout=5)
    csrftoken.raise_for_status()
    csrftoken_payload = csrftoken.json()
    assert csrftoken_payload.get("status") == "ok"
    assert isinstance(csrftoken_payload.get("token"), str)
    assert csrftoken_payload["token"], "CSRF token should not be empty"

    songs = session.get(f"{BASE_URL}/api/songs?limit=5", timeout=5)
    songs.raise_for_status()
    songs_payload = songs.json()
    assert isinstance(songs_payload, list)


def test_smoke_script_invocation():
    proc = subprocess.run(
        ["bash", str(SMOKE_SCRIPT)],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        text=True,
    )
    assert proc.returncode == 0
