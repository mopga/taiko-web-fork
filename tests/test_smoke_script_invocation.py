from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def test_smoke_script_invocation() -> None:
    repo = REPO_ROOT
    env = os.environ.copy()
    env["SMOKE_WEB_DEBUG"] = "1"
    proc = subprocess.run(
        ["bash", "scripts/smoke_web.sh"],
        cwd=str(repo),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    print("---- smoke stdout ----\n" + proc.stdout)
    print("---- smoke stderr ----\n" + proc.stderr)
    assert proc.returncode == 0
