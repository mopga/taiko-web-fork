from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SMOKE_SCRIPT = REPO_ROOT / "scripts" / "smoke_web.sh"


def test_smoke_script_invocation() -> None:
    env = os.environ.copy()
    proc = subprocess.run(
        ["bash", str(SMOKE_SCRIPT)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
    )
    assert proc.returncode == 0
