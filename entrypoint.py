import signal
import subprocess
import sys
from pathlib import Path

START_SCRIPT = Path('/app/start.sh')


def _normalize_line_endings(path: Path) -> None:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return
    normalized = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if data != normalized:
        path.write_bytes(normalized)
        print(f"taiko-start-normalized: path={path} removed-carriage-returns", file=sys.stderr)


def main() -> int:
    _normalize_line_endings(START_SCRIPT)

    if not START_SCRIPT.exists():
        print(f"taiko-start-missing: path={START_SCRIPT}", file=sys.stderr)
        return 1

    args = ["/bin/bash", str(START_SCRIPT), *sys.argv[1:]]
    proc = subprocess.Popen(args)

    def _forward_signal(signum, _frame):
        if proc.poll() is None:
            proc.send_signal(signum)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, _forward_signal)

    try:
        return proc.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == '__main__':
    sys.exit(main())
