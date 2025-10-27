"""Standalone desktop server entrypoint."""

from __future__ import annotations

import argparse
import logging
import os
import signal
from pathlib import Path
from typing import Iterable, Optional

LOGGER = logging.getLogger("taiko.desktop")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000


def _resolve_host_port(args: argparse.Namespace) -> tuple[str, int]:
    host = args.host or os.getenv("TAIKO_DESKTOP_HOST") or DEFAULT_HOST

    port_candidates: Iterable[object] = (
        args.port,
        os.getenv("TAIKO_DESKTOP_PORT"),
        os.getenv("PORT"),
        os.getenv("UVICORN_PORT"),
    )
    for candidate in port_candidates:
        if candidate in (None, ""):
            continue
        try:
            port_value = int(candidate)
        except (TypeError, ValueError):
            LOGGER.debug("desktop.port invalid candidate=%r", candidate)
            continue
        if port_value <= 0 or port_value > 65535:
            LOGGER.debug("desktop.port out_of_range=%r", candidate)
            continue
        return host, port_value

    return host, DEFAULT_PORT


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TaikoWeb desktop server")
    parser.add_argument("--host", dest="host", default=None, help="Host interface to bind")
    parser.add_argument(
        "--port",
        dest="port",
        type=int,
        default=None,
        help="Port to bind the desktop server",
    )
    parser.add_argument(
        "--data-dir",
        dest="data_dir",
        default=None,
        help="Override the data directory (defaults to $DATA_DIR or ~/.taiko-web-data)",
    )
    parser.add_argument(
        "--server",
        dest="server",
        choices=("uvicorn", "waitress"),
        default=None,
        help="Select the HTTP server implementation",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _resolve_data_dir(value: Optional[str]) -> Path:
    data_dir_value = value or os.environ.get("DATA_DIR")
    if data_dir_value:
        path = Path(data_dir_value).expanduser()
    else:
        path = Path.home() / ".taiko-web-data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _prepare_environment(*, data_dir: Path) -> None:
    os.environ["RUN_PROFILE"] = "desktop"
    os.environ.setdefault("DATA_DIR", str(data_dir))


def _log_startup(*, server: str, host: str, port: int, data_dir: Path, app) -> None:
    sessions_dir = app.config.get("SESSION_FILE_DIR")
    sqlite_path = app.config.get("SQLITE_DB_PATH")
    LOGGER.info(
        "desktop.start profile=desktop server=%s host=%s port=%s data_dir=%s db_path=%s sessions_dir=%s",
        server,
        host,
        port,
        data_dir,
        sqlite_path,
        sessions_dir,
    )


def _restore_signals(previous: dict[int, object]) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except Exception:  # pragma: no cover - restoration best effort
            LOGGER.debug("failed to restore signal handler", exc_info=True)


def _run_uvicorn(app, *, host: str, port: int) -> None:
    try:
        import uvicorn
        from uvicorn.middleware.wsgi import WSGIMiddleware
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("uvicorn is required for desktop profile") from exc

    asgi_app = WSGIMiddleware(app)
    config = uvicorn.Config(
        asgi_app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
        lifespan="off",
    )
    server = uvicorn.Server(config)

    previous_handlers = {
        signal.SIGINT: signal.getsignal(signal.SIGINT),
        signal.SIGTERM: signal.getsignal(signal.SIGTERM),
    }

    def _handle_signal(signum, _frame) -> None:
        LOGGER.info("desktop.signal received=%s; shutting down", signum)
        server.should_exit = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        try:
            server.run()
        except KeyboardInterrupt:
            LOGGER.info("desktop.uvicorn stopping host=%s port=%s", host, port)
    finally:
        LOGGER.info("desktop.uvicorn stopped host=%s port=%s", host, port)
        _restore_signals(previous_handlers)


def _run_waitress(app, *, host: str, port: int) -> None:
    try:
        from waitress import serve
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("waitress is required for waitress server mode") from exc

    previous_handlers = {
        signal.SIGINT: signal.getsignal(signal.SIGINT),
        signal.SIGTERM: signal.getsignal(signal.SIGTERM),
    }

    def _handle_signal(signum, _frame) -> None:
        LOGGER.info("desktop.signal received=%s; shutting down", signum)
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        try:
            serve(app, host=host, port=port)
        except KeyboardInterrupt:
            LOGGER.info("desktop.waitress stopping host=%s port=%s", host, port)
    finally:
        _restore_signals(previous_handlers)

    LOGGER.info("desktop.waitress stopped host=%s port=%s", host, port)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    data_dir = _resolve_data_dir(args.data_dir)
    _prepare_environment(data_dir=data_dir)

    host, port = _resolve_host_port(args)
    server_choice = (args.server or os.environ.get("TAIKO_DESKTOP_SERVER") or "uvicorn").lower()

    from app import app as flask_app

    _log_startup(server=server_choice, host=host, port=port, data_dir=data_dir, app=flask_app)

    try:
        if server_choice == "waitress":
            _run_waitress(flask_app, host=host, port=port)
        else:
            _run_uvicorn(flask_app, host=host, port=port)
    finally:
        LOGGER.info(
            "desktop.shutdown profile=desktop server=%s host=%s port=%s",
            server_choice,
            host,
            port,
        )

    return 0


if __name__ == "__main__":  # pragma: no cover - manual execution
    raise SystemExit(main())
