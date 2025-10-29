"""Standalone desktop server entrypoint."""

from __future__ import annotations

import argparse
import atexit
import io
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Iterable, Optional

from server.paths import get_app_dir, get_songs_dir_desktop

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

_FALLBACK_STREAMS: list[io.TextIOBase] = []


def _resolve_logging_level(value: object, *, default: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            resolved = logging.getLevelName(value.upper())
            if isinstance(resolved, int):
                return resolved
            return int(value)
        except Exception:
            return default
    return default


def _ensure_stream(name: str) -> io.TextIOBase:
    stream = getattr(sys, name, None)
    if stream is None:
        handle = open(os.devnull, "w", encoding="utf-8")
        setattr(sys, name, handle)
        _FALLBACK_STREAMS.append(handle)
        return handle
    return stream


def _configure_logging() -> None:
    stdout = _ensure_stream("stdout")
    stderr = _ensure_stream("stderr")
    root_logger = logging.getLogger()

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    level = _resolve_logging_level(os.getenv("TAIKO_LOGLEVEL", "INFO"), default=logging.INFO)
    root_logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    file_handler = logging.FileHandler("desktop.log", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    try:
        if stdout and not os.getenv("PYTEST_CURRENT_TEST"):
            console_level = _resolve_logging_level(
                os.getenv("TAIKO_CONSOLE_LEVEL", "WARNING"), default=logging.WARNING
            )
            console_handler = logging.StreamHandler(stdout)
            console_handler.setLevel(console_level)
            console_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
            root_logger.addHandler(console_handler)
    except Exception:
        pass

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.propagate = True
        logger.handlers.clear()


@atexit.register
def _close_fallback_streams() -> None:
    for stream in _FALLBACK_STREAMS:
        try:
            stream.close()
        except Exception:
            pass


LOGGER = logging.getLogger("taiko.desktop")


def _resolve_host_port(args: argparse.Namespace) -> tuple[str, int]:
    host = args.host or os.getenv("HOST") or os.getenv("TAIKO_DESKTOP_HOST") or DEFAULT_HOST

    if args.port is not None:
        return host, int(args.port)

    env_port = os.getenv("PORT") or os.getenv("TAIKO_DESKTOP_PORT")
    if env_port:
        try:
            port_value = int(env_port)
        except (TypeError, ValueError):
            LOGGER.debug("desktop.port invalid candidate=%r", env_port)
        else:
            if 0 < port_value < 65536:
                return host, port_value
            LOGGER.debug("desktop.port out_of_range=%r", env_port)

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
    app_dir = get_app_dir()
    if value and Path(value).expanduser().resolve() != app_dir.resolve():
        LOGGER.warning("desktop.data_dir override is ignored; using app directory %s", app_dir)
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


def _prepare_environment(*, data_dir: Path) -> None:
    os.environ["RUN_PROFILE"] = "desktop"
    os.environ.setdefault("DATA_DIR", str(data_dir))


def _log_startup(*, server: str, host: str, port: int, data_dir: Path, songs_dir: Path, app) -> None:
    sessions_dir = app.config.get("SESSION_FILE_DIR")
    sqlite_path = app.config.get("SQLITE_DB_PATH")
    LOGGER.info(
        "desktop.start profile=desktop server=%s host=%s port=%s data_dir=%s songs_dir=%s db_path=%s sessions_dir=%s",
        server,
        host,
        port,
        data_dir,
        songs_dir,
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
    config.use_colors = False
    config.log_config = None
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
    _configure_logging()
    args = _parse_args(argv)
    data_dir = _resolve_data_dir(args.data_dir)
    songs_dir = get_songs_dir_desktop()
    try:
        songs_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        LOGGER.warning("desktop.songs_dir ensure_failed path=%s", songs_dir, exc_info=True)
    _prepare_environment(data_dir=data_dir)

    host, port = _resolve_host_port(args)
    os.environ.setdefault("PORT", str(port))
    server_choice = (args.server or os.environ.get("TAIKO_DESKTOP_SERVER") or "uvicorn").lower()

    from app import app as flask_app

    _log_startup(
        server=server_choice,
        host=host,
        port=port,
        data_dir=data_dir,
        songs_dir=songs_dir,
        app=flask_app,
    )
    LOGGER.info("desktop.songs_dir path=%s", songs_dir)

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
