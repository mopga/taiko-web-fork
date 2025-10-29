import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from cachelib.file import FileSystemCache

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _import_desktop_app(monkeypatch, tmp_path: Path):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.setenv("RUN_PROFILE", "desktop")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    sys.modules.pop("app", None)
    for module_name in list(sys.modules.keys()):
        if module_name == "flask_session" or module_name.startswith("flask_session."):
            sys.modules.pop(module_name, None)
    return importlib.import_module("app")


def test_desktop_healthz(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ok"
    assert payload["ok"] is True
    assert payload["profile"] == "desktop"
    assert payload["db"] == "sqlite"
    assert payload["sessions"] == "cachelib"
    assert payload.get("path") == str(tmp_path / "taiko.db")


def test_sessions_filesystem_directory_created(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    cache_backend = app_module.app.config["SESSION_CACHELIB"]
    assert isinstance(cache_backend, FileSystemCache)
    sessions_dir = Path(cache_backend._path)
    assert sessions_dir.exists()
    assert app_module.app.config.get("SESSION_TYPE") == "cachelib"

    def _touch_session():
        from flask import session as flask_session

        flask_session["ping"] = "pong"
        return "ok"

    app_module.app.add_url_rule("/_test_session", "_test_session", _touch_session)

    def _read_session():
        from flask import session as flask_session

        return flask_session.get("ping", "missing")

    app_module.app.add_url_rule("/_read_session", "_read_session", _read_session)

    client = app_module.app.test_client()
    response = client.get("/_test_session")
    assert response.status_code == 200

    cookie_header = response.headers.get("Set-Cookie", "")
    assert "session=" in cookie_header

    follow_response = client.get("/_read_session")
    assert follow_response.status_code == 200
    assert follow_response.get_data(as_text=True) == "pong"

    cache = app_module.app.session_interface.cache
    data_files = list(cache._list_dir())
    assert data_files


def test_no_redis_in_desktop(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    assert app_module.app.config.get("SESSION_TYPE") == "cachelib"
    assert app_module.app.config.get("SESSION_REDIS") is None
    assert "redis" not in caplog.text.lower()


def test_web_profile_unchanged(tmp_path, monkeypatch):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.setenv("RUN_PROFILE", "web")
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("TAIKO_WEB_MONGO_HOST", "localhost:27017")
    monkeypatch.setenv("TAIKO_WEB_MONGO_DB", "taiko-test")
    monkeypatch.setenv("TAIKO_WEB_REDIS_HOST", "localhost")
    monkeypatch.setenv("TAIKO_WEB_REDIS_PORT", "6379")
    monkeypatch.setenv("TAIKO_WEB_REDIS_DB", "0")
    monkeypatch.delenv("TAIKO_WEB_REDIS_PASSWORD", raising=False)

    fake_client = mock.MagicMock()
    fake_db = mock.MagicMock()
    fake_client.__getitem__.return_value = fake_db

    sys.modules.pop("app", None)
    with mock.patch("pymongo.MongoClient", return_value=fake_client):
        with mock.patch("redis.Redis.ping", return_value=True):
            app_module = importlib.import_module("app")

    assert app_module.app.config["SESSION_TYPE"] == "redis"
    assert app_module.app.config.get("SESSION_BACKEND") == "redis"
    assert app_module.app.config.get("SESSION_FILE_DIR") is None


def test_standalone_entrypoint_imports_and_runs_uvicorn(tmp_path, monkeypatch):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.delenv("RUN_PROFILE", raising=False)
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    sys.modules.pop("app", None)

    config_calls = {}

    class FakeConfig:
        def __init__(self, app, host, port, log_level, access_log, lifespan):
            config_calls["app"] = app
            config_calls["host"] = host
            config_calls["port"] = port

    class FakeServer:
        def __init__(self, config):
            config_calls["config"] = config
            self.should_exit = False

        def run(self):
            config_calls["run"] = True

    fake_middleware = SimpleNamespace(WSGIMiddleware=lambda app: app)
    fake_uvicorn = SimpleNamespace(Config=FakeConfig, Server=FakeServer)
    sys.modules["uvicorn"] = fake_uvicorn
    sys.modules["uvicorn.middleware"] = SimpleNamespace(wsgi=fake_middleware)
    sys.modules["uvicorn.middleware.wsgi"] = fake_middleware

    module = importlib.reload(importlib.import_module("standalone.run_desktop"))
    exit_code = module.main(["--port", "12345"])

    assert exit_code == 0
    assert config_calls["host"] == "127.0.0.1"
    assert config_calls["port"] == 12345
    assert config_calls["run"] is True


def test_standalone_entrypoint_waitress(monkeypatch, tmp_path):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.delenv("RUN_PROFILE", raising=False)
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("TAIKO_DESKTOP_SERVER", "waitress")

    sys.modules.pop("app", None)

    serve_calls = {}

    def fake_serve(app, host, port):
        serve_calls["app"] = app
        serve_calls["host"] = host
        serve_calls["port"] = port

    fake_waitress = SimpleNamespace(serve=fake_serve)
    sys.modules["waitress"] = fake_waitress

    module = importlib.reload(importlib.import_module("standalone.run_desktop"))
    exit_code = module.main(["--port", "23456"])

    assert exit_code == 0
    assert serve_calls["host"] == "127.0.0.1"
    assert serve_calls["port"] == 23456


def test_desktop_admin_routes_guarded(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.get("/admin/songs")
    assert response.status_code == 503
    assert b"desktop profile" in response.data.lower()


def test_desktop_api_login_guarded(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.post("/api/login", json={"username": "user", "password": "pass"})
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "error"
    assert payload["message"] == "desktop_profile_feature_unavailable"
