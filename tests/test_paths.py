import importlib
from pathlib import Path

import pytest


def _reload_paths(monkeypatch, *, songs_dir=None, app_root=None):
    for key in ("TAIKO_SONGS_DIR", "TAIKO_APP_ROOT"):
        monkeypatch.delenv(key, raising=False)
    if songs_dir is not None:
        monkeypatch.setenv("TAIKO_SONGS_DIR", str(songs_dir))
    if app_root is not None:
        monkeypatch.setenv("TAIKO_APP_ROOT", str(app_root))

    import server.paths as server_paths

    module = importlib.reload(server_paths)
    module._SONGS_DIR_CACHE = None
    module._SONGS_DIR_LOGGED = False
    return module


@pytest.fixture(autouse=False)
def reset_paths_env(monkeypatch):
    try:
        yield
    finally:
        _reload_paths(monkeypatch)


def test_songs_dir_prefers_user_override(tmp_path, monkeypatch, reset_paths_env):
    user_dir = tmp_path / "user_songs"
    user_dir.mkdir()
    app_root = tmp_path / "app_root"
    (app_root / "songs").mkdir(parents=True)

    module = _reload_paths(monkeypatch, songs_dir=user_dir, app_root=app_root)

    assert module.songs_dir() == user_dir.resolve()


def test_songs_dir_uses_app_root_when_user_missing(tmp_path, monkeypatch, reset_paths_env):
    app_root = tmp_path / "app_root"
    expected = app_root / "songs"
    expected.mkdir(parents=True)

    module = _reload_paths(monkeypatch, songs_dir=None, app_root=app_root)

    assert module.songs_dir() == expected.resolve()


def test_songs_dir_falls_back_to_app_dir(tmp_path, monkeypatch, reset_paths_env):
    fallback_root = tmp_path / "fallback"
    expected = fallback_root / "songs"
    expected.mkdir(parents=True)

    module = _reload_paths(monkeypatch)
    monkeypatch.setattr(module, "app_dir", lambda: fallback_root)
    module._SONGS_DIR_CACHE = None
    module._SONGS_DIR_LOGGED = False

    assert module.songs_dir() == expected.resolve()
