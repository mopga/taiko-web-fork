import importlib
import sys
import types
from pathlib import Path
from unittest import mock


class _StubCollection:
    def create_index(self, *args, **kwargs):
        return None

    def drop_index(self, *args, **kwargs):
        return None

    def update_one(self, *args, **kwargs):
        return None


class _StubDatabase:
    def __init__(self):
        self.users = _StubCollection()
        self.songs = _StubCollection()
        self.scores = _StubCollection()
        self.song_scanner_state = _StubCollection()
        self.counters = _StubCollection()


class _StubMongoClient:
    def __init__(self, *args, **kwargs):
        self._db = _StubDatabase()

    def __getitem__(self, name):  # pragma: no cover - compatibility shim
        return self._db


class _StubRedis:
    def __init__(self, *args, **kwargs):  # pragma: no cover - smoke shim
        pass

    def ping(self):  # pragma: no cover - smoke shim
        return True


class _StubCache:
    def __init__(self, *args, **kwargs):  # pragma: no cover - smoke shim
        pass

    def init_app(self, *args, **kwargs):  # pragma: no cover - smoke shim
        return None

    def cached(self, *args, **kwargs):  # pragma: no cover - smoke shim
        def _decorator(func):
            return func

        return _decorator


class _StubSession:
    def __init__(self, *args, **kwargs):  # pragma: no cover - smoke shim
        pass

    def init_app(self, *args, **kwargs):  # pragma: no cover - smoke shim
        return None


_SYS_MODULE_STUBS = {
    "redis": types.SimpleNamespace(Redis=_StubRedis),
    "flask_caching": types.SimpleNamespace(Cache=_StubCache),
    "flask_session": types.SimpleNamespace(Session=_StubSession),
}


def load_app_module():
    module = sys.modules.get("app")
    if module is not None:
        return module

    for name, stub in _SYS_MODULE_STUBS.items():
        sys.modules.setdefault(name, stub)

    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    with mock.patch("pymongo.MongoClient", new=_StubMongoClient):
        module = importlib.import_module("app")
    return module
