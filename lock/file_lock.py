"""File-based ``LeaderLock`` implementation (desktop-oriented)."""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, TextIO, Union

from lock.interfaces import LeaderLock


@dataclass
class _LockBackend:
    kind: str
    lock: Any


class FileLeaderLock(LeaderLock):
    """``LeaderLock`` implementation persisted on the local filesystem."""

    def __init__(self, path: Union[str, Path], *, lock_timeout: float = 10.0) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_timeout = lock_timeout
        self._backend: Optional[_LockBackend] = None

    def _ensure_backend(self) -> _LockBackend:
        if self._backend is not None:
            return self._backend

        errors = []

        try:  # pragma: no cover - optional dependency
            from fasteners import InterProcessLock  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            errors.append(exc)
        else:
            self._backend = _LockBackend('fasteners', InterProcessLock(str(self._path)))
            return self._backend

        try:  # pragma: no cover - optional dependency
            import portalocker  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            errors.append(exc)
        else:
            lock = portalocker.Lock(
                str(self._path), mode='a+', timeout=self._lock_timeout, encoding='utf-8'
            )
            self._backend = _LockBackend('portalocker', lock)
            return self._backend

        message = 'FileLeaderLock requires either fasteners or portalocker to be installed'
        if errors:
            raise RuntimeError(message) from errors[-1]
        raise RuntimeError(message)

    @contextmanager
    def _exclusive_access(self) -> Iterator[Optional[TextIO]]:
        backend = self._ensure_backend()
        if backend.kind == 'portalocker':
            lock_obj = backend.lock
            with lock_obj as handle:
                handle.seek(0)
                yield handle
                handle.flush()
        else:  # fasteners backend
            lock_obj = backend.lock
            acquired = lock_obj.acquire(blocking=True, timeout=self._lock_timeout)
            if not acquired:
                raise TimeoutError('Failed to acquire file lock within timeout')
            try:
                yield None
            finally:
                lock_obj.release()

    def _read_state(self, handle: Optional[TextIO]) -> Optional[Dict[str, Any]]:
        try:
            if handle is not None:
                handle.seek(0)
                raw = handle.read()
            else:
                raw = self._path.read_text(encoding='utf-8')
        except FileNotFoundError:
            return None
        except OSError:
            return None

        if not raw:
            return None

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return data

    def _write_state(self, state: Optional[Dict[str, Any]], handle: Optional[TextIO]) -> None:
        if handle is not None:
            handle.seek(0)
            if state is None:
                handle.truncate(0)
            else:
                json.dump(state, handle)
                handle.truncate()
            handle.flush()
            return

        if state is None:
            try:
                self._path.unlink()
            except FileNotFoundError:
                return
            return

        tmp_path = self._path.with_suffix(self._path.suffix + '.tmp')
        tmp_path.write_text(json.dumps(state), encoding='utf-8')
        tmp_path.replace(self._path)

    def _is_active(self, state: Dict[str, Any], now: float) -> bool:
        expires_at = state.get('expires_at')
        if expires_at is None:
            return True
        try:
            expiry = float(expires_at)
        except (TypeError, ValueError):
            return False
        return expiry > now

    def get_owner(self) -> Optional[str]:
        now = time.time()
        with self._exclusive_access() as handle:
            state = self._read_state(handle)
            if not state:
                return None
            if not self._is_active(state, now):
                return None
            owner = state.get('token')
            return owner if isinstance(owner, str) else None

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        expires_at = time.time() + float(ttl_seconds)
        with self._exclusive_access() as handle:
            now = time.time()
            state = self._read_state(handle)
            if state and self._is_active(state, now):
                return False
            new_state = {'token': token, 'expires_at': expires_at}
            self._write_state(new_state, handle)
            return True

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        expires_at = time.time() + float(ttl_seconds)
        with self._exclusive_access() as handle:
            now = time.time()
            state = self._read_state(handle)
            if not state or not self._is_active(state, now):
                return False
            if state.get('token') != token:
                return False
            state['expires_at'] = expires_at
            self._write_state(state, handle)
            return True

    def release(self, token: str) -> bool:
        with self._exclusive_access() as handle:
            now = time.time()
            state = self._read_state(handle)
            if not state or state.get('token') != token:
                return False
            if not self._is_active(state, now):
                return False
            self._write_state(None, handle)
            return True
