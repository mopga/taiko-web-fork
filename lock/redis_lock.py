"""Redis-backed ``LeaderLock`` implementation."""
from __future__ import annotations

from typing import Any, Callable, Optional

try:  # pragma: no cover - redis optional at runtime
    from redis import Redis
except Exception:  # pragma: no cover - fallback when redis is unavailable
    Redis = Any  # type: ignore[misc,assignment]

from lock.interfaces import LeaderLock


class RedisLeaderLock(LeaderLock):
    """``LeaderLock`` backed by a Redis key."""

    def __init__(self, client_factory: Callable[[], Redis], key: str) -> None:
        self._client_factory = client_factory
        self._key = key

    def _client(self) -> Redis:
        client = self._client_factory()
        if client is None:
            raise RuntimeError('Redis client is not available')
        return client

    def get_owner(self) -> Optional[str]:
        value = self._client().get(self._key)
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                return None
        if isinstance(value, str):
            return value
        return None

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        result = self._client().set(self._key, token, nx=True, ex=ttl_seconds)
        return bool(result)

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        client = self._client()
        current = client.get(self._key)
        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                current = None
        if current != token:
            return False
        return bool(client.expire(self._key, ttl_seconds))

    def release(self, token: str) -> bool:
        client = self._client()
        current = client.get(self._key)
        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover
                current = None
        if current != token:
            return False
        return bool(client.delete(self._key))
