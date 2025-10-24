"""Redis-backed ``LeaderLock`` implementation."""
from __future__ import annotations

from typing import Any, Callable, Optional

try:  # pragma: no cover - redis optional at runtime
    from redis import Redis, WatchError
except Exception:  # pragma: no cover - fallback when redis is unavailable
    Redis = Any  # type: ignore[misc,assignment]
    WatchError = Exception  # type: ignore[assignment]

from lock.interfaces import LeaderLock


_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""


class RedisLeaderLock(LeaderLock):
    """``LeaderLock`` backed by a Redis key."""

    DEFAULT_TTL = 300

    def __init__(self, client_factory: Callable[[], Redis], key: str) -> None:
        self._client_factory = client_factory
        self._key = key
        self._release_script: Optional[Callable[..., Any]] = None

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

    @staticmethod
    def _normalise_ttl(ttl_seconds: int) -> int:
        ttl_value = int(ttl_seconds or 0)
        if ttl_value <= 0:
            ttl_value = RedisLeaderLock.DEFAULT_TTL
        return ttl_value

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        ttl_value = self._normalise_ttl(ttl_seconds)
        result = self._client().set(self._key, token, nx=True, ex=ttl_value)
        return bool(result)

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        client = self._client()
        ttl_value = self._normalise_ttl(ttl_seconds)
        current = client.get(self._key)
        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                current = None
        if current != token:
            return False
        refreshed = bool(client.expire(self._key, ttl_value))
        if refreshed or ttl_value <= 0:
            return refreshed
        try:  # pragma: no cover - ttl best effort
            ttl_state = client.ttl(self._key)
        except Exception:
            ttl_state = None
        if ttl_state != -1:
            return refreshed
        try:
            while True:
                pipe = client.pipeline()
                try:
                    pipe.watch(self._key)
                    owner = pipe.get(self._key)
                    if isinstance(owner, bytes):
                        try:
                            owner = owner.decode('utf-8')
                        except Exception:
                            owner = None
                    if owner != token:
                        pipe.unwatch()
                        break
                    pipe.multi()
                    pipe.set(self._key, token, xx=True, ex=ttl_value)
                    pipe.execute()
                    refreshed = True
                    break
                except WatchError:
                    continue
                finally:
                    pipe.reset()
        except Exception:
            return refreshed
        return refreshed

    def release(self, token: str) -> bool:
        client = self._client()
        script = self._release_script
        if script is None:
            script = client.register_script(_RELEASE_SCRIPT)
            self._release_script = script
        result = script(keys=[self._key], args=[token], client=client)
        return bool(result)
