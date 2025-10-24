"""Redis-backed ``LeaderLock`` implementation."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

try:  # pragma: no cover - redis optional at runtime
    from redis import Redis
except Exception:  # pragma: no cover - fallback when redis is unavailable
    Redis = Any  # type: ignore[misc,assignment]

from lock.interfaces import LeaderLock


LOGGER = logging.getLogger(__name__)


SCAN_LEADER_KEY = "taiko:scanner:leader"


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
        try:
            result = self._client().set(self._key, token, nx=True, ex=ttl_value)
        except Exception:
            LOGGER.debug('Redis leader lock acquire failed: key=%s token=%s', self._key, token, exc_info=True)
            return False
        return bool(result)

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        ttl_value = self._normalise_ttl(ttl_seconds)
        try:
            client = self._client()
        except Exception:
            LOGGER.debug('Redis leader lock refresh failed to obtain client: key=%s token=%s', self._key, token, exc_info=True)
            return False

        try:
            current = client.get(self._key)
        except Exception:
            LOGGER.debug('Redis leader lock refresh failed to read owner: key=%s token=%s', self._key, token, exc_info=True)
            return False

        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                current = None

        if current != token:
            LOGGER.info(
                'Redis leader lock refresh skipped (not owner): key=%s token=%s owner=%s',
                self._key,
                token,
                current or '<unknown>',
            )
            return False

        try:
            refreshed = bool(client.expire(self._key, ttl_value))
        except Exception:
            LOGGER.debug('Redis leader lock expire failed: key=%s token=%s', self._key, token, exc_info=True)
            return False

        return refreshed

    def release(self, token: str) -> bool:
        try:
            client = self._client()
        except Exception:
            LOGGER.debug('Redis leader lock release failed to obtain client: key=%s token=%s', self._key, token, exc_info=True)
            return False

        script = self._release_script
        if script is None:
            script = client.register_script(_RELEASE_SCRIPT)
            self._release_script = script

        try:
            result = script(keys=[self._key], args=[token], client=client)
        except Exception:
            LOGGER.debug('Redis leader lock release script failed: key=%s token=%s', self._key, token, exc_info=True)
            return False

        ok = bool(result)
        LOGGER.info('Leader lock release result: ok=%s key=%s token=%s', ok, self._key, token)
        return ok
