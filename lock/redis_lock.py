"""Redis-backed ``LeaderLock`` implementation."""
from __future__ import annotations

import logging
import os
import time
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
    _ACQUIRE_WARNING_THRESHOLD = 10
    _ACQUIRE_WARNING_COOLDOWN_SECONDS = 30

    def __init__(self, client_factory: Callable[[], Redis], key: str) -> None:
        self._client_factory = client_factory
        self._key = key
        self._release_script: Optional[Callable[..., Any]] = None
        self._consecutive_acquire_failures = 0
        self._last_acquire_warning_monotonic = float('-inf')

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

    @staticmethod
    def _mask_token(token: str) -> str:
        if len(token) <= 8:
            return token
        return f"{token[:6]}…"

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        ttl_value = self._normalise_ttl(ttl_seconds)
        masked_token = self._mask_token(token)
        try:
            result = self._client().set(self._key, token, nx=True, ex=ttl_value)
        except Exception:
            self._consecutive_acquire_failures += 1
            now = time.monotonic()
            should_warn = False
            if self._consecutive_acquire_failures >= self._ACQUIRE_WARNING_THRESHOLD:
                if now - self._last_acquire_warning_monotonic >= self._ACQUIRE_WARNING_COOLDOWN_SECONDS:
                    should_warn = True
                    self._last_acquire_warning_monotonic = now
            if should_warn:
                LOGGER.warning(
                    'Redis leader lock acquire failing repeatedly: key=%s token=%s failures=%d',
                    self._key,
                    masked_token,
                    self._consecutive_acquire_failures,
                    exc_info=True,
                )
                self._consecutive_acquire_failures = 0
            else:
                LOGGER.debug(
                    'Redis leader lock acquire failed: key=%s token=%s',
                    self._key,
                    masked_token,
                    exc_info=True,
                )
            return False
        self._consecutive_acquire_failures = 0
        if result:
            LOGGER.info(
                'Leader lock acquired: key=%s, token=%s, pid=%d, ttl=%ds',
                self._key,
                masked_token,
                os.getpid(),
                ttl_value,
            )
            return True
        return False

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        ttl_value = self._normalise_ttl(ttl_seconds)
        try:
            client = self._client()
        except Exception:
            LOGGER.debug('Redis leader lock refresh failed to obtain client: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
            return False

        try:
            current = client.get(self._key)
        except Exception:
            LOGGER.debug('Redis leader lock refresh failed to read owner: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
            return False

        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                current = None

        if current != token:
            # ``refresh`` is intentionally a no-op for foreign tokens so callers can
            # detect leadership loss without extending another worker's lock.
            return False

        try:
            refreshed = bool(client.expire(self._key, ttl_value))
        except Exception:
            LOGGER.debug('Redis leader lock expire failed: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
            return False

        return refreshed

    def release(self, token: str) -> bool:
        try:
            client = self._client()
        except Exception:
            LOGGER.debug('Redis leader lock release failed to obtain client: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
            return False

        script = self._release_script
        if script is None:
            register = getattr(client, 'register_script', None)
            if callable(register):
                script = register(_RELEASE_SCRIPT)
                self._release_script = script

        if script is not None:
            try:
                result = script(keys=[self._key], args=[token], client=client)
            except Exception:
                LOGGER.debug('Redis leader lock release script failed: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
                return False
            ok = bool(result)
        else:
            try:
                current = client.get(self._key)
            except Exception:
                LOGGER.debug('Redis leader lock release fallback get failed: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
                return False
            if isinstance(current, bytes):
                try:
                    current = current.decode('utf-8')
                except Exception:
                    current = None
            if current != token:
                return False
            try:
                deleted = client.delete(self._key)
            except Exception:
                LOGGER.debug('Redis leader lock release fallback delete failed: key=%s token=%s', self._key, self._mask_token(token), exc_info=True)
                return False
            ok = bool(deleted)
        LOGGER.info('Leader lock release result: ok=%s key=%s token=%s', ok, self._key, self._mask_token(token))
        return ok

    def ttl(self) -> Optional[int]:
        try:
            result = self._client().ttl(self._key)
        except Exception:
            LOGGER.debug('Redis leader lock ttl lookup failed: key=%s', self._key, exc_info=True)
            return None
        if isinstance(result, int):
            return result
        return None
