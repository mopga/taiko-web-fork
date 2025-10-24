"""Fallback leader lock implementation used when Redis is unavailable."""
from __future__ import annotations

from typing import Optional


class DummyLeaderLock:
    """A no-op leader lock that always succeeds but tracks ownership."""

    def __init__(self) -> None:
        self._owner: Optional[str] = None

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        self._owner = token
        return True

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        return self._owner == token

    def release(self, token: str) -> bool:
        if self._owner != token:
            return False
        self._owner = None
        return True

    def get_owner(self) -> Optional[str]:
        return self._owner
