"""Locking interfaces used by taiko-web."""
from __future__ import annotations

from typing import Optional, Protocol


class LeaderLock(Protocol):
    """Distributed leader election lock abstraction."""

    def get_owner(self) -> Optional[str]:
        ...

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        ...

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        ...

    def release(self, token: str) -> bool:
        ...
