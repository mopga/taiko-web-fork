"""Fallback leader lock implementation used when Redis is unavailable."""


class DummyLeaderLock:
    """A no-op leader lock that always succeeds."""

    def acquire(self, *args, **kwargs):
        return True

    def refresh(self, *args, **kwargs):
        return True

    def release(self, *args, **kwargs):
        return True

    def get_owner(self):
        return "dummy"
