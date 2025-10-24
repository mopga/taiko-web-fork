import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from lock.redis_lock import RedisLeaderLock


class _DummyRedisWithoutScripts:
    def __init__(self):
        self.store = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    def expire(self, key, ttl):
        return key in self.store

    def ttl(self, key):
        return 42 if key in self.store else -2

    def ping(self):
        return True


class RedisLeaderLockFallbackReleaseTest(unittest.TestCase):
    def setUp(self):
        self.client = _DummyRedisWithoutScripts()
        self.lock = RedisLeaderLock(lambda: self.client, 'leader-key')

    def test_release_without_lua_support_deletes_owned_lock(self):
        self.client.set('leader-key', 'token-1')

        released = self.lock.release('token-1')

        self.assertTrue(released)
        self.assertIsNone(self.client.get('leader-key'))

    def test_release_without_lua_support_ignores_foreign_token(self):
        self.client.set('leader-key', 'token-owner')

        released = self.lock.release('token-foreign')

        self.assertFalse(released)
        self.assertEqual('token-owner', self.client.get('leader-key'))


if __name__ == '__main__':
    unittest.main()
