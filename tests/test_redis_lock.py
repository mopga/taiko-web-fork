import sys
import unittest
from pathlib import Path
from unittest import mock

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


class RedisLeaderLockAcquireLoggingTest(unittest.TestCase):
    def test_acquire_logs_single_message_per_failure_with_throttled_warnings(self):
        class _AlwaysFailRedis:
            def __init__(self):
                self.calls = 0

            def set(self, key, value, nx=False, ex=None):
                self.calls += 1
                raise RuntimeError('boom')

        client = _AlwaysFailRedis()
        lock = RedisLeaderLock(lambda: client, 'leader-key')

        with mock.patch.object(RedisLeaderLock, '_ACQUIRE_WARNING_THRESHOLD', 2), \
            mock.patch.object(RedisLeaderLock, '_ACQUIRE_WARNING_COOLDOWN_SECONDS', 10), \
            mock.patch('lock.redis_lock.time.monotonic', side_effect=[0.0, 1.0, 2.0, 3.0, 20.0]):
            with self.assertLogs('lock.redis_lock', level='DEBUG') as captured:
                for _ in range(5):
                    self.assertFalse(lock.acquire('worker-host-abcdef', ttl_seconds=30))

        self.assertEqual(client.calls, 5)
        self.assertEqual(len(captured.output), 5)
        levels = [entry.split(':', 1)[0] for entry in captured.output]
        self.assertEqual(levels, ['DEBUG', 'WARNING', 'DEBUG', 'DEBUG', 'WARNING'])
        self.assertIn('token=worker…', captured.output[0])
        self.assertIn('failures=2', captured.output[1])
        self.assertIn('failures=3', captured.output[4])


class RedisLeaderLockTokenMaskingTest(unittest.TestCase):
    def test_logs_never_expose_raw_tokens(self):
        token = 'worker-ABC12345'

        class _FailingAcquireRedis:
            def set(self, key, value, nx=False, ex=None):
                raise RuntimeError('boom')

        class _ScriptableRedis(_DummyRedisWithoutScripts):
            def register_script(self, script):
                def _script(keys=None, args=None, client=None):
                    # Simulate Lua release success while also cleaning up the key.
                    if keys:
                        self.delete(keys[0])
                    return 1

                return _script

        def _failing_client_factory():
            raise RuntimeError('no redis available')

        with self.assertLogs('lock.redis_lock', level='DEBUG') as captured:
            lock_acquire_fail = RedisLeaderLock(lambda: _FailingAcquireRedis(), 'leader-key')
            self.assertFalse(lock_acquire_fail.acquire(token, ttl_seconds=30))

            lock = RedisLeaderLock(lambda: _ScriptableRedis(), 'leader-key')
            self.assertTrue(lock.acquire(token, ttl_seconds=30))
            self.assertTrue(lock.release(token))

            lock_refresh_fail = RedisLeaderLock(_failing_client_factory, 'leader-key')
            self.assertFalse(lock_refresh_fail.refresh(token, ttl_seconds=30))

            lock_release_fail = RedisLeaderLock(_failing_client_factory, 'leader-key')
            self.assertFalse(lock_release_fail.release(token))

        combined = '\n'.join(captured.output)
        self.assertNotIn(token, combined)
        self.assertRegex(combined, r'token=[^,\s]*…')


if __name__ == '__main__':
    unittest.main()
