"""Factories for constructing storage implementations."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Union

from storage.interfaces import LeaderLock, ManifestStore, SongStore
from storage.mongo_store import MongoManifestStore, MongoSongStore

from lock.redis_lock import RedisLeaderLock


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StorageBundle:
    """Grouped storage dependencies used by the application."""

    song_store: SongStore
    manifest_store: ManifestStore
    leader_lock: Optional[LeaderLock]


def create_storage_bundle(
    *,
    run_profile: str,
    mongo_database_factory: Callable[[], Any],
    redis_client_factory: Optional[Callable[[], Any]] = None,
    leader_lock_key: str = 'taiko:scanner:leader',
    file_leader_lock_path: Optional[Union[str, Path]] = None,
) -> StorageBundle:
    """Create a ``StorageBundle`` for the configured runtime profile."""

    def _songs_collection():
        database = mongo_database_factory()
        if database is None:
            raise RuntimeError('Mongo database is not available')
        return getattr(database, 'songs', None)

    def _manifest_collection():
        database = mongo_database_factory()
        if database is None:
            raise RuntimeError('Mongo database is not available')
        return getattr(database, 'songs_manifest', None)

    song_store: SongStore = MongoSongStore(_songs_collection)
    manifest_store: ManifestStore = MongoManifestStore(_manifest_collection)

    def _create_leader_lock() -> Optional[LeaderLock]:
        return _create_leader_lock_for_profile(
            run_profile=run_profile,
            redis_factory=redis_client_factory,
            leader_lock_key=leader_lock_key,
            file_leader_lock_path=file_leader_lock_path,
        )

    leader_lock = _create_leader_lock()

    # ``run_profile`` primarily exists to support future desktop profiles. The
    # current implementation always returns the Mongo-backed stores while
    # letting the leader lock vary per profile.
    return StorageBundle(song_store=song_store, manifest_store=manifest_store, leader_lock=leader_lock)


def _create_leader_lock_for_profile(
    *,
    run_profile: str,
    redis_factory: Optional[Callable[[], Any]] = None,
    leader_lock_key: str = 'taiko:scanner:leader',
    file_leader_lock_path: Optional[Union[str, Path]] = None,
) -> Optional[LeaderLock]:
    if run_profile == 'web':
        if redis_factory is not None:
            try:
                test_client = redis_factory()
                if test_client is None:
                    raise RuntimeError('Redis client factory returned None')
                test_client.ping()
            except Exception:
                from lock.dummy_lock import DummyLeaderLock  # lazy import to avoid optional dependency

                LOGGER.warning(
                    'Redis unavailable for leader lock; falling back to DummyLeaderLock for web profile',
                    exc_info=True,
                )
                LOGGER.info('Leader lock configured: DummyLeaderLock used key=%s', leader_lock_key)
                return DummyLeaderLock()
            LOGGER.info('Leader lock configured: RedisLeaderLock key=%s', leader_lock_key)
            return RedisLeaderLock(redis_factory, leader_lock_key)
        from lock.dummy_lock import DummyLeaderLock  # lazy import to avoid optional dependency

        LOGGER.warning('No Redis factory for web profile — falling back to DummyLeaderLock')
        LOGGER.info('Leader lock configured: DummyLeaderLock used key=%s', leader_lock_key)
        return DummyLeaderLock()

    if run_profile == 'desktop':
        if file_leader_lock_path is None:
            LOGGER.info(
                'Leader lock disabled: file_leader_lock_path not provided for desktop profile.',
            )
            return None
        from lock.file_lock import FileLeaderLock  # lazy import for optional dependency

        return FileLeaderLock(file_leader_lock_path)

    if redis_factory is not None:
        LOGGER.info('Leader lock configured: RedisLeaderLock key=%s', leader_lock_key)
        return RedisLeaderLock(redis_factory, leader_lock_key)

    LOGGER.info(
        'Leader lock disabled: Redis client factory is not available for run_profile=%s.',
        run_profile,
    )
    return None
