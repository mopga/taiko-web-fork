"""Factories for constructing storage implementations."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Union

from storage.interfaces import LeaderLock, ManifestStore, SongStore
from storage.mongo_store import MongoManifestStore, MongoSongStore

from lock.redis_lock import RedisLeaderLock


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
        if run_profile == 'desktop':
            if file_leader_lock_path is None:
                return None
            from lock.file_lock import FileLeaderLock  # lazy import for optional dependency

            return FileLeaderLock(file_leader_lock_path)

        if redis_client_factory is not None:
            return RedisLeaderLock(redis_client_factory, leader_lock_key)

        return None

    leader_lock = _create_leader_lock()

    # ``run_profile`` primarily exists to support future desktop profiles. The
    # current implementation always returns the Mongo-backed stores while
    # letting the leader lock vary per profile.
    return StorageBundle(song_store=song_store, manifest_store=manifest_store, leader_lock=leader_lock)
