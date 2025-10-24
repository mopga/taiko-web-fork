"""Factories for constructing storage implementations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from storage.interfaces import LeaderLock, ManifestStore, SongStore
from storage.mongo_store import MongoManifestStore, MongoSongStore, RedisLeaderLock


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

    leader_lock: Optional[LeaderLock] = None
    if redis_client_factory is not None:
        leader_lock = RedisLeaderLock(redis_client_factory, leader_lock_key)

    # ``run_profile`` is reserved for future desktop implementations. For now we
    # always return the Mongo-backed bundle while honouring the requested key.
    return StorageBundle(song_store=song_store, manifest_store=manifest_store, leader_lock=leader_lock)
