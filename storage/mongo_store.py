"""MongoDB/Redis-backed storage implementations."""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

try:  # pragma: no cover - optional import for typing
    from pymongo.collection import Collection
except Exception:  # pragma: no cover - pymongo may be absent in type-checking
    Collection = Any  # type: ignore[misc]

try:  # pragma: no cover - optional import for typing
    from redis import Redis
except Exception:  # pragma: no cover - redis optional at runtime
    Redis = Any  # type: ignore[misc,assignment]

from storage.interfaces import LeaderLock, ManifestStore, SongStore


class MongoSongStore(SongStore):
    """``SongStore`` implementation backed by a MongoDB collection."""

    def __init__(self, collection_factory: Callable[[], Collection]):
        self._collection_factory = collection_factory

    def _collection(self) -> Collection:
        collection = self._collection_factory()
        if collection is None:
            raise RuntimeError('Songs collection is not available')
        return collection

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        return self._collection().find(filter, projection, *args, **kwargs)

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        return self._collection().find_one(filter, projection, *args, **kwargs)

    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        return self._collection().find_one_and_update(filter, update, *args, **kwargs)

    def insert_one(self, document: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        return self._collection().insert_one(document, *args, **kwargs)

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return self._collection().update_one(filter, update, *args, **kwargs)

    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return self._collection().update_many(filter, update, *args, **kwargs)

    def delete_one(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        return self._collection().delete_one(filter, *args, **kwargs)

    def delete_many(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        return self._collection().delete_many(filter, *args, **kwargs)

    def create_index(self, keys: Any, *args: Any, **kwargs: Any) -> Any:
        return self._collection().create_index(keys, *args, **kwargs)

    def drop_index(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self._collection().drop_index(name, *args, **kwargs)

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        return self._collection().list_indexes(*args, **kwargs)


class MongoManifestStore(ManifestStore):
    """``ManifestStore`` implementation backed by a MongoDB collection."""

    def __init__(self, collection_factory: Callable[[], Collection]):
        self._collection_factory = collection_factory

    def _collection(self) -> Collection:
        collection = self._collection_factory()
        if collection is None:
            raise RuntimeError('Songs manifest collection is not available')
        return collection

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        return self._collection().find(filter, projection, *args, **kwargs)

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        return self._collection().find_one(filter, projection, *args, **kwargs)

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return self._collection().update_one(filter, update, *args, **kwargs)

    def delete_many(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        return self._collection().delete_many(filter, *args, **kwargs)

    def bulk_write(self, operations: Sequence[Any], *args: Any, **kwargs: Any) -> Any:
        return self._collection().bulk_write(operations, *args, **kwargs)

    def create_index(self, keys: Any, *args: Any, **kwargs: Any) -> Any:
        return self._collection().create_index(keys, *args, **kwargs)

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        return self._collection().list_indexes(*args, **kwargs)


class RedisLeaderLock(LeaderLock):
    """``LeaderLock`` backed by a Redis key."""

    def __init__(self, client_factory: Callable[[], Redis], key: str) -> None:
        self._client_factory = client_factory
        self._key = key

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

    def acquire(self, token: str, ttl_seconds: int) -> bool:
        result = self._client().set(self._key, token, nx=True, ex=ttl_seconds)
        return bool(result)

    def refresh(self, token: str, ttl_seconds: int) -> bool:
        client = self._client()
        current = client.get(self._key)
        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover - defensive decode
                current = None
        if current != token:
            return False
        return bool(client.expire(self._key, ttl_seconds))

    def release(self, token: str) -> bool:
        client = self._client()
        current = client.get(self._key)
        if isinstance(current, bytes):
            try:
                current = current.decode('utf-8')
            except Exception:  # pragma: no cover
                current = None
        if current != token:
            return False
        return bool(client.delete(self._key))
