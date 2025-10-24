"""MongoDB/Redis-backed storage implementations."""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

try:  # pragma: no cover - optional import for typing
    from pymongo.collection import Collection
except Exception:  # pragma: no cover - pymongo may be absent in type-checking
    Collection = Any  # type: ignore[misc]

from storage.interfaces import ManifestStore, SongStore


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

    def bulk_write(
        self,
        operations: Sequence[Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return self._collection().bulk_write(operations, *args, **kwargs)

    def count_documents(
        self,
        filter: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> int:
        return int(self._collection().count_documents(filter, *args, **kwargs))

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


