"""Storage abstraction protocols used by taiko-web."""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

from lock.interfaces import LeaderLock


__all__ = ['SongStore', 'ManifestStore', 'LeaderLock']


class SongStore(Protocol):
    """Data access abstraction for song documents."""

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        ...

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        ...

    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        ...

    def insert_one(self, document: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        ...

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...

    def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...

    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...

    def bulk_write(
        self,
        operations: Sequence[Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...

    def delete_one(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        ...

    def delete_many(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        ...

    def count_documents(
        self,
        filter: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> int:
        ...

    def create_index(self, keys: Any, *args: Any, **kwargs: Any) -> Any:
        ...

    def drop_index(self, name: str, *args: Any, **kwargs: Any) -> Any:
        ...

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        ...


class ManifestStore(Protocol):
    """Data access abstraction for song manifest entries."""

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        ...

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        ...

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...

    def delete_many(self, filter: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        ...

    def bulk_write(self, operations: Sequence[Any], *args: Any, **kwargs: Any) -> Any:
        ...

    def create_index(self, keys: Any, *args: Any, **kwargs: Any) -> Any:
        ...

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        ...


