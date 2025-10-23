#!/usr/bin/env python3
"""Migration helper to ensure songs manifest metadata exists.

This script creates the ``songs_manifest`` collection (and associated metadata
stub) so that the scanner fast-path can activate immediately after deployment.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


LOGGER = logging.getLogger(__name__)


def _coerce_hosts(host_values: Optional[list[str]]) -> list[str]:
    if not host_values:
        return ["127.0.0.1:27017"]
    hosts: list[str] = []
    for value in host_values:
        hosts.extend(token.strip() for token in str(value).split(",") if token.strip())
    return hosts or ["127.0.0.1:27017"]


def _safe_lookup(collection: Optional[Collection], filter_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if collection is None:
        return None
    try:
        document = collection.find_one(filter_doc)
    except Exception:
        LOGGER.debug("Failed to probe collection=%s filter=%s", getattr(collection, "name", "<unknown>"), filter_doc, exc_info=True)
        return None
    if isinstance(document, dict):
        payload = dict(document)
        payload.pop("_id", None)
        return payload
    return None


def _extract_manifest_payload(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    payload: Dict[str, Any] = {}
    if raw.get("checksum"):
        payload["checksum"] = str(raw["checksum"])
    if raw.get("manifest_checksum"):
        payload["manifest_checksum"] = str(raw["manifest_checksum"])
    files_count = raw.get("files_count")
    if isinstance(files_count, (int, float)):
        payload["files_count"] = int(files_count)
    manifest_documents = raw.get("manifest_documents")
    if isinstance(manifest_documents, (int, float)):
        payload["manifest_documents"] = int(manifest_documents)
    updated_at = raw.get("updated_at")
    if isinstance(updated_at, datetime):
        payload["updated_at"] = updated_at
    elif raw.get("updated_at"):
        payload["updated_at"] = datetime.now(timezone.utc)
    return payload


def _ensure_manifest_meta(db: Database) -> None:
    meta = getattr(db, "meta", None)
    if meta is None:
        meta = db["meta"]
    try:
        meta.create_index("_id", unique=True)
    except Exception:
        LOGGER.debug("Failed to ensure meta _id index", exc_info=True)

    existing = _safe_lookup(meta, {"_id": "songs_manifest"})
    if existing is not None:
        LOGGER.info("Existing songs_manifest metadata detected; no changes required")
        return

    legacy_source: Optional[Dict[str, Any]] = None
    metadata_collection = getattr(db, "metadata", None)
    if metadata_collection is not None:
        legacy_source = _safe_lookup(metadata_collection, {"_id": "songs_manifest"})
        if not legacy_source:
            legacy_source = _safe_lookup(metadata_collection, {"key": "songs_manifest"})
        if not legacy_source:
            legacy_source = _safe_lookup(metadata_collection, {"name": "songs_manifest"})

    payload = _extract_manifest_payload(legacy_source)
    payload.setdefault("checksum", "")
    payload.setdefault("manifest_checksum", payload.get("checksum", ""))
    payload.setdefault("files_count", 0)
    payload.setdefault("manifest_documents", payload.get("files_count", 0))
    payload.setdefault("updated_at", None)

    try:
        meta.update_one(
            {"_id": "songs_manifest"},
            {"$setOnInsert": payload},
            upsert=True,
        )
    except Exception:
        LOGGER.exception("Failed to upsert songs_manifest metadata document")
        raise
    else:
        LOGGER.info("Ensured songs_manifest metadata document present")


def _ensure_manifest_indexes(db: Database) -> None:
    manifest_collection = getattr(db, "songs_manifest", None)
    if manifest_collection is None:
        manifest_collection = db["songs_manifest"]
    try:
        manifest_collection.create_index("id", unique=True, name="songs_manifest_id_unique")
    except Exception:
        LOGGER.debug("Failed to ensure songs_manifest id index", exc_info=True)
    try:
        manifest_collection.create_index("title_lc", name="songs_manifest_title_lc")
    except Exception:
        LOGGER.debug("Failed to ensure songs_manifest title index", exc_info=True)
    try:
        manifest_collection.create_index("category", name="songs_manifest_category")
    except Exception:
        LOGGER.debug("Failed to ensure songs_manifest category index", exc_info=True)


def ensure_manifest_collection(db: Database) -> None:
    _ensure_manifest_indexes(db)
    _ensure_manifest_meta(db)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ensure the songs manifest metadata collection exists.")
    parser.add_argument("--uri", default=os.getenv("TAIKO_WEB_MONGO_URI"), help="MongoDB connection URI")
    parser.add_argument(
        "--host",
        action="append",
        help="MongoDB host:port entry (may be supplied multiple times)",
    )
    parser.add_argument("--database", default=os.getenv("TAIKO_WEB_MONGO_DB", "taiko"), help="Database name to use")
    args = parser.parse_args()

    hosts_env = os.getenv("TAIKO_WEB_MONGO_HOST")
    host_tokens: Optional[list[str]] = None
    if args.host:
        host_tokens = args.host
    elif hosts_env:
        host_tokens = [hosts_env]

    if args.uri:
        client = MongoClient(args.uri)
    else:
        client = MongoClient(host=_coerce_hosts(host_tokens))

    db = client[args.database]
    LOGGER.info("Ensuring songs manifest metadata for database=%s", args.database)
    ensure_manifest_collection(db)
    LOGGER.info("songs manifest metadata ensured")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
