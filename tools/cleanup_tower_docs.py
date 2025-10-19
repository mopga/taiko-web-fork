#!/usr/bin/env python3
"""Clean legacy tower/dan chart documents and trigger a targeted rescan."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Set

from pymongo import MongoClient
from pymongo.database import Database

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from songs_scanner import SongScanner  # noqa: E402


LOGGER = logging.getLogger("cleanup")


def _load_config_module():
    module_name = os.environ.get("TAIKO_WEB_CONFIG_MODULE")
    search_order: List[str] = []
    if module_name:
        search_order.append(module_name)
    search_order.extend(["config.config", "config"])

    for name in search_order:
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue

    path_candidates = [
        Path(os.environ.get("TAIKO_WEB_CONFIG_PATH", "config.py")),
        ROOT_DIR / "config" / "config.py",
    ]
    for config_path in path_candidates:
        if not config_path.exists():
            continue
        spec = importlib.util.spec_from_file_location("config", config_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
            return module

    raise FileNotFoundError("Unable to locate configuration module")


_CONFIG = _load_config_module()


def _take_config(name: str, default: Optional[object] = None) -> Optional[object]:
    if hasattr(_CONFIG, name):
        return getattr(_CONFIG, name)
    return default


def _mongo_database() -> Database:
    mongo_cfg = dict(_take_config("MONGO", {}) or {})
    mongo_uri = os.environ.get("TAIKO_WEB_MONGO_URI") or mongo_cfg.get("uri")
    mongo_host = os.environ.get("TAIKO_WEB_MONGO_HOST") or mongo_cfg.get("host")
    if mongo_uri:
        client = MongoClient(mongo_uri)
    else:
        if not mongo_host:
            mongo_host = ["127.0.0.1:27017"]
        client = MongoClient(host=mongo_host)
    db_name = (
        os.environ.get("TAIKO_WEB_MONGO_DB")
        or mongo_cfg.get("database")
        or "taiko"
    )
    return client[db_name]


def _collect_dirty_groups(db: Database) -> Set[str]:
    dirty_keys: Set[str] = set()
    cursor = db.songs.find(
        {"managed_by_scanner": True},
        {"_id": False, "group_key": True, "charts": True, "title": True},
    )
    for doc in cursor:
        charts = doc.get("charts") if isinstance(doc.get("charts"), list) else []
        if charts and all(not isinstance(chart, dict) or not chart for chart in charts):
            if isinstance(doc.get("group_key"), str):
                dirty_keys.add(doc["group_key"])
            continue
        for chart in charts:
            if not isinstance(chart, dict):
                continue
            mode_value = str(chart.get("mode") or "").strip().casefold()
            if mode_value not in {"tower", "dan"}:
                continue
            display = chart.get("display_course")
            if isinstance(display, str) and display:
                continue
            group_key = doc.get("group_key")
            if isinstance(group_key, str) and group_key:
                dirty_keys.add(group_key)
                break
    return dirty_keys


def _purge_documents(db: Database, group_keys: Set[str], *, apply: bool) -> Set[str]:
    if not group_keys:
        return set()
    state_collection = getattr(db, "song_scanner_state", None)
    removed_paths: Set[str] = set()
    if not apply:
        LOGGER.info(
            "Dry run: skipping deletion of %d dirty song documents", len(group_keys)
        )
    else:
        LOGGER.info("Deleting %d dirty song documents", len(group_keys))
        db.songs.delete_many({"group_key": {"$in": list(group_keys)}})
    if state_collection is not None:
        state_docs = list(
            state_collection.find({"group_key": {"$in": list(group_keys)}})
        )
        for doc in state_docs:
            path_value = doc.get("tja_path")
            if isinstance(path_value, str) and path_value:
                removed_paths.add(path_value)
        if apply:
            state_collection.delete_many({"group_key": {"$in": list(group_keys)}})
        else:
            LOGGER.info(
                "Dry run: skipping prune of %d scanner state entries",
                len(state_docs),
            )
    return removed_paths


def _build_scanner(db: Database) -> SongScanner:
    songs_dir_value = (
        os.environ.get("SONGS_DIR")
        or _take_config("SONGS_DIR")
        or str(ROOT_DIR / "public" / "songs")
    )
    songs_dir = Path(songs_dir_value)
    songs_baseurl = os.environ.get("SONGS_BASEURL") or _take_config("SONGS_BASEURL") or "/songs/"
    ignore_globs = _take_config("SCAN_IGNORE_GLOBS") or ["**/.DS_Store", "**/Thumbs.db"]
    coerce_unknown = os.environ.get("COERCE_UNKNOWN_COURSE") or _take_config("COERCE_UNKNOWN_COURSE")
    return SongScanner(
        db=db,
        songs_dir=songs_dir,
        songs_baseurl=str(songs_baseurl),
        ignore_globs=ignore_globs,
        coerce_unknown_course=coerce_unknown,
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cleanup tower/dan documents and optionally trigger a rescan",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform deletions and rescan instead of a dry run",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    db = _mongo_database()
    dirty_groups = _collect_dirty_groups(db)
    if not dirty_groups:
        LOGGER.info("No dirty tower/dan documents detected")
        return 0

    if not args.apply:
        LOGGER.info(
            "Dry run: %d dirty groups would be deleted", len(dirty_groups)
        )
        for group_key in sorted(dirty_groups):
            LOGGER.info("Dry run: would delete group %s", group_key)

    removed_paths = _purge_documents(db, dirty_groups, apply=args.apply)
    if removed_paths:
        if args.apply:
            LOGGER.info("Pruned scanner state for %d charts", len(removed_paths))
        else:
            LOGGER.info(
                "Dry run: would prune scanner state for %d charts",
                len(removed_paths),
            )
    else:
        LOGGER.info("No scanner state entries require pruning")

    if not args.apply:
        LOGGER.info("Dry run: skipping targeted rescan; rerun with --apply to execute")
        return 0

    scanner = _build_scanner(db)
    LOGGER.info("Starting targeted rescan for %d groups", len(dirty_groups))
    summary = scanner.scan(full=False)
    LOGGER.info("Rescan summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
