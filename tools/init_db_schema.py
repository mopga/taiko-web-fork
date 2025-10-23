"""Utilities for initializing MongoDB indexes for taiko-web."""

from __future__ import annotations

import argparse
import logging
import os
from typing import Sequence

from pymongo import MongoClient
from pymongo.database import Database


LOGGER = logging.getLogger(__name__)


def init_db_schema(db: Database) -> None:
    """Ensure the MongoDB indexes required by taiko-web exist.

    The routine is idempotent and may be executed multiple times. It is safe to
    run during deployments or migrations to guarantee freshly provisioned
    databases receive the expected indexes.
    """

    db.users.create_index('username', unique=True)
    try:
        db.songs.drop_index('id_1')
    except Exception:
        pass
    try:
        db.songs.drop_index('songs_id_unique')
    except Exception:
        pass
    id_string_partial_filter = {'id': {'$type': 'string'}}
    db.songs.create_index(
        'id',
        unique=True,
        name='songs_id_unique',
        partialFilterExpression=id_string_partial_filter,
    )
    try:
        db.songs.drop_index('group_key_1')
    except Exception:
        pass
    try:
        db.songs.drop_index('songs_group_key_unique')
    except Exception:
        pass
    scanner_stable_string_partial_filter = {'scanner_stable_id': {'$type': 'string'}}
    try:
        db.songs.create_index(
            [('group_key', 1), ('scanner_stable_id', 1)],
            unique=True,
            name='songs_group_key_scanner_unique',
            partialFilterExpression=scanner_stable_string_partial_filter,
        )
    except Exception:
        LOGGER.debug('Could not ensure compound group/stable index')
    try:
        db.songs.drop_index('songs_scanner_stable_unique')
    except Exception:
        pass
    try:
        db.songs.create_index(
            'scanner_stable_id',
            unique=True,
            name='songs_scanner_stable_id_unique',
            partialFilterExpression=scanner_stable_string_partial_filter,
        )
    except Exception:
        LOGGER.debug('Could not ensure scanner stable id index')
    try:
        db.songs.create_index('group_key', name='songs_group_key_lookup')
    except Exception:
        LOGGER.debug('Could not ensure group_key lookup index')
    try:
        db.songs.create_index([('audioHash', 1), ('titleNormalized', 1)], unique=True, sparse=True)
    except Exception:
        LOGGER.debug('Could not ensure audioHash/titleNormalized index')
    try:
        db.songs.create_index('title_lc', name='songs_title_lc_index')
    except Exception:
        LOGGER.debug('Could not ensure title_lc index')
    try:
        db.songs.create_index('category', name='songs_category_index')
    except Exception:
        LOGGER.debug('Could not ensure category index')
    db.scores.create_index('username')
    try:
        db.song_scanner_state.create_index('tja_path', unique=True)
    except Exception:
        LOGGER.debug('Could not ensure song_scanner_state index')
    try:
        db.counters.update_one({'_id': 'songs'}, {'$setOnInsert': {'seq': 0}}, upsert=True)
    except Exception:
        LOGGER.debug('Could not ensure songs counter document')


def _coerce_hosts(host_values: Sequence[str] | None) -> list[str]:
    if not host_values:
        return ['127.0.0.1:27017']
    hosts: list[str] = []
    for value in host_values:
        hosts.extend(token.strip() for token in str(value).split(',') if token.strip())
    return hosts or ['127.0.0.1:27017']


def main() -> None:
    parser = argparse.ArgumentParser(description='Initialize MongoDB indexes for taiko-web.')
    parser.add_argument('--uri', default=os.getenv('TAIKO_WEB_MONGO_URI'), help='MongoDB connection URI')
    parser.add_argument(
        '--host',
        action='append',
        help='MongoDB host:port entry (may be provided multiple times, defaults to TAIKO_WEB_MONGO_HOST)',
    )
    parser.add_argument('--database', default=os.getenv('TAIKO_WEB_MONGO_DB', 'taiko'), help='Database name to use')
    args = parser.parse_args()

    hosts_env = os.getenv('TAIKO_WEB_MONGO_HOST')
    host_tokens: list[str] | None = None
    if args.host:
        host_tokens = args.host
    elif hosts_env:
        host_tokens = [hosts_env]

    if args.uri:
        client = MongoClient(args.uri)
    else:
        client = MongoClient(host=_coerce_hosts(host_tokens))

    db = client[args.database]
    LOGGER.info('Ensuring taiko-web indexes for database=%s', args.database)
    init_db_schema(db)
    LOGGER.info('Index initialization complete')


if __name__ == '__main__':
    main()
