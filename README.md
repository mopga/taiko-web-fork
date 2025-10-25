# TaikoWeb

This is reworked taiko-web version.

## Improvements

  - docker compose run once for all
  - add win desktop HTA app for running
  - add support for ./song directrory scanningg
  - add support for TJA files auto-patsing
  - add support for auto-adding songs to MongoDb at startup
  - add support for JP and EN songs

## How to negin

U need to install Docker for your system

## How to run

Make container:

```bash
docker compose build --pull --no-cache
```

Run it:

```bash
docker compose up -d
```


## Windows run app

Install WSL2
Install Docker Desktop

Git clone the repo

in repo directory run the file
```bash
start_taiko_edge.hta
```

## Environment

The song scanner and validator can be controlled via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `TJA_VALIDATION_MODE` | `warn` | Validation mode: `off`, `warn`, or `strict`. `strict` turns validation errors into scanner errors. |
| `TJA_VALIDATION_LOG` | `0` | Enable verbose validation logging when set to `1`. |
| `TJA_VALIDATION_SUMMARY` | `1` | Emit aggregated validation summaries when logging is enabled. |
| `SCAN_ON_START` | `auto` | Controls startup behaviour: `auto` (digest + incremental), `force`, or `skip`. |
| `SCAN_LEADER_TTL_SECONDS` | `300` | Expiration time (seconds) for the Redis leader lock key `taiko:scanner:leader`. |
| `SCAN_LEADER_REFRESH_SECONDS` | `75` | TTL refresh cadence; defaults to `TTL / 4` with a minimum of 10 seconds. |
| `SCAN_IO_THREADS` | `min(32, 2 × CPU)` | Maximum worker threads for reading chart headers. |
| `MONGO_BULK_BATCH` | `800` | Number of `UpdateOne` operations buffered before calling `bulk_write`. |
| `SCAN_PROGRESS_EVERY_SECONDS` | `5` | Minimum interval between INFO progress summaries. |
| `SCAN_PROGRESS_EVERY_FILES` | `0` | Optional file-count gate for progress logs; `0` disables file-based throttling. |
| `LEADER_CHECK_INTERVAL` | `200` | How many files to process before re-checking leadership. |

### Production hints

When running in production the scanner persists a songs manifest with a deterministic `manifest_checksum`. The `/api/songs` endpoint exposes this checksum as an HTTP `ETag` header and accepts `If-None-Match` requests to serve `304 Not Modified` responses when the catalog has not changed.

The new `/api/songs/details` endpoint accepts up to 50 comma-separated song identifiers and returns the detailed payloads in the same order. Pass `notes=none` to fetch metadata without full chart data.

### Scan on start

The scanner supports multiple startup modes via the `SCAN_ON_START` configuration value (or environment variable):

* `auto` (default) computes a lightweight filesystem digest at boot, compares it with the persisted manifest metadata stored in the MongoDB `meta` collection, and skips full parsing when nothing changed.
* `force` always performs a full rescan regardless of manifest state.
* `skip` disables automatic scanning entirely; use the admin scan API to refresh manually.

Metadata about the last successful scan lives in the `meta` collection as the `_id="songs_manifest"` document. The document tracks the deterministic `manifest_checksum`, the lightweight filesystem digest (`fs_checksum`), and the number of discovered song files (`manifest_documents`). When the digest and file counts match the persisted values the scanner enters a fast-path mode that avoids reparsing charts. Deployments that predate the manifest feature can run `python -m tools.migration_add_manifest_collection` to create the collection and backfill the metadata entry. Without that document the scanner will perform a full parse on the next boot.

Leader election for incremental rescans uses Redis (`taiko:scanner:leader`). The TTL can be tuned with `SCAN_LEADER_TTL_SECONDS` to accommodate longer scans. When Redis is not configured, workers still perform scans but cannot claim leadership, so the filesystem watcher remains disabled; enable Redis to allow a single process to run the watcher.

#### Scanner leader lifecycle

* The worker that wins leadership first writes `taiko:scanner:leader` with the token `<hostname>:<pid>` before computing filesystem digests or parsing charts. Followers exit early without touching the filesystem when the key already exists.
* Once leadership is established, a background refresher extends the TTL roughly every 60 seconds using `LeaderLock.refresh()`. The refresher treats mismatched tokens as a lock loss and leaves the key untouched to avoid extending another worker's lease.
* `LeaderLock.ttl()` is optional and may return `None`; callers treat that as "TTL unknown" and fall back to the configured TTL when logging metrics.
* When the scan completes—or if an exception aborts it—the refresher stops and `LeaderLock.release()` runs in a `finally` block to clean up Redis. Deployments without Lua support automatically fall back to a compare-and-delete release flow.

## Database maintenance

Run the index initialization utility after provisioning a fresh MongoDB deployment to guarantee all required taiko-web collections have the expected indexes:

```bash
python -m tools.init_db_schema --uri "mongodb://localhost:27017" --database taiko
```

The script also respects the `TAIKO_WEB_MONGO_URI`, `TAIKO_WEB_MONGO_HOST`, and `TAIKO_WEB_MONGO_DB` environment variables, so it can be invoked without arguments in most containerized environments.
