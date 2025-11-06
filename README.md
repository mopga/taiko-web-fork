# TaikoWeb

This is reworked taiko-web version.

## Improvements

  - docker compose run once for all
  - add win desktop bundle building
  - add support for ./song directrory scanningg
  - add support for TJA files auto-patsing
  - add support for auto-adding songs to MongoDb at startup
  - add support for JP and EN songs


## How to run DEV docker mode

Make container:

```bash
docker compose build --pull --no-cache
```

Run it:

```bash
docker compose up -d
```

## Desktop run for DEV

The desktop profile stores persistent state under `DATA_DIR` (a per-user data
directory when the variable is not set). Sessions live in `$DATA_DIR/sessions`,
the SQLite database in `$DATA_DIR/taiko.db`, and logs default to
`$DATA_DIR/logs` when file logging is enabled. Song charts are **not** read from
`DATA_DIR`: the scanner always watches the `songs/` folder next to the running
backend binary (when developing from sources that resolves to `<repo-root>/songs`).

### Quick start standalone DEV version

```bash
# Unix/macOS shell
RUN_PROFILE=desktop python -m standalone.run_desktop --port 8000

# Windows PowerShell
$env:RUN_PROFILE="desktop"; python -m standalone.run_desktop --port 8000

# Windows cmd.exe
set RUN_PROFILE=desktop
python -m standalone.run_desktop --port 8000
```

To relocate the SQLite database and session files, set `DATA_DIR` before
launching the backend (for example `export DATA_DIR=/path/to/taiko-data`).

## Desktop binaries

Pre-built desktop packages are published by the CI workflow:

- Pull requests and feature branches: downloadable artifacts attached to each
  workflow run.
- Nightly builds (scheduled and pushes to `main`): GitHub Release marked as a
  pre-release.
- Tagged releases (`v*`): full GitHub Release with installers and archives.

### Desktop Release (manual)

- Запуск: GitHub → Actions → `desktop-release` → **Run workflow**
- Параметры:
  - `tag_name` (необяз.) — если пусто, будет сгенерирован `nightly-YYYYMMDD-HHMM`
  - `prerelease` (bool) — по умолчанию `true`
  - `draft` (bool) — по умолчанию `false`
- Артефакты раскладываются по платформенным подпапкам внутри `upload/` (Windows/macOS/Linux).
- Требования:
  - В Settings → Actions → General → Workflow permissions включено **Read and write permissions**.

Grab them from the [Releases page](../../releases)
or from the workflow run summary. Each release ships the following files:

- **Windows installer** – `taiko-web-backend-setup-<version>.exe` installs the
  backend under `%APPDATA%\taiko-web-backend` and creates shortcuts that launch
  the desktop profile on port 8000.
- **Windows portable zip** – `taiko-web-backend-windows.zip` contains the
  `taiko-web-backend` folder produced by PyInstaller. Extract it anywhere, then
  run:

  ```powershell
  Expand-Archive taiko-web-backend-windows.zip
  cd taiko-web-backend
  ```

  ```cmd
  set RUN_PROFILE=desktop && taiko-web-backend.exe --port 8000
  ```

- **Linux archive** – `taiko-web-backend-linux-x64.tar.gz` expands to a
  `taiko-web-backend` directory with the self-contained binary. Launch it with:

  ```bash
  tar -xzf taiko-web-backend-linux-x64.tar.gz
  cd taiko-web-backend
  RUN_PROFILE=desktop ./taiko-web-backend --port 8000
  ```

All desktop builds honour the `DATA_DIR` environment variable. When it is not
set the backend falls back to a platform-specific per-user data directory for
the SQLite database and session files. Songs are bundled and discovered
exclusively from the `songs/` directory next to the backend binary (for any
packaging format, including installers, zips, and tarballs):

- Windows: `<install-dir>\songs\`
- macOS/Linux: `<install-dir>/songs/`

Drop each `.tja`/`.tjc` chart into its own subdirectory inside that `songs/`
folder and restart the backend to rescan the library.

> **Note**
> The Windows binaries are unsigned, so SmartScreen may display a warning. Use
> "More info" → "Run anyway" to continue.

### Tower/Dan playlist metadata

The desktop scanner recognises Tower/Dan (Dojo) playlists regardless of the
letter case of the `.m3u8`/`.t3u8` extension. When a course is aggregated from a
playlist the exported chart entries include additional metadata:

- `meta.is_playlist_course` – boolean flag that signals to the clients that the
  course is composed from playlist segments.
- `meta.playlist_path` – relative filesystem path (POSIX-style) to the detected
  playlist within the `songs/` directory.
- `meta.playlist_url` – HTTP URL for the playlist file under `songs_baseurl`.
- `audio_url` (top-level as well as `paths.audio_url`) – continues to point to
  the same playlist URL so the web client can keep streaming the HLS manifest
  exactly as before.

Each aggregated chart also exposes `meta.segments` with per-segment timing,
source chart paths, and WAVE filenames. The desktop front-end calls
`/api/tower/chart` or `/api/dan/chart` when `meta.is_playlist_course` is
present, then sequentially loads and schedules the segment audio described by
`meta.segments`. This keeps the desktop behaviour in sync with playlist
aggregation while preserving HLS playback on the web via `audio_url`.

### Prereqs
- Python 3.10+ (verified on 3.11/3.12)
- Windows: `pip install --upgrade pip` and ensure `python`/`pip` are in `PATH`
- macOS: `xcode-select --install` (toolchain for native extensions, if needed)
- Linux:
  - Debian/Ubuntu: `python3-dev`, `build-essential`
  - Fedora/RHEL/CentOS: `python3-devel`, `gcc`

### Install
```bash
# from the repository root
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Windows cmd.exe: .venv\Scripts\activate.bat
# Unix/macOS:
source .venv/bin/activate

pip install -U pip wheel
pip install -r requirements.txt
# Optional: waitress instead of uvicorn
# pip install waitress

# First run (Desktop)
# profile and directories can be set explicitly
export RUN_PROFILE=desktop
# Optional: override where the SQLite DB and sessions live
# export DATA_DIR="/path/to/taiko-data"
# Windows PowerShell:
# $env:RUN_PROFILE="desktop"
# $env:DATA_DIR="C:\\path\\to\\taiko-data"  # optional
# Windows cmd.exe:
# set RUN_PROFILE=desktop
# set DATA_DIR=C:\path\to\taiko-data  # optional

# start the local server (uvicorn by default)
python -m standalone.run_desktop --port 8000
# or force waitress:
# python -m standalone.run_desktop --server=waitress --port 8000
```

#### Server options

| Flag value | Default | Notes |
| --- | --- | --- |
| `uvicorn` | ✅ | ASGI server with uvloop support when available. Recommended for best performance. |
| `waitress` |  | Pure-Python WSGI server; choose when uvicorn/uvloop wheels are not available. |

Without `--server`, the runner selects `uvicorn`. You can also override the
server via `TAIKO_DESKTOP_SERVER`.

Open: http://127.0.0.1:8000/healthz — it should respond with:

```
{"status": "ok", "profile": "desktop", "db_path": "<DATA_DIR>/taiko.db"}
```

The desktop profile disables Mongo-backed features; the `/healthz` response
surfaces the embedded SQLite backend and the absolute path to the database file.

Then visit http://127.0.0.1:8000/ — the web UI ships with the backend and works
out of the box without an additional Node/webpack build step.

### Managing data

- SQLite DB: `${DATA_DIR}/taiko.db`
- Sessions: `${DATA_DIR}/sessions`
- Logs/cache: `${DATA_DIR}/logs` (if you enable file logging)
- Songs: `<install-dir>/songs` (when running from sources this resolves to `<repo-root>/songs`)

### Adding songs

The desktop profile scans `<app-dir>/songs/`, where `<app-dir>` is the directory
containing the running backend binary. When working from sources this resolves
to `<repo-root>/songs/`; packaged builds include an empty `songs/` folder next to
the executable. Typical installation targets:

- **Windows** installer/portable ZIP: `<install-dir>\backend\taiko-web-backend\songs`
- **macOS** `.app`: `Taiko Web Desktop.app/Contents/Resources/backend/taiko-web-backend/songs`
- **Linux** AppImage/ZIP: `<install-dir>/backend/taiko-web-backend/songs`

You can also use the “Open Songs Folder” menu entry inside the desktop app to
jump directly to the correct directory. Each song belongs in its own directory
that contains a `.tja` or `.tjc` chart file and optional audio/background
assets:

```
taiko-web-backend/
└── songs/
    ├── MySong1/
    │   ├── MySong1.tja
    │   ├── MySong1.ogg
    │   └── bg.jpg
    └── MySong2/
        └── song.tjc
```

After adding or updating songs, restart the desktop server to rescan the
collection. (Automatic hot reload may arrive in a future build.)

### Common issues

- Port already in use: run with another port `--port 8010` or stop the conflicting process.
- Permission denied (sessions dir): ensure the process can write to `${DATA_DIR}`.
- ModuleNotFoundError at startup: activate the virtualenv (`.venv\Scripts\Activate.ps1`, `.venv\Scripts\activate.bat`, or `source .venv/bin/activate`).
- Windows console encoding: set UTF-8 mode via `setx PYTHONUTF8 1` (persisted) or `set PYTHONUTF8=1` (current session).


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
| `SCAN_IO_THREADS` | `min(32, 2 × CPU)` | Maximum worker threads for filesystem digest and header parsing. |
| `SCAN_WRITER_THREADS` | `1` | Mongo bulk write worker threads; values > 1 need careful collision handling. |
| `SCAN_OPS_QUEUE_MAX` | `20000` | Capacity of the parse → writer queue before backpressure kicks in. |
| `SCAN_BATCH_MAX_OPS` | `1000` | Maximum number of operations coalesced into a single Mongo `bulk_write`. |
| `SCAN_BATCH_FLUSH_SECONDS` | `0.75` | Force-flush interval when the current batch is not yet full. |
| `SCAN_PROGRESS_EVERY_SECONDS` | `5` | Minimum interval between INFO progress summaries. |
| `SCAN_PROGRESS_EVERY_FILES` | `0` | Optional file-count gate for progress logs; `0` disables file-based throttling. |
| `LEADER_CHECK_INTERVAL` | `200` | How many files to process before re-checking leadership. |

### Performance notes (first start)

The first full scan now pipelines the heavy stages to minimise wall-clock time:

* The filesystem digest walker enumerates files once and hashes metadata in parallel using `SCAN_IO_THREADS` workers.
* Chart parsing feeds a bounded queue that one or more writer threads consume, issuing Mongo `bulk_write` calls in batches governed by `SCAN_BATCH_MAX_OPS` and `SCAN_BATCH_FLUSH_SECONDS`.
* Regular progress summaries include queue depth to surface backpressure; tune `SCAN_OPS_QUEUE_MAX`, `SCAN_WRITER_THREADS`, and the batch settings to match your MongoDB deployment.

These defaults are conservative and safe on commodity hardware, but larger deployments can increase the knobs to accelerate cold-start imports.

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
