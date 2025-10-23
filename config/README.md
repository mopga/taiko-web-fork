# Scanner configuration

## SCAN_ON_START mode

`SCAN_ON_START` controls what the scanner does when the app boots:

- `auto` (default): calculate a filesystem digest, run an incremental scan only when files changed, otherwise exit early via the fast-path.
- `force`: ignore any cached manifest metadata and perform a full scan immediately.
- `skip`: do not start a scan during boot.

The value can be provided via configuration or the `SCAN_ON_START` environment variable. Legacy boolean values are still accepted and mapped to `force` (truthy) or `skip` (falsy).

## Songs manifest metadata

The scanner stores snapshot metadata in the `meta` collection using the document `_id = "songs_manifest"`. After a successful scan the document contains:

- `checksum`: SHA-1 digest over each file's relative path, size, and `mtime_ns`.
- `files_count`: number of files under the songs directory that contributed to the digest.
- `updated_at`: timestamp (UTC) of the last successful scan.

When the checksum and file count match the current filesystem digest the scanner logs `scan: fast-path (no changes)` and terminates without re-parsing any TJA or media files.
