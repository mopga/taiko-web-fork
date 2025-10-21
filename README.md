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

When running in production the scanner persists a songs manifest with a deterministic `manifest_checksum`. The `/api/songs` endpoint exposes this checksum as an HTTP `ETag` header and accepts `If-None-Match` requests to serve `304 Not Modified` responses when the catalog has not changed.

The new `/api/songs/details` endpoint accepts up to 50 comma-separated song identifiers and returns the detailed payloads in the same order. Pass `notes=none` to fetch metadata without full chart data.