# Taiko Web Desktop Release Notes

Update this document before tagging a release. Provide a concise changelog for the Windows desktop build.

## Highlights

- Initial desktop installer workflow.

## v0.1.1

- Bundle and serve the desktop SPA, favicon, and API routes from the packaged backend so `/` always returns HTML.
- Added layered configuration for the songs directory (CLI flag, environment, desktop config, installer default `{app}\songs`).
- Updated the Windows installer to provision the songs folder and desktop config without removing user content on uninstall.
- Improved desktop smoke checks for HTTP endpoints and graceful shutdown.
- Log the resolved songs directory, scanner counters, and the full route map at startup for easier diagnostics.
- Automatically fall back to the filesystem catalog in the desktop profile when MongoDB is unavailable so `/api/songs` responds with an empty array until scanning completes.

## Known Issues

- None.
