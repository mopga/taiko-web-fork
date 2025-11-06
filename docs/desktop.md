# Desktop Tower/Dan playlist handling

The desktop profile recognises Tower and Dan (Dojo) playlist courses while
preserving the web client behaviour and the existing API contracts.

## Playlist metadata

When the scanner encounters a Tower/Dan playlist (case-insensitive `.m3u8`/
`.t3u8` detection), every aggregated chart exports the following metadata:

- `meta.is_playlist_course` — boolean flag that marks the course as an
  aggregated playlist.
- `meta.playlist_path` — POSIX-style relative path to the playlist inside the
  `songs/` directory.
- `meta.playlist_url` — HTTP URL pointing to the playlist under `songs_baseurl`.
- `audio_url` / `paths.audio_url` — unchanged; still references the playlist URL
  so the web client streams the HLS manifest exactly as before.

The metadata also includes `meta.segments` with the timing, WAVE filenames, and
source chart paths for each playlist segment.

## Desktop loading flow

On desktop (`isDesktopEnvironment()`), `public/src/js/loadsong.js` inspects
`meta.is_playlist_course` and, when present, fetches the aggregated chart from
`/api/tower/chart` or `/api/dan/chart`. The loader then resolves every entry in
`meta.segments`, pulls the referenced WAVE files relative to `meta.playlist_path`
or the segment's `tja_path`, and schedules them sequentially. The combined audio
duration is propagated to the controller, while the web client continues to rely
on `audio_url` and remains unaffected.
