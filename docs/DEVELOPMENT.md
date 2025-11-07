# Development notes

## Dan Dojo and Tower playlists

Desktop builds reuse the same `.t3u8` / `.m3u8` playlist extension for both
audio streams and segmented Dan Dojo / Tower charts. During scanning we look at
the playlist payload to determine the correct semantics:

* Playlists that list media segments such as `.ts`, `.aac`, `.mp3`, `.ogg`, …
  are treated as audio sources (HLS style).
* Playlists that list `.tja` or `.csv` files are treated as exam segment
  manifests. The scanner opens each referenced chart and combines their notes,
  measures, and durations into a single aggregated chart.

When a playlist is identified as a segment manifest the scanner stores the
resolved playlist path on the chart metadata. The aggregated chart contains a
`meta.segments` array describing each segment and a `meta.playlist_path` string
so that `/api/dan-chart` and `/api/tower-chart` callers receive the same
payloads as the web profile.

### Tower chart structure

Tower `.tja` charts rely on branching to expose difficulty previews while the
game always plays the Master branch. Every Tower chart must provide a
`#BRANCHSTART` block with `#N`, `#E`, and `#M` sections where the `#M` segment
contains the canonical gameplay notes. The scanner parses all three branches
but only persists the `#M` measures for API consumers (the remaining branches
are treated as visual aids for the in-game tower). Charts that omit the master
branch cannot be scanned successfully.

Keep playlist and audio files inside the configured `songs_root`. The scanner
validates every referenced path and ignores entries that escape this directory
so desktop packaging (including Electron builds) remains self-contained.
