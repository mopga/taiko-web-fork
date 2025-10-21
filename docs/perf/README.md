# Performance snapshots

```
$ curl -i http://localhost:8000/api/songs
HTTP/1.1 200 OK
ETag: "abc123"
Cache-Control: public, max-age=86400, stale-while-revalidate=600
...

$ curl -i http://localhost:8000/api/songs -H 'If-None-Match: "abc123"'
HTTP/1.1 304 NOT MODIFIED
ETag: "abc123"

$ python songs_scanner.py --warm
scan-summary: found=5234 inserted=0 updated=0 disabled=0 errors=0 skipped=5234 duration=2.781s checksum=def456

XHR batch: /api/songs/details?ids=song-18,song-19,song-20&notes=none (3 requests coalesced)
```
