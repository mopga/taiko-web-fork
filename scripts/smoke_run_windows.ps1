Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:APP_PORT = "8000"
$env:DATA_DIR = Join-Path (Get-Location) "_data"
New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null
$env:RUN_PROFILE = "desktop"
$env:PROFILE = "desktop"

$songsDir = Join-Path (Get-Location) "_songs"
if (Test-Path $songsDir) {
    Remove-Item $songsDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $songsDir | Out-Null

$exe = "dist\backend\taiko-web-backend\taiko-web-backend.exe"
if (-not (Test-Path $exe)) {
    throw "Binary not found: $exe"
}

$logDir = Join-Path (Get-Location) "_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdoutLog = Join-Path $logDir "smoke_stdout.log"
$stderrLog = Join-Path $logDir "smoke_stderr.log"
Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$args = @("--host", "127.0.0.1", "--port", $env:APP_PORT, "--songs-dir", $songsDir)
$startParams = @{
    FilePath = $exe
    ArgumentList = $args
    PassThru = $true
    RedirectStandardOutput = $stdoutLog
    RedirectStandardError = $stderrLog
    WindowStyle = 'Hidden'
}
$process = Start-Process @startParams


try {
  # --- helpers (единственные, без дублей) ---
  function Assert($cond, $msg) { if (-not $cond) { throw $msg } }
  function Invoke-WithRetry([scriptblock]$Action, [int]$Retries = 30, [int]$DelayMs = 500) {
    for ($i=0; $i -lt $Retries; $i++) {
      try { return & $Action } catch { Start-Sleep -Milliseconds $DelayMs }
    }
    & $Action
  }
  function To-Array($v) {
    if ($null -eq $v) { return @() }
    if ($v -is [System.Collections.IEnumerable]) { return @($v) }
    return @($v)
  }

  $baseUrl = "http://127.0.0.1:$env:APP_PORT"

  # --- дождаться здоровья (status: ok) ---
  Invoke-WithRetry {
    $r = Invoke-WebRequest -UseBasicParsing "$baseUrl/healthz" -TimeoutSec 3
    if ($r.StatusCode -ne 200 -or ($r.Content -notmatch '"status"\s*:\s*"ok"')) { throw "healthz not ok" }
    $r
  } | Out-Null

  # --- мягко подождать появления каталога в логе (если лог уже есть) ---
  if (Test-Path $stdoutLog -PathType Leaf) {
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
      if (Select-String -Path $stdoutLog -Pattern 'profile=desktop\s+catalog_source=filesystem' -Quiet) { break }
      Start-Sleep -Seconds 1
    }
  }

  # --- корень HTML (SPA) ---
  $root = Invoke-WithRetry { Invoke-WebRequest -UseBasicParsing "$baseUrl/" -TimeoutSec 5 }
  Assert ($root.StatusCode -eq 200) "Root not 200: $($root.StatusCode)"
  $rootHtml = $root.Content
  $hasHtml = ($rootHtml -match '(?is)<!doctype') -or ($rootHtml -match '(?is)<html')
  Assert $hasHtml "Root HTML marker not found"

  # --- favicon (200 или 304 допустимы) ---
  $fav = Invoke-WithRetry { Invoke-WebRequest -UseBasicParsing "$baseUrl/favicon.ico" -Method Head -TimeoutSec 5 }
  Assert (@(200,304) -contains $fav.StatusCode) "favicon unexpected: $($fav.StatusCode)"

  # --- /api/songs ---
  $songsResp = Invoke-WithRetry { Invoke-WebRequest -UseBasicParsing "$baseUrl/api/songs" -TimeoutSec 5 }
  Assert ($songsResp.StatusCode -eq 200) "/api/songs not 200: $($songsResp.StatusCode)"
  try {
    $songsJson = $songsResp.Content | ConvertFrom-Json -ErrorAction Stop
  } catch {
    Write-Host "---- /api/songs raw body ----"
    Write-Host $songsResp.Content
    throw "Songs JSON parse failed: $($_.Exception.Message)"
  }
  $songs = To-Array $songsJson
  Assert ($songs -is [System.Collections.IEnumerable]) "/api/songs not enumerable"
  if ($songs.Count -gt 0) {
    Assert ($songs[0] -is [pscustomobject]) "/api/songs element is not object"
  }

  Write-Host "Desktop smoke OK: root+api passed. Songs: $($songs.Count)"
  exit 0
}
finally {
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
}
