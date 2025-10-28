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
    $baseUrl = "http://127.0.0.1:$env:APP_PORT"

    # --- helpers ---
    function Assert($cond, $msg) { if (-not $cond) { throw $msg } }

    function Invoke-WithRetry([scriptblock]$Action, [int]$Retries = 20, [int]$DelayMs = 500) {
      for ($i=0; $i -lt $Retries; $i++) {
        try { return & $Action } catch { Start-Sleep -Milliseconds $DelayMs }
      }
      & $Action
    }

    function To-Array($v) {
      if ($null -eq $v) { return @() }
      if ($v -is [string]) { return @($v) }
      if ($v -is [System.Collections.IEnumerable]) { return @($v) }
      return @($v)
    }

    # --- wait for health / start ---
    Invoke-WithRetry {
      $r = Invoke-WebRequest -UseBasicParsing "$baseUrl/healthz" -TimeoutSec 3
      if ($r.StatusCode -ne 200) { throw "healthz=$($r.StatusCode)" }
      $r
    } | Out-Null

    $catalogLogged = $false
    $deadline = (Get-Date).AddSeconds(30)
    while (-not $catalogLogged -and (Get-Date) -lt $deadline) {
        if (Test-Path $stdoutLog -PathType Leaf) {
            if (Select-String -Path $stdoutLog -Pattern 'profile=desktop catalog_source=filesystem' -Quiet) {
                $catalogLogged = $true
                break
            }
        }
        Start-Sleep -Seconds 1
    }
    if (-not $catalogLogged) {
        throw "Expected catalog source log not found"
    }

    # --- check root HTML ---
    $root = Invoke-WithRetry { Invoke-WebRequest -UseBasicParsing "$baseUrl/" -TimeoutSec 5 }
    Assert ($root.StatusCode -eq 200) "Root not 200: $($root.StatusCode)"

    # допускаем разные минификаторы: ищем doctype ИЛИ html-тег
    $rootHtml = $root.Content
    $hasHtml = ($rootHtml -match '(?is)<!doctype') -or ($rootHtml -match '(?is)<html')
    Assert $hasHtml "Root HTML marker not found"

    # --- check /api/songs JSON array (robust to single item) ---
    $songsResp = Invoke-WithRetry { Invoke-WebRequest -UseBasicParsing "$baseUrl/api/songs" -TimeoutSec 5 }
    Assert ($songsResp.StatusCode -eq 200) "/api/songs not 200: $($songsResp.StatusCode)"

    $songsJson = $songsResp.Content | ConvertFrom-Json
    $songs = To-Array $songsJson

    Assert ($songs -is [System.Collections.IEnumerable]) "/api/songs not enumerable"

    if ($songs.Count -gt 0) {
      $first = $songs[0]
      Assert ($first -is [pscustomobject]) "/api/songs element is not object"
    }

    Write-Host "Smoke OK: root+api passed. Songs: $($songs.Count)"

    Invoke-WebRequest -UseBasicParsing "$baseUrl/favicon.ico" -Method Head -TimeoutSec 5 | Out-Null

    try {
        $openApi = Invoke-WebRequest -Uri "$baseUrl/openapi.json" -TimeoutSec 5
        if ($openApi.StatusCode -eq 200) {
            Write-Host "openapi.json present"
        }
    } catch {
        if (-not ($_.Exception.Response.StatusCode -eq 404)) {
            throw $_
        }
        Write-Host "openapi.json not available; skipping"
    }

    Write-Host "Desktop backend smoke test passed"
    exit 0
} catch {
    Write-Error $_
    if (Test-Path $stdoutLog) {
        Write-Host "===== smoke_stdout.log (tail) ====="
        Get-Content $stdoutLog -Tail 200 | Write-Host
    }
    if (Test-Path $stderrLog) {
        Write-Host "===== smoke_stderr.log (tail) ====="
        Get-Content $stderrLog -Tail 200 | Write-Host
    }
    exit 1
} finally {
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
}
