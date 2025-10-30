Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:APP_PORT = "8000"
$env:DATA_DIR = Join-Path (Get-Location) "_data"
New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null
$env:RUN_PROFILE = "desktop"
$env:PROFILE = "desktop"

$backendRoot = Join-Path (Get-Location) "standalone/dist/backend/taiko-web-backend"
$exe = Join-Path $backendRoot "taiko-web-backend.exe"
if (-not (Test-Path $exe)) {
    throw "Binary not found: $exe"
}

Write-Host "WorkingDir: $PWD"
Write-Host "Exe (before resolve): $exe"
$absExe = (Resolve-Path -LiteralPath $exe).Path
Write-Host "Exe (abs): $absExe"

$songsDir = Join-Path (Split-Path $absExe) "songs"
New-Item -ItemType Directory -Force -Path $songsDir | Out-Null

$logDir = Join-Path (Get-Location) "_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdoutLog = Join-Path $logDir "smoke_stdout.log"
$stderrLog = Join-Path $logDir "smoke_stderr.log"
Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$env:PORT = $env:APP_PORT
$args = @("--host", "127.0.0.1", "--port", $env:APP_PORT)
$startParams = @{
    FilePath = $absExe
    ArgumentList = $args
    WorkingDirectory = $backendRoot
    PassThru = $true
    RedirectStandardOutput = $stdoutLog
    RedirectStandardError = $stderrLog
    WindowStyle = 'Hidden'
}
$process = $null
try {
    $process = Start-Process @startParams -ErrorAction Stop
}
catch {
    if ($_.Exception -is [System.ComponentModel.Win32Exception]) {
        Write-Host ("Win32Exception.NativeErrorCode = {0}" -f $_.Exception.NativeErrorCode)
    }
    throw
}


try {
  # --- helpers (единственные, без дублей) ---
  function Assert($cond, $msg) { if (-not $cond) { throw $msg } }
  function Invoke-WithRetry([scriptblock]$Action, [int]$Retries = 30, [int]$DelayMs = 500) {
    for ($i=0; $i -lt $Retries; $i++) {
      try { return & $Action } catch { Start-Sleep -Milliseconds $DelayMs }
    }
    & $Action
  }

  $baseUrl = "http://127.0.0.1:$env:APP_PORT"

  # --- дождаться здоровья (status: ok) ---
  $health = Invoke-WithRetry {
    Invoke-RestMethod -Uri "$baseUrl/healthz" -Method GET -TimeoutSec 3
  }
  Assert ($health.status -eq 'ok') "healthz status != ok: $($health | ConvertTo-Json -Compress)"
  Assert ($health.profile -eq 'desktop') "unexpected profile: $($health.profile)"
  $dbPath = $health.db_path
  Assert ($dbPath) "db_path missing from /healthz payload"
  Assert (Test-Path -LiteralPath $dbPath) "db_path does not exist: $dbPath"
  $dbResolved = (Resolve-Path -LiteralPath $dbPath).Path
  $expectedRoot = (Resolve-Path -LiteralPath $env:DATA_DIR).Path
  Assert ($dbResolved.StartsWith($expectedRoot, [System.StringComparison]::OrdinalIgnoreCase)) "db_path not under DATA_DIR"

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
  # Нормализация к массиву (устойчиво к 0/1/N элементов)
  if ($null -eq $songsJson) {
    $songs = @()
  }
  elseif ($songsJson -is [System.Array]) {
    $songs = $songsJson
  }
  else {
    $songs = @($songsJson)
  }

  if ($songs.Count -gt 0) {
    Assert ($songs[0] -is [pscustomobject]) "/api/songs element is not object"
    $first = $songs[0]
    Assert ($first.is_playable) "First song is not playable"
    $difficulties = $first.difficulties
    Assert ($null -ne $difficulties) "First song missing difficulties"
    $diffProps = @()
    if ($difficulties -is [pscustomobject]) {
      $diffProps = ($difficulties | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name)
    } elseif ($difficulties -is [System.Collections.IDictionary]) {
      $diffProps = $difficulties.Keys
    }
    Assert ($diffProps.Count -gt 0) "First song has no difficulties"
    foreach ($name in $diffProps) {
      $value = $difficulties.$name
      if ($difficulties -is [System.Collections.IDictionary]) {
        $value = $difficulties[$name]
      }
      Assert ($value -is [pscustomobject] -or $value -is [System.Collections.IDictionary]) "Difficulty $name is not an object"
    }
  }

  Write-Host "Smoke OK: /api/songs count=$($songs.Count)"
  Write-Host "Desktop smoke OK: root+api passed."
  exit 0
}
catch {
  Write-Error $_
  try {
    $errRecord = $_
    $exception = $errRecord.Exception
    $statusCode = $null
    $requestUri = $null
    $responseBody = $null

    if ($exception -is [Microsoft.PowerShell.Commands.HttpResponseException]) {
      $httpResponse = $exception.Response
      if ($httpResponse) {
        try { $statusCode = [int]$httpResponse.StatusCode } catch {}
        try { $requestUri = $httpResponse.RequestMessage.RequestUri } catch {}
        try { $responseBody = $httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult() } catch {}
      }
    }
    elseif ($exception -is [System.Net.WebException]) {
      $httpResponse = $exception.Response
      if ($httpResponse -is [System.Net.HttpWebResponse]) {
        try { $statusCode = [int]$httpResponse.StatusCode } catch {}
        try { $requestUri = $httpResponse.ResponseUri } catch {}
        try {
          $stream = $httpResponse.GetResponseStream()
          if ($stream) {
            try {
              $reader = New-Object System.IO.StreamReader($stream)
              $responseBody = $reader.ReadToEnd()
            }
            finally {
              if ($reader) { $reader.Dispose() }
              $stream.Dispose()
            }
          }
        } catch {}
      }
    }
    elseif ($errRecord.ErrorDetails -and $errRecord.ErrorDetails.Message) {
      $responseBody = $errRecord.ErrorDetails.Message
    }

    if ($statusCode -or $requestUri -or $responseBody) {
      Write-Host "===== HTTP error details ====="
      if ($statusCode -ne $null) { Write-Host "StatusCode: $statusCode" }
      if ($requestUri) { Write-Host "RequestUri: $requestUri" }
      if ($responseBody) {
        Write-Host "---- Response body ----"
        Write-Host $responseBody
      }
    }
  } catch {
    Write-Host "Failed to extract HTTP error details: $($_.Exception.Message)"
  }
  if (Test-Path $stdoutLog) {
    Write-Host "===== smoke_stdout.log (tail) ====="
    Get-Content $stdoutLog -Tail 200 | Write-Host
  }
  if (Test-Path $stderrLog) {
    Write-Host "===== smoke_stderr.log (tail) ====="
    Get-Content $stderrLog -Tail 200 | Write-Host
  }
  exit 1
}
finally {
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
}
