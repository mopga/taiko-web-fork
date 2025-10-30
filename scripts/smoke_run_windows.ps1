Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:APP_PORT = if ($env:APP_PORT) { $env:APP_PORT } else { "8000" }
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

$absRoot = Split-Path $absExe
$dataDir = (Resolve-Path -LiteralPath $env:DATA_DIR).Path
$songsRoot1 = Join-Path $absRoot "songs"
$songsRoot2 = Join-Path $dataDir "songs"
New-Item -ItemType Directory -Force -Path $songsRoot1, $songsRoot2 | Out-Null

$trackDir1 = Join-Path $songsRoot1 "test-track"
$trackDir2 = Join-Path $songsRoot2 "test-track"
foreach ($dir in @($trackDir1, $trackDir2)) {
    if (Test-Path $dir) {
        Remove-Item -Path $dir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
New-Item -ItemType Directory -Force -Path $trackDir1, $trackDir2 | Out-Null

$chartPath1 = Join-Path $trackDir1 "test.tja"
$chartPath2 = Join-Path $trackDir2 "test.tja"
$audioPath1 = Join-Path $trackDir1 "audio.ogg"
$audioPath2 = Join-Path $trackDir2 "audio.ogg"

$chartBody = @'
TITLE: Test Track
SUBTITLE: Smoke
WAVE: audio.ogg
OFFSET: 0
COURSE: Oni
LEVEL: 1
#START
1111,
0000,
2222,
0000,
3333,
0000,
4444,
0000,
#END
'@
Set-Content -Path $chartPath1 -Value $chartBody -Encoding UTF8
Set-Content -Path $chartPath2 -Value $chartBody -Encoding UTF8
[System.IO.File]::WriteAllBytes($audioPath1, [byte[]]::new(0))
[System.IO.File]::WriteAllBytes($audioPath2, [byte[]]::new(0))

$logDir = Join-Path (Get-Location) "_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdoutLog = Join-Path $logDir "smoke_stdout.log"
$stderrLog = Join-Path $logDir "smoke_stderr.log"
Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$port = $env:APP_PORT
$env:PORT = $port
$baseUrl = "http://127.0.0.1:$port"
$args = @("--host", "127.0.0.1", "--port", $port)
$startParams = @{
    FilePath = $absExe
    ArgumentList = $args
    WorkingDirectory = $absRoot
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

  try {
    Invoke-WebRequest -UseBasicParsing "$baseUrl/" -TimeoutSec 5 | Out-Null
  } catch {
    Start-Sleep -Milliseconds 200
  }

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

  # --- /api/songs --- ждём НЕ пустой список
  function Normalize-Songs([string]$body) {
    if ([string]::IsNullOrWhiteSpace($body)) { return @() }
    # безопасная нормализация строки (BOM/NUL)
    $b = $body -replace "`uFEFF","" -replace "`0",""
    try {
      $json = $b | ConvertFrom-Json -ErrorAction Stop
    } catch {
      return @()           # не парсится — считаем пустым
    }

    if ($null -eq $json) { return @() }
    if ($json -is [pscustomobject] -and ($json | Get-Member -Name items -ErrorAction SilentlyContinue)) {
      $items = $json.items
      if ($items -is [System.Array]) { return $items }
      if ($items -is [System.Collections.IEnumerable]) { return @($items) }
      return @()
    }
    if ($json -is [System.Array]) { return $json }
    if ($json -is [System.Collections.IEnumerable]) { return @($json) }
    return @()
  }

  $songs    = @()
  $deadline = (Get-Date).AddSeconds(60)
  do {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing "$baseUrl/api/songs" -TimeoutSec 5
      if ($resp.StatusCode -ne 200) { Start-Sleep -Milliseconds 500; continue }
      $songs = Normalize-Songs $resp.Content
    } catch {
      $songs = @()
    }
    if ($songs.Count -eq 0) { Start-Sleep -Milliseconds 500 }
  } while ($songs.Count -eq 0 -and (Get-Date) -lt $deadline)

  if ($songs.Count -eq 0) {
    Write-Host "---- /api/songs raw body (still empty) ----"
    try { $resp = Invoke-WebRequest -UseBasicParsing "$baseUrl/api/songs" -TimeoutSec 5; Write-Host $resp.Content } catch {}
    throw "No songs found in /api/songs after wait"
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
    if ($process) {
        try {
            if (-not $process.HasExited) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
            try {
                [void]$process.WaitForExit(5000)
            } catch {}
        } finally {
            try { $process.Dispose() } catch {}
        }
    }
}
