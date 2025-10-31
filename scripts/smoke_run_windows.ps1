Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-SeqCount([object]$x) {
  if ($null -eq $x) { return 0 }
  if ($x -is [System.Array]) { return $x.Count }
  if ($x -is [System.Collections.IEnumerable]) { return @($x).Count }
  return 0
}

function Normalize-Songs([string]$body) {
  if ([string]::IsNullOrWhiteSpace($body)) { return @() }
  $b = $body -replace '^\uFEFF','' -replace '\x00',''
  try { $json = $b | ConvertFrom-Json -ErrorAction Stop } catch { return @() }
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

$env:RUN_PROFILE = "desktop"
$env:PROFILE = "desktop"

$repoRoot = (Get-Location)
if ([string]::IsNullOrWhiteSpace($env:DATA_DIR)) {
  $env:DATA_DIR = Join-Path $repoRoot "_data"
}
New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null

$backendRoot = Join-Path $repoRoot "standalone/dist/backend/taiko-web-backend"
$exe = Join-Path $backendRoot "taiko-web-backend.exe"
if (-not (Test-Path -LiteralPath $exe)) {
  throw "Binary not found: $exe"
}

$absExe = (Resolve-Path -LiteralPath $exe).Path
$absRoot = Split-Path -Parent $absExe

$portCandidate = $null
if (-not [string]::IsNullOrWhiteSpace($env:PORT)) { $portCandidate = $env:PORT }
elseif (-not [string]::IsNullOrWhiteSpace($env:APP_PORT)) { $portCandidate = $env:APP_PORT }
else { $portCandidate = (Get-Random -Minimum 20000 -Maximum 40000).ToString() }
$port = $portCandidate
$env:PORT = $port
$baseUrl = "http://127.0.0.1:$port"

$logDir = Join-Path $repoRoot "_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdoutLog = Join-Path $logDir "smoke_stdout.log"
$stderrLog = Join-Path $logDir "smoke_stderr.log"
Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$dataDir = (Resolve-Path -LiteralPath $env:DATA_DIR).Path
$songsRoot1 = Join-Path $absRoot "songs"
$songsRoot2 = Join-Path $dataDir "songs"
New-Item -ItemType Directory -Force -Path $songsRoot1, $songsRoot2 | Out-Null

$trackDir1 = Join-Path $songsRoot1 "test-track"
$trackDir2 = Join-Path $songsRoot2 "test-track"
foreach ($dir in @($trackDir1, $trackDir2)) {
  if (Test-Path -LiteralPath $dir) {
    Remove-Item -Path $dir -Recurse -Force -ErrorAction SilentlyContinue
  }
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

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

$chartPath1 = Join-Path $trackDir1 "test.tja"
$chartPath2 = Join-Path $trackDir2 "test.tja"
$audioPath1 = Join-Path $trackDir1 "audio.ogg"
$audioPath2 = Join-Path $trackDir2 "audio.ogg"
Set-Content -Path $chartPath1 -Value $chartBody -Encoding UTF8
Set-Content -Path $chartPath2 -Value $chartBody -Encoding UTF8
[System.IO.File]::WriteAllBytes($audioPath1, [byte[]]::new(0))
[System.IO.File]::WriteAllBytes($audioPath2, [byte[]]::new(0))

Write-Host "WorkingDir: $absRoot"
Write-Host "Exe: $absExe"
Write-Host "Port: $port"

$startParams = @{
  FilePath = $absExe
  ArgumentList = @("--host", "127.0.0.1", "--port", $port)
  WorkingDirectory = $absRoot
  PassThru = $true
  RedirectStandardOutput = $stdoutLog
  RedirectStandardError = $stderrLog
  WindowStyle = 'Hidden'
}

$process = $null
$resp = $null
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
  Start-Sleep -Milliseconds 200

  $health = $null
  $deadline = (Get-Date).AddSeconds(60)
  do {
    try {
      $healthResponse = Invoke-WebRequest -UseBasicParsing "$baseUrl/healthz" -TimeoutSec 5
      if ($healthResponse.StatusCode -ne 200) { Start-Sleep -Milliseconds 500; continue }
      $health = $healthResponse.Content | ConvertFrom-Json -ErrorAction Stop
    } catch {
      $health = $null
    }
    if ($null -eq $health) { Start-Sleep -Milliseconds 500 }
  } while (($null -eq $health) -and (Get-Date) -lt $deadline)

  if ($null -eq $health) {
    throw "Timed out waiting for /healthz"
  }
  if ($health.status -ne 'ok') {
    throw "healthz status != ok: $($health | ConvertTo-Json -Compress)"
  }
  if ($health.profile -ne 'desktop') {
    throw "healthz profile != desktop: $($health.profile)"
  }
  $dbPath = $health.db_path
  if ([string]::IsNullOrWhiteSpace($dbPath)) {
    throw "db_path missing from /healthz payload"
  }
  if (-not (Test-Path -LiteralPath $dbPath)) {
    throw "db_path does not exist: $dbPath"
  }
  $dbResolved = (Resolve-Path -LiteralPath $dbPath).Path
  $expectedRoot = (Resolve-Path -LiteralPath $env:DATA_DIR).Path
  if (-not $dbResolved.StartsWith($expectedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "db_path not under DATA_DIR"
  }

  $rootResp = Invoke-WebRequest -UseBasicParsing "$baseUrl/" -TimeoutSec 5
  if ($rootResp.StatusCode -ne 200) {
    throw "Root not 200: $($rootResp.StatusCode)"
  }
  $rootHtml = $rootResp.Content
  $hasHtml = ($rootHtml -match '(?is)<!doctype') -or ($rootHtml -match '(?is)<html')
  if (-not $hasHtml) {
    throw "Root HTML marker not found"
  }

  $fav = Invoke-WebRequest -UseBasicParsing "$baseUrl/favicon.ico" -Method Head -TimeoutSec 5
  if (-not (@(200,304) -contains $fav.StatusCode)) {
    throw "favicon unexpected: $($fav.StatusCode)"
  }

  $songs = @()
  $deadline = (Get-Date).AddSeconds(60)
  do {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing "$baseUrl/api/songs" -TimeoutSec 5
      if ($resp.StatusCode -ne 200) { Start-Sleep -Milliseconds 500; continue }
      $songs = Normalize-Songs $resp.Content
    } catch {
      $songs = @()
    }
    if ((Get-SeqCount $songs) -eq 0) { Start-Sleep -Milliseconds 500 }
  } while ((Get-SeqCount $songs) -eq 0 -and (Get-Date) -lt $deadline -and ($env:SMOKE_REQUIRE_SONGS -in @('1','true','yes')))

  $songCount = Get-SeqCount $songs
  if ($songCount -eq 0 -and -not ($env:SMOKE_REQUIRE_SONGS -in @('1','true','yes'))) {
    Write-Host "Songs list is empty (expected in CI)"
  }
  if ($songCount -eq 0 -and ($env:SMOKE_REQUIRE_SONGS -in @('1','true','yes'))) {
    throw "No songs found in /api/songs after wait"
  }

  $modesJson = $null
  $deadline = (Get-Date).AddSeconds(30)
  do {
    try {
      $modesResp = Invoke-WebRequest -UseBasicParsing "$baseUrl/api/modes" -TimeoutSec 5
      if ($modesResp.StatusCode -ne 200) { Start-Sleep -Milliseconds 500; continue }
      try {
        $modesJson = $modesResp.Content | ConvertFrom-Json -ErrorAction Stop
      } catch {
        $modesJson = $null
      }
    } catch {
      $modesJson = $null
    }
    if ($null -eq $modesJson) { Start-Sleep -Milliseconds 500 }
  } while (($null -eq $modesJson) -and (Get-Date) -lt $deadline)

  if ($null -eq $modesJson) {
    throw "Failed to load /api/modes"
  }
  $statusValue = $modesJson.status
  if (-not (@('ok','disabled') -contains $statusValue)) {
    throw "Unexpected /api/modes status: $($modesJson | ConvertTo-Json -Compress)"
  }
  if ($statusValue -eq 'ok') {
    $modesList = $modesJson.modes
    if ($null -eq $modesList -or -not ($modesList -is [System.Collections.IEnumerable])) {
      throw "Invalid /api/modes payload: missing modes array"
    }
  }

  $categoriesJson = $null
  $deadline = (Get-Date).AddSeconds(30)
  do {
    try {
      $categoriesResp = Invoke-WebRequest -UseBasicParsing "$baseUrl/api/categories" -TimeoutSec 5
      if ($categoriesResp.StatusCode -ne 200) { Start-Sleep -Milliseconds 500; continue }
      try {
        $categoriesJson = $categoriesResp.Content | ConvertFrom-Json -ErrorAction Stop
      } catch {
        $categoriesJson = $null
      }
    } catch {
      $categoriesJson = $null
    }
    if ($null -eq $categoriesJson) { Start-Sleep -Milliseconds 500 }
  } while (($null -eq $categoriesJson) -and (Get-Date) -lt $deadline)

  if ($null -eq $categoriesJson) {
    throw "Failed to load /api/categories"
  }
  $isArray = $categoriesJson -is [System.Array]
  if (-not $isArray) {
    $isEnumerable = $categoriesJson -is [System.Collections.IEnumerable]
    $isObject = $categoriesJson -is [pscustomobject]
    if ($isObject) {
      $hasItems = $categoriesJson | Get-Member -Name items -ErrorAction SilentlyContinue
      if ($null -eq $hasItems) {
        throw "Unexpected /api/categories object payload"
      }
    } elseif (-not $isEnumerable) {
      throw "Unexpected /api/categories payload type"
    }
  }

  Write-Host ("Smoke OK: /api/songs count={0}" -f $songCount)
}
catch {
  $errMsg = $null
  if ($null -ne $_ -and $null -ne $_.Exception -and -not [string]::IsNullOrEmpty($_.Exception.Message)) {
    $errMsg = $_.Exception.Message
  } else {
    $errMsg = [string]$_
  }
  Write-Host "Smoke failure: $errMsg"
  if ($null -ne $resp) {
    Write-Host "---- last /api/songs body ----"
    try { Write-Host ($resp.Content | Out-String) } catch {}
  }
  if (Test-Path $stdoutLog) {
    Write-Host "===== smoke_stdout.log (tail) ====="
    try { Get-Content $stdoutLog -Tail 200 | Write-Host } catch {}
  }
  if (Test-Path $stderrLog) {
    Write-Host "===== smoke_stderr.log (tail) ====="
    try { Get-Content $stderrLog -Tail 200 | Write-Host } catch {}
  }
  throw
}
finally {
  if ($process) {
    try {
      if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
      }
    } catch {}
    try { $process.Dispose() } catch {}
  }
}
