Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:RUN_PROFILE = "desktop"
$env:PROFILE = "desktop"

$repoRoot = (Get-Location).Path
if ([string]::IsNullOrWhiteSpace($env:DATA_DIR)) {
  $env:DATA_DIR = Join-Path $repoRoot "_data"
}
New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null

$backendRoot = Join-Path $repoRoot "standalone/dist/backend/taiko-web-backend"
$exe = Join-Path $backendRoot "taiko-web-backend.exe"
if (-not (Test-Path -LiteralPath $exe)) {
  throw "Binary not found: $exe"
}

$logDir = Join-Path $repoRoot "_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdoutLog = Join-Path $logDir "smoke_stdout.log"
$stderrLog = Join-Path $logDir "smoke_stderr.log"
Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$absExe = (Resolve-Path -LiteralPath $exe).Path
$absRoot = Split-Path -Parent $absExe

$portCandidate = $null
if ($null -ne $env:PORT -and -not [string]::IsNullOrWhiteSpace($env:PORT)) {
  $portCandidate = $env:PORT
} elseif ($null -ne $env:APP_PORT -and -not [string]::IsNullOrWhiteSpace($env:APP_PORT)) {
  $portCandidate = $env:APP_PORT
} else {
  $portCandidate = (Get-Random -Minimum 20000 -Maximum 40000).ToString()
}
$port = $portCandidate
$env:PORT = $port
$baseUrl = "http://127.0.0.1:$port"

$startParams = @{
  FilePath = $absExe
  ArgumentList = @("--host", "127.0.0.1", "--port", $port)
  WorkingDirectory = $absRoot
  PassThru = $true
  RedirectStandardOutput = $stdoutLog
  RedirectStandardError = $stderrLog
  WindowStyle = 'Hidden'
}

function Invoke-SmokeRequest {
  param(
    [string]$Method,
    [string]$Path,
    [int[]]$ExpectedStatus
  )

  $response = $null
  try {
    $response = Invoke-WebRequest -UseBasicParsing "$baseUrl$Path" -Method $Method -TimeoutSec 10
    $code = [int]$response.StatusCode
  } catch {
    $resp = $_.Exception.Response
    if ($null -ne $resp) {
      $code = [int]$resp.StatusCode
    } else {
      throw
    }
  }

  if (-not ($ExpectedStatus -contains $code)) {
    throw "Unexpected status $code for $Path"
  }

  return $response
}

$process = $null
try {
  $process = Start-Process @startParams -ErrorAction Stop

  Start-Sleep -Milliseconds 200

  $health = $null
  $deadline = (Get-Date).AddSeconds(60)
  do {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing "$baseUrl/healthz" -TimeoutSec 5
      if ($resp.StatusCode -ne 200) {
        Start-Sleep -Milliseconds 500
        continue
      }
      $health = $resp.Content | ConvertFrom-Json -ErrorAction Stop
    } catch {
      $health = $null
    }
    if ($null -eq $health) {
      Start-Sleep -Milliseconds 500
    }
  } while (($null -eq $health) -and (Get-Date) -lt $deadline)

  if ($null -eq $health) {
    throw "Timed out waiting for /healthz"
  }

  if ($health.status -ne 'ok') {
    throw "healthz status != ok"
  }
  if ($health.profile -ne 'desktop') {
    throw "healthz profile != desktop"
  }

  $dbPath = $health.db_path
  if ([string]::IsNullOrWhiteSpace($dbPath)) {
    throw "db_path missing from /healthz payload"
  }
  if (-not (Test-Path -LiteralPath $dbPath)) {
    throw "db_path does not exist: $dbPath"
  }
  $dbResolved = (Resolve-Path -LiteralPath $dbPath).Path
  $dataDirResolved = (Resolve-Path -LiteralPath $env:DATA_DIR).Path
  if (-not $dbResolved.StartsWith($dataDirResolved, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "db_path not under DATA_DIR"
  }

  Invoke-SmokeRequest -Method Get -Path "/" -ExpectedStatus @(200) | Out-Null
  Invoke-SmokeRequest -Method Get -Path "/favicon.ico" -ExpectedStatus @(200, 304) | Out-Null
  Invoke-SmokeRequest -Method Get -Path "/api/songs" -ExpectedStatus @(200) | Out-Null
  Invoke-SmokeRequest -Method Get -Path "/api/modes" -ExpectedStatus @(200) | Out-Null
  Invoke-SmokeRequest -Method Get -Path "/api/categories" -ExpectedStatus @(200) | Out-Null

  Write-Host "Smoke OK"
}
catch {
  $errMsg = $null
  if ($null -ne $_ -and $null -ne $_.Exception -and -not [string]::IsNullOrEmpty($_.Exception.Message)) {
    $errMsg = $_.Exception.Message
  } else {
    $errMsg = [string]$_
  }
  Write-Host "Smoke failure: $errMsg"
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
  if ($null -ne $process) {
    try {
      if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
      }
    } catch {}
    try { $process.Dispose() } catch {}
  }
}
