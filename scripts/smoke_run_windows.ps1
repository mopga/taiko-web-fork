Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

$args = @("--host", "127.0.0.1", "--port", "8000", "--songs-dir", $songsDir)
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
    $baseUrl = "http://127.0.0.1:8000"

    function Invoke-SmokeRequest {
        param(
            [string]$Method,
            [string]$Path,
            [int[]]$ExpectedStatus,
            [ScriptBlock]$Assertion
        )

        $uri = "$baseUrl$Path"
        $response = $null
        try {
            $response = Invoke-WebRequest -Method $Method -Uri $uri -TimeoutSec 5
        } catch {
            throw "Request to $uri failed: $($_.Exception.Message)"
        }

        if ($ExpectedStatus -notcontains $response.StatusCode) {
            throw "Unexpected status $($response.StatusCode) for $uri"
        }

        if ($Assertion) {
            & $Assertion $response
        }
    }

    $healthy = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $health = Invoke-WebRequest -Uri "$baseUrl/healthz" -TimeoutSec 2
            if ($health.StatusCode -eq 200 -and $health.Content -match '"status":"ok"') {
                $healthy = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    if (-not $healthy) {
        throw "healthz check failed"
    }

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

    Invoke-SmokeRequest -Method Get -Path '/' -ExpectedStatus @(200) -Assertion {
        param($resp)
        if (-not $resp.Content -or $resp.Content -notmatch '</html>') {
            throw "Root HTML not served (status=$($resp.StatusCode))"
        }
    }

    Invoke-SmokeRequest -Method Head -Path '/favicon.ico' -ExpectedStatus @(200, 304)

    $songsDeadline = (Get-Date).AddSeconds(45)
    $songsResponse = $null
    while ((Get-Date) -lt $songsDeadline) {
        try {
            $candidate = Invoke-WebRequest -Method Get -Uri "$baseUrl/api/songs" -TimeoutSec 5
        } catch {
            Start-Sleep -Seconds 1
            continue
        }

        if ($candidate.StatusCode -ne 200) {
            Start-Sleep -Seconds 1
            continue
        }

        $songsResponse = $candidate
        break
    }

    if (-not $songsResponse) {
        throw "Timed out waiting for /api/songs to return 200"
    }

    $songsContentType = $songsResponse.Headers['Content-Type']
    if (-not $songsContentType -or $songsContentType -notmatch 'application/json') {
        Write-Host "---- /api/songs raw body ----"
        Write-Host $songsResponse.Content
        throw "Songs response content type unexpected: $songsContentType"
    }

    try {
        $songsJson = $songsResponse.Content | ConvertFrom-Json -ErrorAction Stop
    } catch {
        Write-Host "---- /api/songs raw body ----"
        Write-Host $songsResponse.Content
        throw "Songs response is not valid JSON: $($_.Exception.Message)"
    }

    if (-not $songsJson) {
        $songsJson = @()
    }

    if ($songsJson.GetType().Name -ne 'Object[]') {
        Write-Host "---- /api/songs raw body ----"
        Write-Host $songsResponse.Content
        throw "/api/songs is not array"
    }

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
