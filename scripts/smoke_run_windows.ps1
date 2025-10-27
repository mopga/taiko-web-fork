Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$env:DATA_DIR = Join-Path (Get-Location) "_data"
New-Item -ItemType Directory -Force -Path $env:DATA_DIR | Out-Null
$env:RUN_PROFILE = "desktop"

$exe = "dist\\backend\\taiko-web-backend\\taiko-web-backend.exe"
if (-not (Test-Path $exe)) {
    throw "Binary not found: $exe"
}

$log = Join-Path (Get-Location) "smoke_windows.log"
if (Test-Path $log) {
    Remove-Item $log -Force
}
$args = @("--host", "127.0.0.1", "--port", "8000")
$process = Start-Process -FilePath $exe -ArgumentList $args -PassThru -RedirectStandardOutput $log -RedirectStandardError $log -WindowStyle Hidden
try {
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://127.0.0.1:8000/healthz" -TimeoutSec 2
            if ($response.StatusCode -eq 200 -and $response.Content -match '"status":"ok"' -and $response.Content -match '"db":"sqlite"') {
                Write-Host "healthz OK"
                exit 0
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    Write-Error "healthz failed"
    if (Test-Path $log) {
        Write-Host "===== smoke_windows.log (tail) ====="
        Get-Content $log -Tail 200 | Write-Host
    }
    exit 1
} finally {
    if ($process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
}
