param(
    [int]$TimeoutSeconds = 300,
    [int]$PollIntervalSeconds = 5
)

$ErrorActionPreference = 'Stop'

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message"
}

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeDirectory = Resolve-Path (Join-Path $scriptDirectory '..')

Push-Location $composeDirectory
try {
    Write-Info 'Starting docker-compose services...'
    docker-compose up -d | Write-Output
    if ($LASTEXITCODE -ne 0) {
        throw 'docker-compose failed to start services.'
    }

    $containerIds = docker-compose ps -q
    if (-not $containerIds -or $containerIds.Count -eq 0) {
        throw 'No containers were started by docker-compose.'
    }

    Write-Info "Waiting for $($containerIds.Count) container(s) to become ready..."
    $startTime = Get-Date

    while ($true) {
        $allReady = $true
        foreach ($containerId in $containerIds) {
            $stateJson = docker inspect --format '{{json .State}}' $containerId
            $state = $stateJson | ConvertFrom-Json

            if ($null -ne $state.Health) {
                $ready = $state.Health.Status -eq 'healthy'
                $status = $state.Health.Status
            } else {
                $ready = $state.Running -eq $true
                $status = if ($state.Running) { 'running' } elseif ($state.Status) { $state.Status } else { 'unknown' }
            }

            if (-not $ready) {
                $allReady = $false
                Write-Info "Container $containerId status: $status"
            }
        }

        if ($allReady) {
            break
        }

        if ((Get-Date) - $startTime -gt [TimeSpan]::FromSeconds($TimeoutSeconds)) {
            throw "Timeout of $TimeoutSeconds seconds reached while waiting for containers to become ready."
        }

        Start-Sleep -Seconds $PollIntervalSeconds
    }

    Write-Info 'All containers are ready. Launching browser at http://localhost:8000 ...'
    Start-Process 'http://localhost:8000'
}
finally {
    Pop-Location
}
