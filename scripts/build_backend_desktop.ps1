Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Test-Path "build/backend") { Remove-Item "build/backend" -Recurse -Force }
if (Test-Path "dist/backend") { Remove-Item "dist/backend" -Recurse -Force }
if (Test-Path "standalone/dist/backend") { Remove-Item "standalone/dist/backend" -Recurse -Force }
if (Test-Path "standalone/dist/build-backend") { Remove-Item "standalone/dist/build-backend" -Recurse -Force }

python standalone/packaging/build_backend.py

Write-Host "✅ Build completed: standalone/dist/backend/taiko-web-backend"
