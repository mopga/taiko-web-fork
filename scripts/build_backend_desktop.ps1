Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Test-Path "build/backend") { Remove-Item "build/backend" -Recurse -Force }
if (Test-Path "dist/backend") { Remove-Item "dist/backend" -Recurse -Force }

pyinstaller packaging/pyinstaller.spec --distpath dist/backend --workpath build/backend --clean

Write-Host "✅ Build completed: dist/backend/taiko-web-backend"
