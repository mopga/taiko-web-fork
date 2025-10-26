#!/usr/bin/env bash
set -euo pipefail

rm -rf build/backend dist/backend
pyinstaller packaging/pyinstaller.spec --distpath dist/backend --workpath build/backend --clean

echo "✅ Build completed: dist/backend/taiko-web-backend"
