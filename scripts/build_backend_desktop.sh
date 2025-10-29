#!/usr/bin/env bash
set -euo pipefail

rm -rf build/backend dist/backend
python standalone/packaging/build_backend.py

echo "✅ Build completed: dist/backend/taiko-web-backend"
