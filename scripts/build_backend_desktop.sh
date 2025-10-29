#!/usr/bin/env bash
set -euo pipefail

rm -rf build/backend dist/backend standalone/dist/backend standalone/dist/build-backend
python standalone/packaging/build_backend.py

echo "✅ Build completed: standalone/dist/backend/taiko-web-backend"
