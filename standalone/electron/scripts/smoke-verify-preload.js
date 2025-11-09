#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');
const process = require('node:process');
const asar = require('@electron/asar');

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
}

function main() {
  const input = process.env.APP_ASAR || process.argv[2];
  if (!input) {
    fail('Usage: smoke-verify-preload.js <path-to-app.asar>');
    return;
  }

  const archivePath = path.resolve(input);
  if (!fs.existsSync(archivePath)) {
    fail(`Provided app.asar does not exist: ${archivePath}`);
    return;
  }

  let preloadSource = null;
  try {
    preloadSource = asar.extractFile(archivePath, 'preload.js').toString('utf8');
  } catch (error) {
    fail(`Failed to read preload.js from ${archivePath}: ${error.message}`);
    return;
  }

  if (preloadSource.indexOf('function ensureAssetsBase') === -1) {
    fail('Expected preload.js inside asar to include function ensureAssetsBase');
    return;
  }

  const resourcesDir = path.dirname(archivePath);
  const launcherDir = path.join(resourcesDir, 'assets', 'launcher');
  const titleAsset = path.join(launcherDir, 'title-screen.png');
  const mascotAsset = path.join(launcherDir, 'dancing-don.gif');

  const missingAssets = [titleAsset, mascotAsset].filter((assetPath) => !fs.existsSync(assetPath));
  if (missingAssets.length > 0) {
    fail(`Missing launcher assets: ${missingAssets.join(', ')}`);
    return;
  }

  process.stdout.write(`[smoke] ensureAssetsBase present in ${archivePath}\n`);
  process.stdout.write(`[smoke] launcher assets verified at ${launcherDir}\n`);
}

main();
