const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { fileURLToPath } = require('node:url');
const Module = require('module');

const PRELOAD_PATH = path.join(__dirname, '..', 'preload.js');
const PRELOAD_MODULE_ID = require.resolve(PRELOAD_PATH);

test('getAssetUrl returns valid file URLs for launcher assets in dev mode', () => {
  const originalLoad = Module._load;
  const originalElectronDev = process.env.ELECTRON_DEV;
  const originalSplashDiag = process.env.ELECTRON_SPLASH_DIAG;
  let exposedDesktop = null;

  const electronStub = {
    contextBridge: {
      exposeInMainWorld: (_key, value) => {
        exposedDesktop = value;
      },
    },
    ipcRenderer: {
      on: () => undefined,
      removeListener: () => undefined,
      invoke: () => Promise.resolve(),
    },
    app: null,
  };

  Module._load = function patchedModuleLoad(request, parent, isMain) {
    if (request === 'electron') {
      return electronStub;
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  process.env.ELECTRON_DEV = '1';
  delete process.env.ELECTRON_SPLASH_DIAG;

  delete require.cache[PRELOAD_MODULE_ID];

  try {
    require(PRELOAD_PATH);
  } finally {
    Module._load = originalLoad;
    if (originalElectronDev === undefined) {
      delete process.env.ELECTRON_DEV;
    } else {
      process.env.ELECTRON_DEV = originalElectronDev;
    }
    if (originalSplashDiag === undefined) {
      delete process.env.ELECTRON_SPLASH_DIAG;
    } else {
      process.env.ELECTRON_SPLASH_DIAG = originalSplashDiag;
    }
    delete require.cache[PRELOAD_MODULE_ID];
  }

  assert.ok(exposedDesktop, 'desktop API should be exposed');

  const titleUrl = exposedDesktop.getAssetUrl('launcher', 'title-screen.png');
  assert.ok(titleUrl, 'title screen asset should resolve');
  assert.ok(titleUrl.startsWith('file://'), 'title screen asset should use file://');
  const titlePath = fileURLToPath(titleUrl);
  assert.strictEqual(path.basename(titlePath), 'title-screen.png');
  assert.ok(fs.existsSync(titlePath), 'title screen asset path should exist');

  const mascotUrl = exposedDesktop.getAssetUrl('launcher/dancing-don.gif');
  assert.ok(mascotUrl, 'mascot asset should resolve');
  assert.ok(mascotUrl.startsWith('file://'), 'mascot asset should use file://');
  const mascotPath = fileURLToPath(mascotUrl);
  assert.strictEqual(path.basename(mascotPath), 'dancing-don.gif');
  assert.ok(fs.existsSync(mascotPath), 'mascot asset path should exist');
});
