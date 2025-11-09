const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { fileURLToPath } = require('node:url');
const Module = require('module');

const PRELOAD_PATH = path.join(__dirname, '..', 'preload.js');

const assetsSourceDir = path.join(__dirname, '..', '..', '..', 'assets', 'launcher');
const sourceTitleAsset = path.join(assetsSourceDir, 'title-screen.png');
const sourceMascotAsset = path.join(assetsSourceDir, 'dancing-don.gif');

function loadDesktop(preloadPath, options = {}) {
  const { electronDev = false, splashDiag = false } = options;

  const originalLoad = Module._load;
  const hadElectronDev = Object.prototype.hasOwnProperty.call(process.env, 'ELECTRON_DEV');
  const originalElectronDev = process.env.ELECTRON_DEV;
  const hadSplashDiag = Object.prototype.hasOwnProperty.call(process.env, 'ELECTRON_SPLASH_DIAG');
  const originalSplashDiag = process.env.ELECTRON_SPLASH_DIAG;

  let exposedDesktop = null;
  const preloadLogs = [];

  const hadWindow = Object.prototype.hasOwnProperty.call(global, 'window');
  const originalWindow = global.window;
  const windowStub = {
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    setTimeout: (...args) => setTimeout(...args),
    clearTimeout: (...args) => clearTimeout(...args),
  };
  global.window = windowStub;

  const electronStub = {
    contextBridge: {
      exposeInMainWorld: (_key, value) => {
        exposedDesktop = value;
      },
    },
    ipcRenderer: {
      send: (_channel, payload) => {
        preloadLogs.push(String(payload));
        return undefined;
      },
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

  if (electronDev === null) {
    delete process.env.ELECTRON_DEV;
  } else {
    process.env.ELECTRON_DEV = electronDev ? '1' : '0';
  }

  if (splashDiag === null) {
    delete process.env.ELECTRON_SPLASH_DIAG;
  } else {
    process.env.ELECTRON_SPLASH_DIAG = splashDiag ? '1' : '0';
  }

  const moduleId = require.resolve(preloadPath);
  delete require.cache[moduleId];

  try {
    require(preloadPath);
  } finally {
    Module._load = originalLoad;

    if (hadWindow) {
      global.window = originalWindow;
    } else {
      delete global.window;
    }

    if (hadElectronDev) {
      process.env.ELECTRON_DEV = originalElectronDev;
    } else {
      delete process.env.ELECTRON_DEV;
    }

    if (hadSplashDiag) {
      process.env.ELECTRON_SPLASH_DIAG = originalSplashDiag;
    } else {
      delete process.env.ELECTRON_SPLASH_DIAG;
    }

    delete require.cache[moduleId];
  }

  assert.ok(exposedDesktop, 'desktop API should be exposed');

  return { desktop: exposedDesktop, logs: preloadLogs };
}

test('getAssetUrl resolves launcher assets via process.resourcesPath candidates', () => {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'preload-packaged-'));
  const resourcesDir = path.join(tmpRoot, 'resources');
  const assetsDir = path.join(resourcesDir, 'assets', 'launcher');
  fs.mkdirSync(assetsDir, { recursive: true });
  fs.copyFileSync(sourceTitleAsset, path.join(assetsDir, 'title-screen.png'));
  fs.copyFileSync(sourceMascotAsset, path.join(assetsDir, 'dancing-don.gif'));

  const hadResourcesPath = Object.prototype.hasOwnProperty.call(process, 'resourcesPath');
  const originalResourcesPath = process.resourcesPath;
  const originalExecPath = process.execPath;

  process.resourcesPath = resourcesDir;
  process.execPath = path.join(tmpRoot, 'Taiko.exe');

  try {
    const { desktop } = loadDesktop(PRELOAD_PATH, { electronDev: false, splashDiag: false });

    const titleUrl = desktop.getAssetUrl('launcher', 'title-screen.png');
    assert.ok(titleUrl, 'title screen asset should resolve');
    assert.ok(titleUrl.startsWith('file://'), 'title screen asset should use file://');
    const titlePath = fileURLToPath(titleUrl);
    assert.strictEqual(path.basename(titlePath), 'title-screen.png');
    assert.ok(fs.existsSync(titlePath), 'title screen asset path should exist');

    const mascotUrl = desktop.getAssetUrl('launcher/dancing-don.gif');
    assert.ok(mascotUrl, 'mascot asset should resolve');
    assert.ok(mascotUrl.startsWith('file://'), 'mascot asset should use file://');
    const mascotPath = fileURLToPath(mascotUrl);
    assert.strictEqual(path.basename(mascotPath), 'dancing-don.gif');
    assert.ok(fs.existsSync(mascotPath), 'mascot asset path should exist');
  } finally {
    if (hadResourcesPath) {
      process.resourcesPath = originalResourcesPath;
    } else {
      delete process.resourcesPath;
    }
    process.execPath = originalExecPath;
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});

test('getAssetUrl resolves launcher assets via __dirname fallback when process paths are empty', () => {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'preload-dirname-'));
  const tempResourcesDir = path.join(tmpRoot, 'resources');
  const tempAppDir = path.join(tempResourcesDir, 'app');
  const tempPreloadPath = path.join(tempAppDir, 'preload.js');
  const tempAssetsDir = path.join(tempResourcesDir, 'assets', 'launcher');

  fs.mkdirSync(tempAppDir, { recursive: true });
  fs.mkdirSync(tempAssetsDir, { recursive: true });
  fs.copyFileSync(PRELOAD_PATH, tempPreloadPath);
  fs.copyFileSync(sourceTitleAsset, path.join(tempAssetsDir, 'title-screen.png'));
  fs.copyFileSync(sourceMascotAsset, path.join(tempAssetsDir, 'dancing-don.gif'));

  const hadResourcesPath = Object.prototype.hasOwnProperty.call(process, 'resourcesPath');
  const originalResourcesPath = process.resourcesPath;
  const originalExecPath = process.execPath;

  process.resourcesPath = '';
  process.execPath = '';

  try {
    const { desktop } = loadDesktop(tempPreloadPath, { electronDev: false, splashDiag: false });

    const titleUrl = desktop.getAssetUrl('launcher', 'title-screen.png');
    assert.ok(titleUrl, 'title screen asset should resolve');
    assert.ok(titleUrl.startsWith('file://'), 'title screen asset should use file://');
    const titlePath = fileURLToPath(titleUrl);
    assert.strictEqual(path.basename(titlePath), 'title-screen.png');
    assert.ok(fs.existsSync(titlePath), 'title screen asset path should exist');
  } finally {
    if (hadResourcesPath) {
      process.resourcesPath = originalResourcesPath;
    } else {
      delete process.resourcesPath;
    }
    process.execPath = originalExecPath;
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});

test('dev mode keeps dev asset base as the primary candidate', () => {
  const { desktop } = loadDesktop(PRELOAD_PATH, { electronDev: true, splashDiag: false });

  assert.ok(desktop.debugAssets);
  assert.ok(Array.isArray(desktop.debugAssets.bases));
  assert.ok(desktop.debugAssets.bases.length > 0);
  assert.strictEqual(desktop.debugAssets.bases[0].kind, 'dev');

  const titleUrl = desktop.getAssetUrl('launcher', 'title-screen.png');
  assert.ok(titleUrl, 'title screen asset should resolve');
  assert.ok(titleUrl.startsWith('file://'), 'title screen asset should use file://');
  const titlePath = fileURLToPath(titleUrl);
  assert.ok(fs.existsSync(titlePath), 'title screen asset path should exist');
});

test('preload smoke test logs asset status once assets are available', async () => {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'preload-smoke-'));
  const resourcesDir = path.join(tmpRoot, 'resources');
  const assetsDir = path.join(resourcesDir, 'assets', 'launcher');
  fs.mkdirSync(assetsDir, { recursive: true });
  fs.copyFileSync(sourceTitleAsset, path.join(assetsDir, 'title-screen.png'));
  fs.copyFileSync(sourceMascotAsset, path.join(assetsDir, 'dancing-don.gif'));

  const hadResourcesPath = Object.prototype.hasOwnProperty.call(process, 'resourcesPath');
  const originalResourcesPath = process.resourcesPath;
  const originalExecPath = process.execPath;

  process.resourcesPath = resourcesDir;
  process.execPath = path.join(tmpRoot, 'Taiko.exe');

  try {
    const { desktop, logs } = loadDesktop(PRELOAD_PATH, { electronDev: false, splashDiag: false });
    await desktop.ensureAssetsBase();
    await new Promise(resolve => setTimeout(resolve, 0));
    const smokeLog = logs.find(line => line.includes('[desktop:preload] smoke'));
    assert.ok(smokeLog, 'smoke log should be emitted');
    assert.ok(smokeLog.includes('assetsBase='), 'smoke log should include assets base info');
  } finally {
    if (hadResourcesPath) {
      process.resourcesPath = originalResourcesPath;
    } else {
      delete process.resourcesPath;
    }
    process.execPath = originalExecPath;
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});
