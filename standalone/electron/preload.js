const electronModule = require('electron');
const { contextBridge, ipcRenderer } = electronModule;
const electronApp = electronModule.app;
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

const isPackaged = __dirname.includes('app.asar');

if (!Object.prototype.hasOwnProperty.call(process.env, 'ELECTRON_SPLASH_DIAG')) {
  process.env.ELECTRON_SPLASH_DIAG = '1';
}

const enableSplashDiag = process.env.ELECTRON_SPLASH_DIAG === '1';
const packagedAssetsBase =
  process.resourcesPath && typeof process.resourcesPath === 'string'
    ? path.join(process.resourcesPath, 'assets')
    : null;
const unpackedAssetsBase =
  process.resourcesPath && typeof process.resourcesPath === 'string'
    ? path.join(process.resourcesPath, 'app.asar.unpacked', 'assets')
    : null;
// Дополнительные «железные» базы для инсталлятора на основе win-unpacked:
const execDir = path.dirname(process.execPath || '');
const altResourcesAssetsBase = execDir ? path.join(execDir, 'resources', 'assets') : null;
const altFlatAssetsBase      = execDir ? path.join(execDir, 'assets')            : null;
const devAssetsBase = path.join(__dirname, '..', '..', 'assets');

const assetBases = [];
const assetBaseSet = new Set();

const pushAssetBase = (kind, fsPath) => {
  if (!fsPath) {
    return;
  }
  const resolved = path.resolve(fsPath);
  if (assetBaseSet.has(resolved)) {
    return;
  }
  try {
    if (!fs.existsSync(resolved)) {
      return;
    }
  } catch (error) {
    return;
  }
  const entry = {
    kind,
    fsPath: resolved,
    url: pathToFileURL(resolved).href,
  };
  assetBases.push(entry);
  assetBaseSet.add(resolved);
  return entry;
};

// Порядок важен: стандартные пути, затем альтернативы рядом с exe
if (packagedAssetsBase)      pushAssetBase('packaged', packagedAssetsBase);
if (unpackedAssetsBase)      pushAssetBase('unpacked', unpackedAssetsBase);
if (altResourcesAssetsBase)  pushAssetBase('packaged-alt', altResourcesAssetsBase);
if (altFlatAssetsBase)       pushAssetBase('packaged-alt', altFlatAssetsBase);

// Dev fallback: если базы выше не нашлись или явно dev-режим
if (assetBases.length === 0 || process.env.ELECTRON_DEV === '1') {
  if (fs.existsSync(devAssetsBase)) {
    pushAssetBase('dev', devAssetsBase);
  }
}

let assetsBase = null;
let assetsBaseUrl = null;

if (assetBases.length > 0) {
  assetsBase = assetBases[0].fsPath;
  assetsBaseUrl = assetBases[0].url;
} else {
  console.warn('[desktop:preload] No asset directories found; asset URLs will be unavailable.');
}

function onStatusUpdate(callback) {
  if (typeof callback !== 'function') {
    return () => undefined;
  }
  const channel = 'desktop:status';
  const listener = (_event, payload) => {
    callback(payload);
  };
  ipcRenderer.on(channel, listener);
  return () => {
    ipcRenderer.removeListener(channel, listener);
  };
}

function resolveAssetPath(...segments) {
  const parts = [];

  const pushSegment = (segment) => {
    if (typeof segment !== 'string') {
      return;
    }
    const token = segment.trim();
    if (token.length > 0) {
      parts.push(token);
    }
  };

  const flatten = (value) => {
    if (Array.isArray(value)) {
      value.forEach(flatten);
      return;
    }
    pushSegment(value);
  };

  segments.forEach(flatten);

  if (parts.length === 0) {
    return null;
  }

  const fallbackBases = assetsBase && assetsBaseUrl
    ? [{ kind: 'fallback', fsPath: assetsBase, url: assetsBaseUrl }]
    : [];
  const candidates = assetBases.length ? assetBases : fallbackBases;

  for (const base of candidates) {
    const candidatePath = path.join(base.fsPath, ...parts);
    try {
      if (fs.existsSync(candidatePath)) {
        return candidatePath;
      }
    } catch (error) {
      // Ignore file access errors and continue to other bases.
    }
  }

  return null;
}

function getAssetUrl(primary, ...rest) {
  const assetPath =
    rest.length === 0 && typeof primary === 'string' && primary.includes('/')
      ? resolveAssetPath(...primary.split(/[\\/]+/))
      : resolveAssetPath(primary, ...rest);
  if (!assetPath) {
    return null;
  }
  try {
    if (fs.existsSync(assetPath)) {
      return pathToFileURL(assetPath).href;
    }
  } catch (error) {
    return null;
  }
  return null;
}

const debugAssets = Object.freeze({
  isPackaged,
  electronDev: process.env.ELECTRON_DEV === '1',
  activeBase: assetsBase,
  activeBaseUrl: assetsBaseUrl,
  bases: assetBases.map((base) => ({ ...base })),
});

const createDiagnoseAssets = () => {
  if (!enableSplashDiag) {
    return null;
  }

  const resolveWithStatus = (...segments) => {
    const filePath = resolveAssetPath(...segments);
    if (!filePath) {
      return { path: null, exists: false };
    }
    let exists = false;
    try {
      exists = fs.existsSync(filePath);
    } catch (error) {
      exists = false;
    }
    return { path: filePath, exists };
  };

  const resolvedIsPackaged =
    electronApp && typeof electronApp.isPackaged === 'boolean'
      ? electronApp.isPackaged
      : isPackaged;

  const fallbackBases = assetsBase && assetsBaseUrl
    ? [{ kind: 'fallback', fsPath: assetsBase, url: assetsBaseUrl }]
    : [];
  const basesForReport = assetBases.length ? assetBases : fallbackBases;

  return {
    isPackaged: resolvedIsPackaged,
    resourcesPath: process.resourcesPath,
    assetsBase,
    assetBases: basesForReport.map(({ kind, fsPath, url }) => ({ kind, fsPath, url })),
    resolved: {
      title: resolveWithStatus('launcher', 'title-screen.png'),
      mascot: resolveWithStatus('launcher', 'dancing-don.gif'),
    },
  };
};

const desktopApi = {
  onStatus: onStatusUpdate,
  requestQuit: () => ipcRenderer.invoke('desktop:quit'),
  chooseSongsDir: () => ipcRenderer.invoke('desktop:chooseSongsDir'),
  log: (msg) => ipcRenderer.invoke('desktop:log', msg),
  getAssetUrl,
  debugAssets,
};

if (enableSplashDiag) {
  desktopApi.diagnoseAssets = () => createDiagnoseAssets();
}

contextBridge.exposeInMainWorld('desktop', desktopApi);
