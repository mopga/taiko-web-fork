const { contextBridge, ipcRenderer } = require('electron');
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

const isPackaged = __dirname.includes('app.asar');
const packagedAssetsBase =
  process.resourcesPath && typeof process.resourcesPath === 'string'
    ? path.join(process.resourcesPath, 'assets')
    : null;
const unpackedAssetsBase =
  process.resourcesPath && typeof process.resourcesPath === 'string'
    ? path.join(process.resourcesPath, 'app.asar.unpacked', 'assets')
    : null;
const devAssetsBase = path.join(__dirname, '..', 'assets');

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

if (isPackaged) {
  if (packagedAssetsBase) {
    pushAssetBase('packaged', packagedAssetsBase);
  }
  if (unpackedAssetsBase) {
    pushAssetBase('unpacked', unpackedAssetsBase);
  }
}

if (!isPackaged || process.env.ELECTRON_DEV === '1' || assetBases.length === 0) {
  if (fs.existsSync(devAssetsBase)) {
    pushAssetBase('dev', devAssetsBase);
  }
}

const assetsBase = assetBases.length > 0 ? assetBases[0].fsPath : devAssetsBase;
const assetsBaseUrl = assetBases.length > 0 ? assetBases[0].url : pathToFileURL(assetsBase).href;

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

  const candidates = assetBases.length
    ? assetBases
    : [{ kind: 'fallback', fsPath: assetsBase, url: assetsBaseUrl }];

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

  const base = candidates[0];
  return base ? path.join(base.fsPath, ...parts) : null;
}

function getAssetUrl(primary, ...rest) {
  const assetPath =
    rest.length === 0 && typeof primary === 'string' && primary.includes('/')
      ? resolveAssetPath(primary.split(/[\\/]+/))
      : resolveAssetPath(primary, rest);
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

contextBridge.exposeInMainWorld('desktop', {
  onStatus: onStatusUpdate,
  requestQuit: () => ipcRenderer.invoke('desktop:quit'),
  chooseSongsDir: () => ipcRenderer.invoke('desktop:chooseSongsDir'),
  getAssetUrl,
  debugAssets,
});
