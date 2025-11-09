const electronModule = require('electron');
const { contextBridge, ipcRenderer } = electronModule;
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

// Определение isPackaged: проверяем несколько признаков
const isPackaged = (() => {
  // 1. Проверяем наличие app.asar в пути
  if (__dirname.includes('app.asar')) {
    return true;
  }
  // 2. Проверяем наличие process.resourcesPath (устанавливается Electron в packaged режиме)
  if (process.resourcesPath && typeof process.resourcesPath === 'string') {
    // resourcesPath существует только в packaged режиме
    return true;
  }
  // 3. Проверяем, что execPath указывает на установленное приложение (не dev режим)
  if (process.execPath && typeof process.execPath === 'string') {
    const execDir = path.dirname(process.execPath);
    // Если рядом с exe есть resources папка, скорее всего это packaged
    try {
      if (fs.existsSync(path.join(execDir, 'resources'))) {
        return true;
      }
    } catch (error) {
      // Игнорируем ошибки
    }
  }
  return false;
})();

if (!Object.prototype.hasOwnProperty.call(process.env, 'ELECTRON_SPLASH_DIAG')) {
  process.env.ELECTRON_SPLASH_DIAG = '1';
}

const enableSplashDiag = process.env.ELECTRON_SPLASH_DIAG === '1';

// Определяем execDir один раз для всех путей
const execPathValue =
  typeof process !== 'undefined' && process.execPath && typeof process.execPath === 'string'
    ? process.execPath
    : null;
const execDir = execPathValue ? path.dirname(execPathValue) : null;

// Dev режим: путь относительно __dirname (для разработки)
const devAssetsBase = path.join(__dirname, '..', '..', 'assets');

const assetBases = [];
const assetBaseSet = new Set();
const assetBaseMap = new Map();

const pushAssetBase = (kind, fsPath, options = {}) => {
  if (!fsPath) {
    return;
  }
  const resolved = path.resolve(fsPath);
  try {
    // Проверяем существование базовой директории
    if (!fs.existsSync(resolved)) {
      return;
    }
    // Дополнительная проверка: убеждаемся, что это директория (не файл)
    const stats = fs.statSync(resolved);
    if (!stats.isDirectory()) {
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

  if (assetBaseSet.has(resolved)) {
    if (options.preferFront) {
      const existing = assetBaseMap.get(resolved);
      if (existing) {
        const existingIndex = assetBases.indexOf(existing);
        if (existingIndex >= 0) {
          assetBases.splice(existingIndex, 1);
        }
      }
      assetBases.unshift(entry);
      assetBaseMap.set(resolved, entry);
    }
    return assetBaseMap.get(resolved) || null;
  }

  if (options.preferFront) {
    assetBases.unshift(entry);
  } else {
    assetBases.push(entry);
  }
  assetBaseSet.add(resolved);
  assetBaseMap.set(resolved, entry);
  return entry;
};

// Порядок важен: стандартные пути, затем альтернативы рядом с exe и __dirname
// Проверяем пути в порядке приоритета для максимальной совместимости

const resourcesPathValue =
  typeof process !== 'undefined' && process.resourcesPath && typeof process.resourcesPath === 'string'
    ? process.resourcesPath
    : null;

if (resourcesPathValue) {
  pushAssetBase('packaged', path.join(resourcesPathValue, 'assets'));
}

if (resourcesPathValue) {
  pushAssetBase('unpacked', path.join(resourcesPathValue, 'app.asar.unpacked', 'assets'));
}

if (execDir) {
  pushAssetBase('packaged-alt', path.join(execDir, 'resources', 'assets'));
}

if (execDir) {
  pushAssetBase('packaged-alt', path.join(execDir, 'assets'));
}

pushAssetBase('dirname', path.resolve(__dirname, '..', 'assets'));
pushAssetBase('dirname', path.resolve(__dirname, '..', '..', 'assets'));

// Dev fallback: если базы выше не нашлись или явно dev-режим
if (assetBases.length === 0 || process.env.ELECTRON_DEV === '1') {
  if (devAssetsBase && fs.existsSync(devAssetsBase)) {
    pushAssetBase('dev', devAssetsBase, { preferFront: true });
  }
}

// Диагностическое логирование (только в dev или если не найдено баз)
if (enableSplashDiag && (assetBases.length === 0 || process.env.ELECTRON_DEV === '1')) {
  console.log('[desktop:preload] Asset path diagnosis:', {
    isPackaged,
    execPath: typeof process !== 'undefined' ? process.execPath : null,
    execDir,
    resourcesPath: typeof process !== 'undefined' ? process.resourcesPath : null,
    dirname: __dirname,
    assetBasesLength: assetBases.length,
    __dirname,
    checkedPaths: {
      resourcesPathAssets: resourcesPathValue ? path.join(resourcesPathValue, 'assets') : null,
      resourcesPathUnpacked: resourcesPathValue
        ? path.join(resourcesPathValue, 'app.asar.unpacked', 'assets')
        : null,
      execDirResources: execDir ? path.join(execDir, 'resources', 'assets') : null,
      execDirFlat: execDir ? path.join(execDir, 'assets') : null,
      devAssetsBase,
    },
    foundBases: assetBases.map(b => ({ kind: b.kind, fsPath: b.fsPath })),
  });
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

  // isPackaged уже определен выше, используем его напрямую
  const resolvedIsPackaged = isPackaged;

  const fallbackBases = assetsBase && assetsBaseUrl
    ? [{ kind: 'fallback', fsPath: assetsBase, url: assetsBaseUrl }]
    : [];
  const basesForReport = assetBases.length ? assetBases : fallbackBases;

  // Всегда возвращаем объект с диагностической информацией
  const diagnosis = {
    isPackaged: resolvedIsPackaged,
    resourcesPath: (typeof process !== 'undefined' && process.resourcesPath) || null,
    execPath: (typeof process !== 'undefined' && process.execPath) || null,
    execDir: execDir || null,
    dirname: __dirname,
    assetBasesLength: assetBases.length,
    assetsBase: assetsBase || null,
    assetsBaseUrl: assetsBaseUrl || null,
    assetBases: basesForReport.map(({ kind, fsPath, url }) => ({ kind, fsPath, url })),
    resolved: {
      title: resolveWithStatus('launcher', 'title-screen.png'),
      mascot: resolveWithStatus('launcher', 'dancing-don.gif'),
    },
  };

  return diagnosis;
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
