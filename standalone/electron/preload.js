const electronModule = require('electron');
const { contextBridge, ipcRenderer } = electronModule;
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

// Определение isPackaged: используем встроенный способ Electron
const isPackaged = (() => {
  try {
    // В Electron process.resourcesPath устанавливается только в packaged режиме
    if (process.resourcesPath && typeof process.resourcesPath === 'string') {
      return true;
    }
    // Альтернативная проверка: наличие app.asar в пути
    if (__dirname.includes('app.asar')) {
      return true;
    }
    // Проверка через execPath: если рядом с exe есть resources, это packaged
    if (process.execPath && typeof process.execPath === 'string') {
      const execDir = path.dirname(process.execPath);
      try {
        const resourcesPath = path.join(execDir, 'resources');
        if (fs.existsSync(resourcesPath)) {
          return true;
        }
      } catch (error) {
        // Игнорируем ошибки
      }
    }
  } catch (error) {
    // Игнорируем ошибки при определении
  }
  return false;
})();

if (!Object.prototype.hasOwnProperty.call(process.env, 'ELECTRON_SPLASH_DIAG')) {
  process.env.ELECTRON_SPLASH_DIAG = '1';
}

const enableSplashDiag = process.env.ELECTRON_SPLASH_DIAG === '1';

// Получаем пути для поиска ассетов
// Используем ту же логику, что и resolveWindowIcon в main.js
const getAssetSearchPaths = () => {
  const paths = [];
  
  // 1. process.resourcesPath/assets (основной путь для extraResources в packaged режиме)
  // Это самый важный путь - electron-builder помещает extraResources в process.resourcesPath
  if (process.resourcesPath && typeof process.resourcesPath === 'string') {
    const resourcesAssets = path.join(process.resourcesPath, 'assets');
    paths.push({ kind: 'resources', path: resourcesAssets, priority: 10 });
  }
  
  // 2. execDir/resources/assets (альтернативный путь для packaged приложений)
  // На Windows/Mac структура: AppDir/resources/assets/launcher/
  if (process.execPath && typeof process.execPath === 'string') {
    const execDir = path.dirname(process.execPath);
    const execResourcesAssets = path.join(execDir, 'resources', 'assets');
    paths.push({ kind: 'exec-resources', path: execResourcesAssets, priority: 9 });
  }
  
  // 3. __dirname relative paths (для dev режима и fallback)
  // Если preload.js в app.asar, это не сработает, но для dev режима нужно
  try {
    const dirnameAssets1 = path.resolve(__dirname, '..', 'assets');
    paths.push({ kind: 'dirname-up', path: dirnameAssets1, priority: 7 });
  } catch (error) {
    // Игнорируем ошибки
  }
  
  try {
    const dirnameAssets2 = path.resolve(__dirname, '..', '..', 'assets');
    paths.push({ kind: 'dirname-up-up', path: dirnameAssets2, priority: 6 });
  } catch (error) {
    // Игнорируем ошибки
  }
  
  // Сортируем по приоритету (больше = выше приоритет)
  paths.sort((a, b) => b.priority - a.priority);
  
  return paths;
};

// Находим первую существующую директорию с ассетами
const findAssetsBase = () => {
  const searchPaths = getAssetSearchPaths();
  
  for (const { kind, path: searchPath } of searchPaths) {
    try {
      const resolved = path.resolve(searchPath);
      if (fs.existsSync(resolved)) {
        const stats = fs.statSync(resolved);
        if (stats.isDirectory()) {
          // Проверяем, что это действительно директория с launcher подпапкой
          const launcherPath = path.join(resolved, 'launcher');
          if (fs.existsSync(launcherPath)) {
            const launcherStats = fs.statSync(launcherPath);
            if (launcherStats.isDirectory()) {
              if (enableSplashDiag) {
                console.log(`[desktop:preload] Found assets at ${kind}: ${resolved}`);
              }
              return resolved;
            }
          }
        }
      }
    } catch (error) {
      // Игнорируем ошибки и продолжаем поиск
      if (enableSplashDiag) {
        console.log(`[desktop:preload] Error checking ${kind} path ${searchPath}:`, error.message);
      }
    }
  }
  
  return null;
};

const assetsBase = findAssetsBase();
const assetsBaseUrl = assetsBase ? pathToFileURL(assetsBase).href : null;

if (enableSplashDiag) {
  console.log('[desktop:preload] Asset resolution:', {
    isPackaged,
    resourcesPath: process.resourcesPath || null,
    execPath: process.execPath || null,
    dirname: __dirname,
    assetsBase: assetsBase || null,
    assetsBaseUrl: assetsBaseUrl || null,
  });
}

if (!assetsBase) {
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
  if (!assetsBase) {
    return null;
  }

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

  try {
    const assetPath = path.join(assetsBase, ...parts);
    if (fs.existsSync(assetPath)) {
      return assetPath;
    }
  } catch (error) {
    // Ignore file access errors
    if (enableSplashDiag) {
      console.log('[desktop:preload] Error resolving asset path:', error.message);
    }
  }

  return null;
}

function getAssetUrl(primary, ...rest) {
  if (!assetsBase) {
    if (enableSplashDiag) {
      console.log('[desktop:preload] getAssetUrl called but assetsBase is not set');
    }
    return null;
  }

  const assetPath =
    rest.length === 0 && typeof primary === 'string' && primary.includes('/')
      ? resolveAssetPath(...primary.split(/[\\/]+/))
      : resolveAssetPath(primary, ...rest);
  
  if (!assetPath) {
    if (enableSplashDiag) {
      console.log(`[desktop:preload] Could not resolve asset path for: ${primary}${rest.length > 0 ? ', ' + rest.join(', ') : ''}`);
    }
    return null;
  }
  
  try {
    if (fs.existsSync(assetPath)) {
      // pathToFileURL correctly handles Windows paths and converts them to file:// URLs
      const url = pathToFileURL(assetPath).href;
      if (enableSplashDiag) {
        console.log(`[desktop:preload] Resolved asset URL: ${url} (from path: ${assetPath})`);
      }
      return url;
    } else {
      if (enableSplashDiag) {
        console.log(`[desktop:preload] Asset file does not exist: ${assetPath}`);
      }
    }
  } catch (error) {
    if (enableSplashDiag) {
      console.log('[desktop:preload] Error generating asset URL:', error.message, error.stack);
    }
    return null;
  }
  
  return null;
}

const debugAssets = Object.freeze({
  isPackaged,
  electronDev: process.env.ELECTRON_DEV === '1',
  activeBase: assetsBase,
  activeBaseUrl: assetsBaseUrl,
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

  const execDir = process.execPath ? path.dirname(process.execPath) : null;

  const diagnosis = {
    isPackaged,
    resourcesPath: process.resourcesPath || null,
    execPath: process.execPath || null,
    execDir: execDir || null,
    dirname: __dirname,
    assetsBase: assetsBase || null,
    assetsBaseUrl: assetsBaseUrl || null,
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
