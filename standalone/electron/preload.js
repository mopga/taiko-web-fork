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
// КРИТИЧЕСКИ ВАЖНО: В packaged режиме extraResources находятся в process.resourcesPath
// Структура: AppDir/resources/assets/launcher/
const getAssetSearchPaths = () => {
  const paths = [];
  
  // 1. process.resourcesPath/assets (САМЫЙ ВАЖНЫЙ - основной путь для extraResources)
  // В Electron process.resourcesPath указывает на AppDir/resources/ в packaged режиме
  if (process.resourcesPath && typeof process.resourcesPath === 'string') {
    const resourcesAssets = path.join(process.resourcesPath, 'assets');
    paths.push({ kind: 'resources-path', path: resourcesAssets, priority: 20 });
  }
  
  // 2. execDir/resources/assets (надежный fallback)
  // Если process.resourcesPath не установлен, вычисляем вручную
  if (process.execPath && typeof process.execPath === 'string') {
    const execDir = path.dirname(process.execPath);
    const execResourcesAssets = path.join(execDir, 'resources', 'assets');
    paths.push({ kind: 'exec-resources', path: execResourcesAssets, priority: 19 });
  }
  
  // 3. Альтернативный путь: если resourcesPath указывает не туда
  // Попробуем получить из process.defaultApp (может помочь в некоторых случаях)
  try {
    if (process.execPath) {
      const execDir = path.dirname(process.execPath);
      // Проверяем несколько вариантов
      const variant1 = path.join(execDir, 'resources', 'app.asar.unpacked', 'assets');
      paths.push({ kind: 'unpacked-resources', path: variant1, priority: 15 });
    }
  } catch (error) {
    // Игнорируем
  }
  
  // 4. __dirname relative paths (только для dev режима)
  // В packaged режиме это не сработает, так как мы внутри app.asar
  if (!isPackaged || process.env.ELECTRON_DEV === '1') {
    try {
      const dirnameAssets1 = path.resolve(__dirname, '..', 'assets');
      paths.push({ kind: 'dirname-up', path: dirnameAssets1, priority: 10 });
    } catch (error) {
      // Игнорируем ошибки
    }
    
    try {
      const dirnameAssets2 = path.resolve(__dirname, '..', '..', 'assets');
      paths.push({ kind: 'dirname-up-up', path: dirnameAssets2, priority: 9 });
    } catch (error) {
      // Игнорируем ошибки
    }
  }
  
  // Сортируем по приоритету (больше = выше приоритет)
  paths.sort((a, b) => b.priority - a.priority);
  
  return paths;
};

// Находим первую существующую директорию с ассетами
const findAssetsBase = () => {
  const searchPaths = getAssetSearchPaths();
  
  if (enableSplashDiag) {
    console.log('[desktop:preload] Searching for assets in paths:', searchPaths.map(p => `${p.kind}: ${p.path}`));
  }
  
  for (const { kind, path: searchPath } of searchPaths) {
    try {
      // НЕ используем path.resolve здесь, так как это может сломать абсолютные пути
      // Вместо этого используем path.normalize для очистки пути
      const normalizedPath = path.normalize(searchPath);
      
      if (enableSplashDiag) {
        console.log(`[desktop:preload] Checking ${kind}: ${normalizedPath}`);
      }
      
      if (fs.existsSync(normalizedPath)) {
        const stats = fs.statSync(normalizedPath);
        if (stats.isDirectory()) {
          // Проверяем, что это действительно директория с launcher подпапкой
          const launcherPath = path.join(normalizedPath, 'launcher');
          if (fs.existsSync(launcherPath)) {
            const launcherStats = fs.statSync(launcherPath);
            if (launcherStats.isDirectory()) {
              // Дополнительная проверка: убедимся, что файлы действительно там
              const titleScreenPath = path.join(launcherPath, 'title-screen.png');
              const dancingDonPath = path.join(launcherPath, 'dancing-don.gif');
              if (fs.existsSync(titleScreenPath) && fs.existsSync(dancingDonPath)) {
                if (enableSplashDiag) {
                  console.log(`[desktop:preload] ✓ Found assets at ${kind}: ${normalizedPath}`);
                  console.log(`[desktop:preload]   - title-screen.png: ${fs.existsSync(titleScreenPath) ? 'EXISTS' : 'MISSING'}`);
                  console.log(`[desktop:preload]   - dancing-don.gif: ${fs.existsSync(dancingDonPath) ? 'EXISTS' : 'MISSING'}`);
                }
                return normalizedPath;
              } else {
                if (enableSplashDiag) {
                  console.log(`[desktop:preload] ⚠ Found launcher dir but files missing at ${kind}: ${normalizedPath}`);
                }
              }
            }
          } else {
            if (enableSplashDiag) {
              console.log(`[desktop:preload] ✗ No launcher subdirectory at ${kind}: ${normalizedPath}`);
            }
          }
        } else {
          if (enableSplashDiag) {
            console.log(`[desktop:preload] ✗ Path exists but is not a directory at ${kind}: ${normalizedPath}`);
          }
        }
      } else {
        if (enableSplashDiag) {
          console.log(`[desktop:preload] ✗ Path does not exist at ${kind}: ${normalizedPath}`);
        }
      }
    } catch (error) {
      // Игнорируем ошибки и продолжаем поиск
      if (enableSplashDiag) {
        console.log(`[desktop:preload] ✗ Error checking ${kind} path ${searchPath}:`, error.message);
      }
    }
  }
  
  if (enableSplashDiag) {
    console.log('[desktop:preload] ✗ Could not find assets directory in any of the checked paths');
  }
  
  return null;
};

// Синхронный поиск ассетов
let assetsBase = findAssetsBase();
let assetsBaseCache = assetsBase; // Кэш для использования в функциях
let assetsBaseInitialized = !!assetsBase;
let assetsBaseUrl = assetsBase ? pathToFileURL(assetsBase).href : null;

// Если не нашли синхронно, попробуем через IPC (асинхронно)
if (!assetsBase && enableSplashDiag) {
  console.log('[desktop:preload] Assets not found synchronously, will try IPC method on first request');
  console.log('[desktop:preload] process.resourcesPath:', process.resourcesPath || 'not set');
  console.log('[desktop:preload] process.execPath:', process.execPath || 'not set');
  if (process.execPath) {
    const execDir = path.dirname(process.execPath);
    console.log('[desktop:preload] execDir:', execDir);
    const testPath = path.join(execDir, 'resources', 'assets');
    console.log('[desktop:preload] Test path exists:', fs.existsSync(testPath));
    if (fs.existsSync(testPath)) {
      const launcherPath = path.join(testPath, 'launcher');
      console.log('[desktop:preload] Launcher path exists:', fs.existsSync(launcherPath));
    }
  }
}

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

// Функция для получения пути к ассетам через IPC (более надежный способ)
async function getAssetsPathFromMain() {
  try {
    const assetsPath = await ipcRenderer.invoke('desktop:getAssetsPath');
    if (assetsPath && typeof assetsPath === 'string') {
      return assetsPath;
    }
  } catch (error) {
    if (enableSplashDiag) {
      console.log('[desktop:preload] Error getting assets path from main:', error.message);
    }
  }
  return null;
}

async function ensureAssetsBase() {
  if (assetsBaseCache) {
    return assetsBaseCache;
  }

  let basePath = findAssetsBase();

  if (!basePath) {
    basePath = await getAssetsPathFromMain();
  }

  if (basePath && typeof basePath === 'string') {
    assetsBaseCache = basePath;
    try {
      assetsBaseUrl = pathToFileURL(basePath).href;
    } catch (error) {
      if (enableSplashDiag) {
        console.log('[desktop:preload] Error converting assets base path to URL:', error.message);
      }
      assetsBaseUrl = null;
    }
    return assetsBaseCache;
  }

  return null;
}

function resolveAssetPath(...segments) {
  // Используем кэшированное значение или исходное
  const currentAssetsBase = assetsBaseCache || assetsBase;
  
  if (!currentAssetsBase) {
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
    // Используем path.join для безопасного объединения путей
    const assetPath = path.join(currentAssetsBase, ...parts);
    // Нормализуем путь (убираем лишние разделители, но сохраняем абсолютный путь)
    const normalizedPath = path.normalize(assetPath);
    
    if (fs.existsSync(normalizedPath)) {
      return normalizedPath;
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
  // Используем кэшированное значение или исходное
  const currentAssetsBase = assetsBaseCache || assetsBase;
  
  if (!currentAssetsBase) {
    if (enableSplashDiag) {
      console.log('[desktop:preload] getAssetUrl called but assetsBase is not set');
      console.log('[desktop:preload] Attempting to find assets base...');
    }
    // Попробуем найти еще раз синхронно
    assetsBaseCache = findAssetsBase();
    if (!assetsBaseCache) {
      if (enableSplashDiag) {
        console.log('[desktop:preload] Still could not find assets base');
      }
      return null;
    }
    if (enableSplashDiag) {
      console.log(`[desktop:preload] Found assets base on demand: ${assetsBaseCache}`);
    }
  }

  const assetPath =
    rest.length === 0 && typeof primary === 'string' && primary.includes('/')
      ? resolveAssetPath(...primary.split(/[\\/]+/))
      : resolveAssetPath(primary, ...rest);
  
  if (!assetPath) {
    if (enableSplashDiag) {
      console.log(`[desktop:preload] Could not resolve asset path for: ${primary}${rest.length > 0 ? ', ' + rest.join(', ') : ''}`);
      console.log(`[desktop:preload] Current assetsBase: ${currentAssetsBase || assetsBaseCache || 'null'}`);
    }
    return null;
  }
  
  try {
    if (fs.existsSync(assetPath)) {
      // pathToFileURL правильно обрабатывает Windows пути и конвертирует их в file:// URLs
      // На Windows это создаст file:///C:/path/to/file.png
      const url = pathToFileURL(assetPath).href;
      if (enableSplashDiag) {
        console.log(`[desktop:preload] ✓ Resolved asset URL: ${url}`);
        console.log(`[desktop:preload]   From path: ${assetPath}`);
      }
      return url;
    } else {
      if (enableSplashDiag) {
        console.log(`[desktop:preload] ✗ Asset file does not exist: ${assetPath}`);
        console.log(`[desktop:preload]   Assets base: ${currentAssetsBase || assetsBaseCache}`);
        console.log(`[desktop:preload]   Primary: ${primary}, Rest: ${rest.join(', ')}`);
      }
    }
  } catch (error) {
    if (enableSplashDiag) {
      console.log('[desktop:preload] ✗ Error generating asset URL:', error.message);
      if (error.stack) {
        console.log('[desktop:preload] Stack:', error.stack);
      }
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
  getAssetsPath: getAssetsPathFromMain,
  ensureAssetsBase,
  debugAssets,
};

if (enableSplashDiag) {
  desktopApi.diagnoseAssets = () => createDiagnoseAssets();
}

const frozenDesktopApi = Object.freeze(desktopApi);

try {
  contextBridge.exposeInMainWorld('desktop', frozenDesktopApi);
} catch (error) {
  console.error('[desktop:preload] Failed to expose desktop API:', error);
}
