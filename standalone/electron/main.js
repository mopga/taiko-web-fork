const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const https = require('https');
const { spawn } = require('child_process');
const treeKill = require('tree-kill');

const APP_ID = 'com.taikoweb.desktop';
const DEFAULT_PORT = 8000;
const HEALTH_TIMEOUT_MS = 60_000;
const SONGS_TIMEOUT_MS = 60_000;

let mainWindow = null;
let backendProcess = null;
let backendUrl = null;
let backendReady = false;
let quitting = false;
let starting = false;
let lastStatusMessage = '';

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
}

app.on('second-instance', () => {
  if (mainWindow) {
    if (mainWindow.isMinimized()) {
      mainWindow.restore();
    }
    mainWindow.focus();
  }
});

if (process.platform === 'win32') {
  app.setAppUserModelId(APP_ID);
}

ipcMain.handle('desktop:quit', async () => {
  quitting = true;
  await stopBackend();
  app.quit();
});

app.whenReady().then(() => {
  createMainWindow();
  startBackendFlow();
});

app.on('before-quit', (event) => {
  if (quitting) {
    return;
  }
  event.preventDefault();
  quitting = true;
  stopBackend().finally(() => {
    app.exit();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWindow === null) {
    createMainWindow();
    if (!backendReady && !starting) {
      startBackendFlow();
    }
  }
});

process.on('exit', () => {
  quitting = true;
  if (backendProcess) {
    try {
      treeKill(backendProcess.pid);
    } catch (error) {
      // noop
    }
  }
});

function createMainWindow() {
  if (mainWindow) {
    return mainWindow;
  }

  const window = new BrowserWindow({
    width: 1280,
    height: 720,
    show: false,
    useContentSize: true,
    backgroundColor: '#000000',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  window.once('ready-to-show', () => {
    window.show();
  });

  window.webContents.once('did-finish-load', () => {
    if (lastStatusMessage) {
      window.webContents.send('desktop:status', { message: lastStatusMessage });
    }
  });

  window.on('closed', () => {
    mainWindow = null;
  });

  window.loadFile(path.join(__dirname, 'renderer', 'splash.html')).catch(() => {
    // window load failures handled later during bootstrap
  });

  mainWindow = window;
  return window;
}

async function startBackendFlow() {
  if (starting) {
    return;
  }
  starting = true;

  try {
    updateStatus('Подготавливаем среду…');
    const backendExecutable = resolveBackendExecutable();
    const backendWorkingDir = path.dirname(backendExecutable);
    ensurePathExists(backendWorkingDir);

    const dataDir = ensureDataDirectory();
    const port = resolvePort();
    backendUrl = `http://127.0.0.1:${port}`;

    updateStatus('Запускаем сервер…');
    backendProcess = spawnBackend(backendExecutable, backendWorkingDir, dataDir, port);

    updateStatus('Ожидаем запуск сервера…');
    await waitForHealthz(`${backendUrl}/healthz`, HEALTH_TIMEOUT_MS);

    updateStatus('Сканируем песни…');
    const songCount = await waitForSongs(`${backendUrl}/api/songs`, SONGS_TIMEOUT_MS);
    if (songCount > 0) {
      updateStatus(`Найдено песен: ${songCount}`);
    } else {
      updateStatus('Песни будут доступны после сканирования…');
    }

    backendReady = true;
    const window = createMainWindow();
    await window.loadURL(`${backendUrl}/`);
  } catch (error) {
    backendReady = false;
    await stopBackend();
    await showStartupError(error);
  } finally {
    starting = false;
  }
}

function resolveBackendExecutable() {
  const exeName = process.platform === 'win32' ? 'taiko-web-backend.exe' : 'taiko-web-backend';
  if (process.env.ELECTRON_DEV === '1' || !app.isPackaged) {
    const candidate = path.resolve(__dirname, '..', 'dist', 'backend', 'taiko-web-backend', exeName);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
    throw new Error(`Backend executable not found at ${candidate}`);
  }

  const packaged = path.join(process.resourcesPath, 'backend', exeName);
  if (fs.existsSync(packaged)) {
    return packaged;
  }
  throw new Error(`Backend executable not found at ${packaged}`);
}

function ensurePathExists(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function ensureDataDirectory() {
  const userRoot = app.getPath('userData');
  const dataDir = path.join(userRoot, 'taiko-web-data');
  const songsDir = path.join(dataDir, 'songs');
  fs.mkdirSync(songsDir, { recursive: true });
  return dataDir;
}

function resolvePort() {
  const sources = [process.env.PORT, process.env.APP_PORT];
  for (const source of sources) {
    const value = Number.parseInt(source ?? '', 10);
    if (Number.isInteger(value) && value > 0 && value < 65_536) {
      return value;
    }
  }
  return DEFAULT_PORT;
}

function spawnBackend(executable, workingDir, dataDir, port) {
  const env = {
    ...process.env,
    RUN_PROFILE: 'desktop',
    PORT: String(port),
    DATA_DIR: dataDir,
  };

  const args = ['--host', '127.0.0.1', '--port', String(port), '--data-dir', dataDir];
  const child = spawn(executable, args, {
    cwd: workingDir,
    windowsHide: true,
    stdio: 'ignore',
    env,
  });

  child.unref();

  child.once('exit', (code, signal) => {
    if (quitting) {
      return;
    }
    backendProcess = null;
    if (!backendReady) {
      showStartupError(new Error('Бэкенд завершился до запуска.')).catch(() => {
        app.quit();
      });
    } else {
      showFatalBackendExit(code, signal);
    }
  });

  child.once('error', (error) => {
    if (quitting) {
      return;
    }
    backendProcess = null;
    showStartupError(error).catch(() => {
      app.quit();
    });
  });

  return child;
}

async function waitForHealthz(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const payload = await fetchJson(url);
      if (payload && payload.status === 'ok' && payload.profile === 'desktop') {
        const dbPath = payload.db_path;
        if (typeof dbPath === 'string') {
          return payload;
        }
      }
    } catch (error) {
      // retry until timeout
    }
    await delay(500);
  }
  throw new Error('Не удалось дождаться готовности бэкенда.');
}

async function waitForSongs(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const payload = await fetchJson(url);
      return extractSongCount(payload);
    } catch (error) {
      lastError = error;
      await delay(500);
    }
  }
  if (lastError) {
    throw lastError;
  }
  return 0;
}

function extractSongCount(payload) {
  if (!payload) {
    return 0;
  }
  if (Array.isArray(payload)) {
    return payload.length;
  }
  if (payload.items && Array.isArray(payload.items)) {
    return payload.items.length;
  }
  return 0;
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const request = lib.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        timeout: 5_000,
      },
      (response) => {
        if (response.statusCode && response.statusCode >= 200 && response.statusCode < 300) {
          const chunks = [];
          response.on('data', (chunk) => chunks.push(chunk));
          response.on('end', () => {
            try {
              const body = Buffer.concat(chunks).toString('utf-8');
              resolve(JSON.parse(body));
            } catch (error) {
              reject(error);
            }
          });
        } else {
          reject(new Error(`Unexpected status ${response.statusCode}`));
        }
      }
    );

    request.on('timeout', () => {
      request.destroy(new Error('Request timeout'));
    });

    request.on('error', reject);
    request.end();
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function updateStatus(message) {
  lastStatusMessage = message;
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('desktop:status', { message });
  }
}

async function showStartupError(error) {
  const message = error instanceof Error ? error.message : String(error);
  const window = createMainWindow();
  const result = await dialog.showMessageBox(window, {
    type: 'error',
    title: 'Taiko Web',
    message: 'Не удалось запустить сервер.',
    detail: message,
    buttons: ['Повторить', 'Выход'],
    defaultId: 0,
    cancelId: 1,
  });

  if (result.response === 0) {
    startBackendFlow();
  } else {
    quitting = true;
    await stopBackend();
    app.quit();
  }
}

function showFatalBackendExit(code, signal) {
  const message = `Бэкенд завершился (${signal ?? code ?? 'unknown'}). Приложение будет закрыто.`;
  const window = createMainWindow();
  dialog
    .showMessageBox(window, {
      type: 'error',
      title: 'Taiko Web',
      message,
      buttons: ['OK'],
    })
    .finally(() => {
      quitting = true;
      stopBackend().finally(() => {
        app.quit();
      });
    });
}

function stopBackend() {
  if (!backendProcess) {
    return Promise.resolve();
  }

  const child = backendProcess;
  backendProcess = null;

  return new Promise((resolve) => {
    const cleanup = () => {
      resolve();
    };

    const onExit = () => {
      child.removeListener('exit', onExit);
      cleanup();
    };

    child.once('exit', onExit);

    try {
      if (process.platform === 'win32') {
        treeKill(child.pid, 'SIGTERM', () => {
          setTimeout(() => {
            try {
              treeKill(child.pid, 'SIGKILL');
            } catch (error) {
              // ignore
            }
          }, 2000);
        });
      } else {
        child.kill('SIGTERM');
        setTimeout(() => {
          try {
            if (!child.killed) {
              child.kill('SIGKILL');
            }
          } catch (error) {
            // ignore
          }
        }, 2000);
      }
    } catch (error) {
      cleanup();
    }

    setTimeout(() => {
      child.removeListener('exit', onExit);
      cleanup();
    }, 5000);
  });
}

