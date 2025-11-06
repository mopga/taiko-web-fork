const { app, BrowserWindow, dialog, ipcMain, Menu } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const https = require('https');
const net = require('net');
const { spawn } = require('child_process');
const treeKill = require('tree-kill');

const APP_ID = 'com.taikoweb.desktop';
const DEFAULT_PORT = 8000;
const HEALTH_TIMEOUT_MS = 30_000;
const SONGS_TIMEOUT_MS = 60_000;

let mainWindow = null;
let backendProcess = null;
let backendUrl = null;
let backendReady = false;
let quitting = false;
let starting = false;
let currentPort = null;
let dataDirPath = null;
let songsLinkPath = null;
let selectedSongsPath = null;
let songsScanPromise = null;
let lastStatusMessage = 'Запускаем Taiko Web…';
let lastStatusPayload = {
  message: lastStatusMessage,
  detail: null,
  progress: null,
  port: currentPort,
  songsPath: selectedSongsPath,
  errorMessage: null,
};

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

ipcMain.handle('desktop:chooseSongsDir', () => chooseSongsDirectory());

async function chooseSongsDirectory() {
  try {
    const info = ensureDataDirectory();
    emitStatus();
    const window = createMainWindow();
    const result = await dialog.showOpenDialog(window, {
      properties: ['openDirectory', 'createDirectory'],
    });
    if (result.canceled || !result.filePaths || result.filePaths.length === 0) {
      return { canceled: true };
    }
    const selectedPath = result.filePaths[0];
    const appliedPath = applySongsDirectory(selectedPath, info);
    updateStatus(`Папка песен: ${appliedPath}`);
    if (backendReady && backendUrl) {
      await runSongsScan();
    }
    return { canceled: false, path: appliedPath };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    updateStatus('Не удалось обновить папку песен.', { errorMessage: message });
    return { canceled: false, error: message };
  }
}

function setupApplicationMenu() {
  const template = [];

  if (process.platform === 'darwin') {
    template.push({
      label: app.name,
      submenu: [
        { role: 'about' },
        { type: 'separator' },
        { role: 'services' },
        { type: 'separator' },
        { role: 'hide' },
        { role: 'hideOthers' },
        { role: 'unhide' },
        { type: 'separator' },
        { role: 'quit' },
      ],
    });
  }

  template.push({
    label: 'File',
    submenu: [
      {
        label: 'Выбрать папку с песнями…',
        accelerator: 'CmdOrCtrl+O',
        click: () => {
          chooseSongsDirectory().catch(() => {
            // errors handled inside chooseSongsDirectory via status updates
          });
        },
      },
      { type: 'separator' },
      process.platform === 'darwin' ? { role: 'close' } : { role: 'quit' },
    ],
  });

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function resolveWindowIcon() {
  if (process.platform !== 'win32') {
    return undefined;
  }

  const candidates = [
    path.join(__dirname, '..', '..', 'assets', 'launcher', 'app.ico'),
  ];

  if (process.resourcesPath) {
    candidates.push(path.join(process.resourcesPath, 'assets', 'launcher', 'app.ico'));
    candidates.push(path.join(process.resourcesPath, 'app.ico'));
  }

  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    } catch (error) {
      // ignore
    }
  }

  return undefined;
}

app.whenReady().then(() => {
  setupApplicationMenu();
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

  const windowOptions = {
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
  };

  const iconPath = resolveWindowIcon();
  if (iconPath) {
    windowOptions.icon = iconPath;
  }

  const window = new BrowserWindow(windowOptions);

  window.once('ready-to-show', () => {
    window.show();
  });

  window.webContents.once('did-finish-load', () => {
    if (lastStatusPayload) {
      window.webContents.send('desktop:status', lastStatusPayload);
    }
  });

  window.on('closed', () => {
    mainWindow = null;
  });

  window.loadFile(path.join(__dirname, 'renderer', 'splash.html')).catch(() => {
    // window load failures handled later during bootstrap
  });

  mainWindow = window;
  emitStatus();
  return window;
}

async function startBackendFlow() {
  if (starting) {
    return;
  }
  starting = true;

  try {
    backendReady = false;
    backendUrl = null;
    currentPort = getEnvPort();
    songsScanPromise = null;

    updateStatus('Подготавливаем среду…');
    const backendExecutable = resolveBackendExecutable();
    const backendWorkingDir = path.dirname(backendExecutable);
    ensurePathExists(backendWorkingDir);

    const info = ensureDataDirectory();
    emitStatus();

    updateStatus('Определяем порт…');
    const port = await resolvePort();
    currentPort = port;
    emitStatus();
    backendUrl = `http://127.0.0.1:${port}`;

    updateStatus('Запускаем сервер…');
    backendProcess = spawnBackend(backendExecutable, backendWorkingDir, info.dataDir, port);

    updateStatus('Ожидаем запуск сервера…');
    await waitForHealthz(`${backendUrl}/healthz`, HEALTH_TIMEOUT_MS);

    await runSongsScan();

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
  fs.mkdirSync(dataDir, { recursive: true });

  const songsDir = path.join(dataDir, 'songs');
  try {
    if (!fs.existsSync(songsDir)) {
      fs.mkdirSync(songsDir, { recursive: true });
    }
  } catch (error) {
    if (error.code !== 'EEXIST') {
      throw error;
    }
  }

  dataDirPath = dataDir;
  songsLinkPath = songsDir;
  selectedSongsPath = resolveSongsTarget(songsDir);

  try {
    if (!fs.existsSync(selectedSongsPath)) {
      fs.mkdirSync(selectedSongsPath, { recursive: true });
    }
  } catch (error) {
    if (error.code !== 'EEXIST') {
      throw error;
    }
  }

  return {
    dataDir,
    songsLink: songsDir,
    songsTarget: selectedSongsPath,
  };
}

async function resolvePort() {
  const envPort = getEnvPort();
  if (envPort) {
    return envPort;
  }
  const preferred = DEFAULT_PORT;
  if (await checkPortAvailability(preferred)) {
    return preferred;
  }
  return findAvailablePort(20_000, 40_000);
}

function spawnBackend(executable, workingDir, dataDir, port) {
  const env = {
    ...process.env,
    RUN_PROFILE: 'desktop',
    PORT: String(port),
    DATA_DIR: dataDir,
    SONGS_DIR: selectedSongsPath ?? '',
    LOG_LEVEL: process.env.LOG_LEVEL ?? 'info',
  };

  const args = ['--host', '127.0.0.1', '--port', String(port)];
  const captureLogs = true; // always capture to file; forward to console in dev
  const child = spawn(executable, args, {
    cwd: workingDir,
    windowsHide: true,
    stdio: captureLogs ? ['ignore', 'pipe', 'pipe'] : 'ignore',
    env,
  });

  if (captureLogs) {
    setupBackendLogging(child);
  }

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

function setupBackendLogging(child) {
  try {
    const logsDir = path.join(app.getPath('userData'), 'logs');
    fs.mkdirSync(logsDir, { recursive: true });
    const timestamp = new Date().toISOString().replace(/[:]/g, '-');
    const logPath = path.join(logsDir, `backend-${timestamp}.log`);
    const logStream = fs.createWriteStream(logPath, { flags: 'a' });

    const forward = (chunk, isStdErr = false) => {
      if (!logStream.destroyed) {
        logStream.write(chunk);
      }
      if (process.env.ELECTRON_DEV === '1') {
        try {
          const text = chunk.toString('utf-8');
          if (isStdErr) {
            // eslint-disable-next-line no-console
            console.error(text.trimEnd());
          } else {
            // eslint-disable-next-line no-console
            console.log(text.trimEnd());
          }
        } catch (_) {
          // ignore console forwarding errors
        }
      }
    };

    if (child.stdout) {
      child.stdout.on('data', (c) => forward(c, false));
    }

    if (child.stderr) {
      child.stderr.on('data', (c) => forward(c, true));
    }

    const finalize = () => {
      if (!logStream.destroyed) {
        logStream.end();
      }
    };

    child.once('close', finalize);
    child.once('exit', finalize);
    // simple rotation: keep latest 10 files
    try {
      const files = fs
        .readdirSync(logsDir)
        .filter((f) => f.startsWith('backend-') && f.endsWith('.log'))
        .map((f) => ({ f, t: fs.statSync(path.join(logsDir, f)).mtimeMs }))
        .sort((a, b) => b.t - a.t);
      for (let i = 10; i < files.length; i += 1) {
        try {
          fs.unlinkSync(path.join(logsDir, files[i].f));
        } catch (_) {
          // ignore
        }
      }
    } catch (_) {
      // ignore rotation issues
    }
  } catch (error) {
    // ignore logging issues to avoid breaking the app in dev mode
  }
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

function updateStatus(message, extra = {}) {
  lastStatusMessage = message;
  emitStatus(extra);
}

function emitStatus(extra = {}) {
  const payload = {
    message: lastStatusMessage,
    detail: extra.detail ?? null,
    progress: typeof extra.progress === 'number' ? extra.progress : null,
    port: currentPort,
    songsPath: getCurrentSongsPath(),
    errorMessage: null,
    ...extra,
  };
  lastStatusPayload = payload;
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('desktop:status', payload);
  }
}

function getCurrentSongsPath() {
  if (selectedSongsPath) {
    return selectedSongsPath;
  }
  if (songsLinkPath) {
    return resolveSongsTarget(songsLinkPath);
  }
  return null;
}

function resolveSongsTarget(linkPath) {
  if (!linkPath) {
    return null;
  }
  try {
    const stats = fs.lstatSync(linkPath);
    if (stats.isSymbolicLink()) {
      const rawTarget = fs.readlinkSync(linkPath);
      if (path.isAbsolute(rawTarget)) {
        return path.normalize(rawTarget);
      }
      return path.normalize(path.resolve(path.dirname(linkPath), rawTarget));
    }
    return path.normalize(linkPath);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return path.normalize(linkPath);
    }
    throw error;
  }
}

function applySongsDirectory(targetPath, info) {
  const linkPath = (info && info.songsLink) || songsLinkPath;
  const dataDir = (info && info.dataDir) || dataDirPath;
  if (!linkPath) {
    throw new Error('Путь к каталогу песен недоступен.');
  }
  if (!targetPath) {
    throw new Error('Не выбран каталог с песнями.');
  }

  const resolvedTarget = path.normalize(path.resolve(targetPath));
  fs.mkdirSync(resolvedTarget, { recursive: true });

  if (path.normalize(path.resolve(linkPath)) === resolvedTarget) {
    selectedSongsPath = resolvedTarget;
    return resolvedTarget;
  }

  try {
    const stats = fs.lstatSync(linkPath);
    if (stats.isSymbolicLink()) {
      fs.unlinkSync(linkPath);
    } else if (stats.isDirectory()) {
      const backupName = `songs-backup-${Date.now()}`;
      const backupPath = path.join(dataDir ?? path.dirname(linkPath), backupName);
      fs.renameSync(linkPath, backupPath);
    } else {
      fs.unlinkSync(linkPath);
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }
  }

  const linkType = process.platform === 'win32' ? 'junction' : 'dir';
  fs.symlinkSync(resolvedTarget, linkPath, linkType);

  songsLinkPath = linkPath;
  selectedSongsPath = resolvedTarget;

  return resolvedTarget;
}

function runSongsScan() {
  if (!backendUrl) {
    return Promise.resolve(0);
  }
  if (songsScanPromise) {
    return songsScanPromise;
  }

  songsScanPromise = (async () => {
    updateStatus('Сканируем песни…');
    try {
      const count = await waitForSongs(`${backendUrl}/api/songs`, SONGS_TIMEOUT_MS);
      if (count > 0) {
        updateStatus(`Найдено песен: ${count}`);
      } else {
        updateStatus('Песни будут доступны после сканирования…');
      }
      return count;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      updateStatus('Не удалось обновить список песен.', { errorMessage: message });
      return 0;
    } finally {
      songsScanPromise = null;
    }
  })();

  return songsScanPromise;
}

function getEnvPort() {
  const sources = [process.env.TAIKO_DESKTOP_PORT, process.env.PORT, process.env.APP_PORT];
  for (const source of sources) {
    if (source === undefined || source === null) {
      continue;
    }
    const value = Number.parseInt(String(source), 10);
    if (Number.isInteger(value) && value > 0 && value < 65_536) {
      return value;
    }
  }
  return null;
}

function checkPortAvailability(port) {
  return new Promise((resolve) => {
    const server = net.createServer();

    const cleanup = () => {
      server.removeAllListeners('error');
      server.removeAllListeners('listening');
    };

    server.once('error', () => {
      cleanup();
      resolve(false);
    });

    server.once('listening', () => {
      server.close(() => {
        cleanup();
        resolve(true);
      });
    });

    try {
      server.listen(port, '127.0.0.1');
    } catch (error) {
      cleanup();
      resolve(false);
    }
  });
}

async function findAvailablePort(min, max) {
  const attempts = 30;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const candidate = min + Math.floor(Math.random() * (max - min + 1));
    if (await checkPortAvailability(candidate)) {
      return candidate;
    }
  }
  throw new Error('Не удалось подобрать свободный порт для запуска.');
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

function httpPost(url, timeoutMs) {
  return new Promise((resolve) => {
    try {
      const parsed = new URL(url);
      const lib = parsed.protocol === 'https:' ? https : http;
      const req = lib.request(
        {
          method: 'POST',
          protocol: parsed.protocol,
          hostname: parsed.hostname,
          port: parsed.port,
          path: parsed.pathname + parsed.search,
          timeout: timeoutMs,
          headers: { 'Content-Type': 'application/json' },
        },
        (res) => {
          res.resume();
          resolve();
        }
      );
      req.on('timeout', () => req.destroy());
      req.on('error', () => resolve());
      req.end();
    } catch (_) {
      resolve();
    }
  });
}

async function gracefulShutdown(child) {
  try {
    if (backendUrl) {
      await httpPost(`${backendUrl}/shutdown`, 3_000);
      await delay(1_000);
    }
  } catch (_) {
    // ignore
  }

  return new Promise((resolve) => {
    const timeoutForceKillMs = 10_000;
    const softTimeoutMs = 3_000;

    const finish = () => resolve();

    const onExit = () => {
      child.removeListener('exit', onExit);
      finish();
    };

    child.once('exit', onExit);

    try {
      if (process.platform === 'win32') {
        treeKill(child.pid, 'SIGTERM');
      } else {
        child.kill('SIGTERM');
      }
    } catch (_) {
      // ignore
    }

    setTimeout(() => {
      try {
        if (process.platform === 'win32') {
          treeKill(child.pid, 'SIGKILL');
        } else if (!child.killed) {
          child.kill('SIGKILL');
        }
      } catch (_) {
        // ignore
      }
    }, softTimeoutMs);

    setTimeout(() => {
      child.removeListener('exit', onExit);
      finish();
    }, timeoutForceKillMs);
  });
}

function stopBackend() {
  if (!backendProcess) {
    return Promise.resolve();
  }

  const child = backendProcess;
  backendProcess = null;
  backendReady = false;
  backendUrl = null;
  emitStatus();

  return gracefulShutdown(child);
}

