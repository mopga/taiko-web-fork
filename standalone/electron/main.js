const { app, BrowserWindow, Menu, ipcMain, shell, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const log = require('electron-log');
const treeKill = require('tree-kill');
const { spawn } = require('child_process');

const APP_ID = 'com.taiko.web.desktop';
const BACKEND_PORT = 8000;
const isDev = process.env.ELECTRON_DEV === '1';
const isWindows = process.platform === 'win32';

let mainWindow = null;
let backendProcess = null;
let backendUrl = null;
let backendReady = false;
let isShuttingDown = false;
let failureDialogOpen = false;
let backendRoot = '';
let songsDir = '';

log.transports.file.level = 'info';
log.info('Starting Taiko Web Desktop');

if (isWindows) {
  app.setAppUserModelId(APP_ID);
}

if (!app.requestSingleInstanceLock()) {
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

backendRoot = resolveBackendRoot();
songsDir = ensureSongsDirectory(backendRoot);
log.info('Backend root resolved to', backendRoot);
log.info('Songs directory configured at', songsDir);

ipcMain.handle('open-songs-folder', async () => openSongsFolder());
ipcMain.handle('toggle-fullscreen', async () => toggleFullscreen());
ipcMain.handle('graceful-quit', async () => {
  await initiateShutdown();
  app.quit();
  return true;
});

function ensureDirectoryExists(targetPath) {
  if (!fs.existsSync(targetPath)) {
    fs.mkdirSync(targetPath, { recursive: true });
  }
  return targetPath;
}

function resolveBackendRoot() {
  const exeName = isWindows ? 'taiko-web-backend.exe' : 'taiko-web-backend';
  const candidates = [];
  if (isDev) {
    candidates.push(path.resolve(__dirname, '..', '..', 'dist', 'backend', 'taiko-web-backend'));
    candidates.push(path.resolve(__dirname, '..', 'dist', 'backend', 'taiko-web-backend'));
  } else {
    const installRoot = resolveInstallRoot();
    candidates.push(path.join(installRoot, 'resources', 'app', 'backend'));
    candidates.push(path.join(installRoot, 'resources', 'backend'));
    candidates.push(path.join(installRoot, 'backend'));
  }
  for (const candidate of candidates) {
    const binaryCandidate = path.join(candidate, exeName);
    if (fs.existsSync(binaryCandidate)) {
      return candidate;
    }
  }
  return candidates[0];
}

function ensureSongsDirectory(root) {
  const target = path.join(root, 'songs');
  return ensureDirectoryExists(target);
}

function resolveInstallRoot() {
  if (isDev) {
    return path.resolve(__dirname, '..', '..');
  }
  if (process.platform === 'darwin') {
    return path.resolve(process.execPath, '..', '..');
  }
  return path.dirname(process.execPath);
}

function resolveAssetPath(...segments) {
  if (!isDev && process.platform === 'darwin') {
    return path.join(resolveInstallRoot(), 'Resources', 'assets', 'launcher', ...segments);
  }
  return path.join(resolveInstallRoot(), 'assets', 'launcher', ...segments);
}

function resolveLauncherIcon() {
  const candidate = resolveAssetPath('app.ico');
  if (fs.existsSync(candidate)) {
    return candidate;
  }
  log.warn('Launcher icon not found at', candidate);
  return null;
}

function openSongsFolder() {
  log.info('Opening songs folder at', songsDir);
  return shell.openPath(songsDir);
}

function toggleFullscreen() {
  const focused = BrowserWindow.getFocusedWindow();
  if (focused) {
    const next = !focused.isFullScreen();
    focused.setFullScreen(next);
    return next;
  }
  return false;
}

async function quitAndExit() {
  await initiateShutdown();
  app.quit();
}

function createMenu() {
  const template = [];
  if (process.platform === 'darwin') {
    template.push({
      label: app.name,
      submenu: [
        { role: 'about' },
        { type: 'separator' },
        { role: 'hide' },
        { role: 'hideothers' },
        { role: 'unhide' },
        { type: 'separator' },
        {
          label: 'Quit',
          accelerator: 'Cmd+Q',
          click: () => {
            quitAndExit().catch((error) => log.error('Failed to quit application', error));
          },
        },
      ],
    });
  }

  template.push({
    label: 'File',
    submenu: [
      {
        label: 'Open Songs Folder',
        accelerator: 'CmdOrCtrl+O',
        click: () => openSongsFolder(),
      },
      { type: 'separator' },
      {
        label: 'Exit',
        accelerator: process.platform === 'darwin' ? 'Cmd+Q' : 'Alt+F4',
        click: () => {
          quitAndExit().catch((error) => log.error('Failed to quit application', error));
        },
      },
    ],
  });

  const viewSubmenu = [
    {
      label: 'Toggle Fullscreen',
      accelerator: process.platform === 'darwin' ? 'Ctrl+Cmd+F' : 'F11',
      click: () => toggleFullscreen(),
    },
  ];

  if (isDev) {
    viewSubmenu.push({
      label: 'Toggle Developer Tools',
      accelerator: 'CmdOrCtrl+Shift+I',
      click: () => {
        const focused = BrowserWindow.getFocusedWindow();
        if (focused) {
          focused.webContents.toggleDevTools();
        }
      },
    });
  }

  template.push({ label: 'View', submenu: viewSubmenu });

  if (!isDev) {
    template.push({ label: 'Help', submenu: [{ role: 'toggleDevTools', visible: false }] });
  }

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function createWindow() {
  if (!backendUrl) {
    throw new Error('Cannot create window before backend URL is defined');
  }
  const targetUrl = `${normalizeBaseUrl(backendUrl)}/`;
  if (mainWindow && !mainWindow.isDestroyed()) {
    log.info('Reloading UI from', targetUrl);
    mainWindow.loadURL(targetUrl);
    return mainWindow;
  }

  const preloadPath = path.join(__dirname, 'preload.js');
  const windowIcon = resolveLauncherIcon();

  mainWindow = new BrowserWindow({
    width: 1280,
    height: 720,
    useContentSize: true,
    show: false,
    backgroundColor: '#000000',
    icon: windowIcon || undefined,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      preload: preloadPath,
    },
  });

  createMenu();

  log.info('Loading UI from', targetUrl);
  mainWindow.loadURL(targetUrl);

  mainWindow.webContents.on('did-finish-load', () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.show();
    }
  });

  mainWindow.on('close', (event) => {
    if (isShuttingDown) {
      return;
    }
    event.preventDefault();
    quitAndExit().catch((error) => log.error('Failed to quit application', error));
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  return mainWindow;
}

async function isPortAvailable(port) {
  return new Promise((resolve) => {
    const server = http.createServer();
    const cleanUp = () => {
      try {
        server.close();
      } catch (error) {
        log.warn('Port check cleanup error', error.message);
      }
    };
    server.once('error', () => {
      cleanUp();
      resolve(false);
    });
    server.listen(port, '127.0.0.1', () => {
      cleanUp();
      resolve(true);
    });
    server.unref();
  });
}

async function allocateBackendPort() {
  const desired = BACKEND_PORT;
  const envPort = process.env.PORT ? Number(process.env.PORT) : null;
  if (envPort && envPort !== desired) {
    log.warn(`Ignoring PORT=${envPort}; desktop build uses fixed port ${desired}`);
  }
  const available = await isPortAvailable(desired);
  if (!available) {
    throw new Error(`Backend port ${desired} is unavailable. Is another instance running?`);
  }
  return desired;
}

function resolveBackendBinary() {
  const exeName = isWindows ? 'taiko-web-backend.exe' : 'taiko-web-backend';
  const root = backendRoot || resolveBackendRoot();
  return path.join(root, exeName);
}

function spawnBackend(port) {
  backendRoot = backendRoot || resolveBackendRoot();
  songsDir = ensureSongsDirectory(backendRoot);
  const backendPath = resolveBackendBinary();
  if (!fs.existsSync(backendPath)) {
    throw new Error(`Backend binary not found at ${backendPath}`);
  }
  log.info('Spawning backend at', backendPath, 'on port', port);
  const chosenPort = port;
  const env = {
    ...process.env,
    RUN_PROFILE: 'desktop',
    PORT: String(chosenPort),
  };
  const child = spawn(backendPath, [], {
    env,
    cwd: backendRoot,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
    windowsHide: true,
  });
  if (child.stdout) {
    child.stdout.setEncoding('utf-8');
    child.stdout.on('data', (chunk) => {
      chunk
        .toString()
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0)
        .forEach((line) => log.info('[backend]', line));
    });
  }
  if (child.stderr) {
    child.stderr.setEncoding('utf-8');
    child.stderr.on('data', (chunk) => {
      chunk
        .toString()
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0)
        .forEach((line) => log.error('[backend]', line));
    });
  }
  child.once('error', (error) => {
    log.error('Backend process error', error);
    if (!isShuttingDown) {
      handleBackendFailure('Backend process failed to launch.');
    }
  });
  child.once('exit', (code, signal) => {
    log.warn('Backend exited', { code, signal });
    if (backendProcess === child) {
      backendProcess = null;
    }
    if (isShuttingDown) {
      return;
    }
    backendReady = false;
    if (app.isReady()) {
      handleBackendFailure('Backend process exited unexpectedly.');
    }
  });
  backendProcess = child;
  return child;
}

function httpRequest(method, url, timeout = 1000, allowedStatuses = []) {
  return new Promise((resolve, reject) => {
    const request = http.request(url, { method, timeout }, (response) => {
      const statusCode = response.statusCode ?? 0;
      const acceptable =
        (statusCode >= 200 && statusCode < 300) || allowedStatuses.includes(statusCode);
      if (acceptable) {
        response.resume();
        resolve({ statusCode });
      } else {
        const error = new Error(`Unexpected status: ${statusCode}`);
        error.statusCode = statusCode;
        reject(error);
      }
    });
    request.on('timeout', () => {
      const timeoutError = new Error('Request timed out');
      timeoutError.code = 'ETIMEDOUT';
      request.destroy(timeoutError);
    });
    request.on('error', reject);
    request.end();
  });
}

function normalizeBaseUrl(url) {
  return url.replace(/\/+$/, '');
}

async function waitForHealthz(url) {
  const timeout = 45000;
  const startedAt = Date.now();
  let delay = 200;
  const baseUrl = normalizeBaseUrl(url);
  while (!isShuttingDown && Date.now() - startedAt < timeout) {
    try {
      await httpRequest('GET', `${baseUrl}/healthz`, 1500);
      log.info('Backend is healthy at', baseUrl);
      backendReady = true;
      return true;
    } catch (error) {
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 5000);
    }
  }
  return false;
}

async function initiateShutdown() {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  backendReady = false;
  log.info('Initiating graceful shutdown');

  let shutdownStatus = null;
  if (!process.env.ELECTRON_BACKEND_URL && backendUrl) {
    try {
      const response = await httpRequest(
        'POST',
        `${normalizeBaseUrl(backendUrl)}/admin/shutdown`,
        1500,
        [404],
      );
      shutdownStatus = response.statusCode;
      if (shutdownStatus === 404) {
        log.info('Backend shutdown endpoint unavailable (404); will terminate process manually.');
      } else {
        log.info('Requested backend shutdown via HTTP');
      }
    } catch (error) {
      if (error && error.statusCode === 404) {
        shutdownStatus = 404;
        log.info('Backend shutdown endpoint returned 404; terminating manually.');
      } else if (error && error.code === 'ETIMEDOUT') {
        log.warn('Backend shutdown request timed out; forcing termination.');
      } else {
        log.warn('Failed to request backend shutdown', error ? error.message : error);
      }
    }
  }

  if (backendProcess && backendProcess.pid) {
    await new Promise((resolve) => {
      let settled = false;
      const cleanup = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };

      const shutdownTimeoutMs = 1500;
      let timer = null;
      const forceTerminate = () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (backendProcess && backendProcess.pid) {
          log.warn('Forcing backend termination');
          treeKill(backendProcess.pid, 'SIGKILL', cleanup);
        } else {
          cleanup();
        }
      };

      timer = setTimeout(forceTerminate, shutdownTimeoutMs);

      backendProcess.once('exit', () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        cleanup();
      });

      if (shutdownStatus === 404) {
        forceTerminate();
        return;
      }

      try {
        if (isWindows) {
          treeKill(backendProcess.pid, 'SIGTERM', (error) => {
            if (error) {
              log.warn('Error sending SIGTERM to backend', error.message);
            }
          });
        } else {
          backendProcess.kill('SIGTERM');
        }
      } catch (error) {
        log.warn('Error sending SIGTERM to backend', error.message);
        forceTerminate();
      }
    });
  }
  backendProcess = null;
}

async function handleBackendFailure(message) {
  if (isShuttingDown || failureDialogOpen) {
    return;
  }
  failureDialogOpen = true;
  const result = await dialog.showMessageBox({
    type: 'error',
    buttons: ['Retry', 'Quit'],
    defaultId: 0,
    cancelId: 1,
    title: 'Taiko Web Desktop',
    message,
    detail: 'Would you like to retry starting the backend or quit the application?',
  });
  failureDialogOpen = false;
  if (result.response === 0) {
    startBackendFlow();
  } else {
    await initiateShutdown();
    app.quit();
  }
}

async function startBackendFlow() {
  if (isShuttingDown) {
    return;
  }

  backendReady = false;

  try {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.stop();
      mainWindow.hide();
    }

    if (process.env.ELECTRON_BACKEND_URL) {
      backendUrl = normalizeBaseUrl(process.env.ELECTRON_BACKEND_URL);
      log.info('Using external backend at', backendUrl);
      const healthy = await waitForHealthz(backendUrl);
      if (!healthy) {
        await handleBackendFailure('Failed to reach external backend.');
        return;
      }
      createWindow();
      return;
    }

    const port = await allocateBackendPort();
    backendUrl = normalizeBaseUrl(`http://127.0.0.1:${port}`);
    spawnBackend(port);
    const healthy = await waitForHealthz(backendUrl);
    if (!healthy) {
      log.error('Backend health check failed');
      if (backendProcess && backendProcess.pid) {
        treeKill(backendProcess.pid, 'SIGKILL');
      }
      backendProcess = null;
      await handleBackendFailure('Backend did not become ready in time.');
      return;
    }
    createWindow();
  } catch (error) {
    log.error('Failed to start backend flow', error);
    await handleBackendFailure('Failed to start the backend process.');
  }
}

app.whenReady().then(() => {
  startBackendFlow();
});

app.on('before-quit', (event) => {
  if (!isShuttingDown) {
    event.preventDefault();
    quitAndExit().catch((error) => log.error('Failed to quit application', error));
  }
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWindow === null && backendReady) {
    createWindow();
  }
});

['SIGINT', 'SIGTERM', 'SIGHUP'].forEach((signal) => {
  process.on(signal, async () => {
    log.info(`Received ${signal}, shutting down.`);
    await initiateShutdown();
    process.exit(0);
  });
});

