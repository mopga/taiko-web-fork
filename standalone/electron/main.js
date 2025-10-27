const { app, BrowserWindow, Menu, ipcMain, shell, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const log = require('electron-log');
const treeKill = require('tree-kill');
const { spawn } = require('child_process');

const isDev = process.env.ELECTRON_DEV === '1' || process.env.NODE_ENV === 'development';

let mainWindow = null;
let backendProcess = null;
let backendUrl = null;
let backendReady = false;
let isShuttingDown = false;
let failureDialogOpen = false;

log.transports.file.level = 'info';
log.info('Starting Taiko Web Desktop (Fork)');

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

const songsDir = resolveDirectory('SONGS_DIR', path.join(app.getPath('music'), 'TaikoSongs'));
const dataDir = resolveDirectory('DATA_DIR', app.getPath('userData'));

ipcMain.handle('open-songs-folder', async () => openSongsFolder());
ipcMain.handle('toggle-fullscreen', async () => toggleFullscreen());
ipcMain.handle('graceful-quit', async () => {
  await initiateShutdown();
  app.quit();
  return true;
});

function resolveDirectory(envKey, fallbackPath) {
  const target = process.env[envKey] ? path.resolve(process.env[envKey]) : fallbackPath;
  if (!fs.existsSync(target)) {
    fs.mkdirSync(target, { recursive: true });
  }
  return target;
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

  mainWindow = new BrowserWindow({
    width: 1280,
    height: 720,
    useContentSize: true,
    show: false,
    backgroundColor: '#000000',
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

async function findAvailablePort(startPort = 3123, attempts = 20) {
  let port = startPort;
  for (let i = 0; i < attempts; i += 1) {
    const available = await new Promise((resolve) => {
      const server = http.createServer();
      server.once('error', (err) => {
        log.warn(`Port ${port} unavailable: ${err.code || err.message}`);
        try {
          server.close(() => resolve(false));
        } catch (closeError) {
          resolve(false);
        }
      });
      server.listen(port, '127.0.0.1', () => {
        server.close(() => resolve(true));
      });
      server.on('close', () => {});
      server.unref();
    });
    if (available) {
      return port;
    }
    port += 1;
  }
  throw new Error('Unable to find free port for backend');
}

function resolveBackendBinary() {
  const executableName = process.platform === 'win32' ? 'taiko-web-backend.exe' : 'taiko-web-backend';
  const relativePath = path.join('dist', 'backend', executableName);
  const binaryPath = isDev
    ? path.resolve(__dirname, '..', relativePath)
    : path.join(process.resourcesPath, relativePath);
  return binaryPath;
}

function spawnBackend(port) {
  const backendPath = resolveBackendBinary();
  if (!fs.existsSync(backendPath)) {
    throw new Error(`Backend binary not found at ${backendPath}`);
  }
  log.info('Spawning backend at', backendPath, 'on port', port);
  const env = {
    ...process.env,
    RUN_PROFILE: 'desktop',
    PORT: String(port),
    DATA_DIR: dataDir,
    SONGS_DIR: songsDir,
  };
  const child = spawn(backendPath, [], {
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
  });
  const limitLine = (line) => (line.length > 4096 ? `${line.slice(0, 4096)}…` : line);
  child.stdout.on('data', (chunk) => {
    chunk
      .toString()
      .split(/\r?\n/)
      .filter(Boolean)
      .forEach((line) => log.info('[backend]', limitLine(line)));
  });
  child.stderr.on('data', (chunk) => {
    chunk
      .toString()
      .split(/\r?\n/)
      .filter(Boolean)
      .forEach((line) => log.error('[backend]', limitLine(line)));
  });
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

function httpRequest(method, url, timeout = 1000) {
  return new Promise((resolve, reject) => {
    const request = http.request(url, { method, timeout }, (response) => {
      if (response.statusCode && response.statusCode >= 200 && response.statusCode < 300) {
        response.resume();
        resolve(true);
      } else {
        reject(new Error(`Unexpected status: ${response.statusCode}`));
      }
    });
    request.on('timeout', () => {
      request.destroy(new Error('Request timed out'));
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

  if (!process.env.ELECTRON_BACKEND_URL && backendUrl) {
    try {
      await httpRequest('POST', `${normalizeBaseUrl(backendUrl)}/shutdown`, 1000);
      log.info('Requested backend shutdown via HTTP');
    } catch (error) {
      log.warn('Failed to request backend shutdown', error.message);
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

      const timer = setTimeout(() => {
        if (backendProcess && backendProcess.pid) {
          log.warn('Forcing backend termination');
          treeKill(backendProcess.pid, 'SIGKILL', cleanup);
        } else {
          cleanup();
        }
      }, 5000);

      backendProcess.once('exit', () => {
        clearTimeout(timer);
        cleanup();
      });

      try {
        backendProcess.kill('SIGTERM');
      } catch (error) {
        log.warn('Error sending SIGTERM to backend', error.message);
        clearTimeout(timer);
        cleanup();
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
    title: 'Taiko Web Desktop (Fork)',
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

    const port = await findAvailablePort();
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

