const { contextBridge, ipcRenderer } = require('electron');
const path = require('path');
const { pathToFileURL } = require('url');

const isPackaged = __dirname.includes('app.asar');
const assetsBase =
  process.env.ELECTRON_DEV === '1' || !isPackaged
    ? path.join(__dirname, '..', 'assets')
    : path.join(process.resourcesPath, 'assets');

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

  return path.join(assetsBase, ...parts);
}

function getAssetUrl(primary, ...rest) {
  const assetPath =
    rest.length === 0 && typeof primary === 'string' && primary.includes('/')
      ? resolveAssetPath(primary.split(/[\\/]+/))
      : resolveAssetPath(primary, rest);
  if (!assetPath) {
    return null;
  }
  return pathToFileURL(assetPath).href;
}

contextBridge.exposeInMainWorld('desktop', {
  onStatus: onStatusUpdate,
  requestQuit: () => ipcRenderer.invoke('desktop:quit'),
  chooseSongsDir: () => ipcRenderer.invoke('desktop:chooseSongsDir'),
  getAssetUrl,
});
