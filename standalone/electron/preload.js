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

function resolveAssetPath(relativePath) {
  if (typeof relativePath !== 'string' || relativePath.length === 0) {
    return null;
  }
  return path.join(assetsBase, relativePath);
}

function getAssetUrl(relativePath) {
  const assetPath = resolveAssetPath(relativePath);
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
