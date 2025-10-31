const { contextBridge, ipcRenderer } = require('electron');

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

contextBridge.exposeInMainWorld('desktop', {
  onStatus: onStatusUpdate,
  requestQuit: () => ipcRenderer.invoke('desktop:quit'),
});
