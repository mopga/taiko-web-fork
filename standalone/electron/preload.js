const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('taiko', {
  openSongsFolder: () => ipcRenderer.invoke('open-songs-folder'),
  toggleFullscreen: () => ipcRenderer.invoke('toggle-fullscreen'),
  quitApp: () => ipcRenderer.invoke('graceful-quit'),
});
