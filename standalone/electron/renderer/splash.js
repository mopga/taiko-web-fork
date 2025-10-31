(function () {
  const statusElement = document.getElementById('status');
  if (!statusElement) {
    return;
  }

  function updateStatus(payload) {
    if (!payload || typeof payload.message !== 'string') {
      return;
    }
    statusElement.textContent = payload.message;
  }

  if (window.desktop && typeof window.desktop.onStatus === 'function') {
    window.desktop.onStatus(updateStatus);
  }
})();
