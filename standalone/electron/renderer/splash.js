(function () {
  const statusElement = document.getElementById('status');
  const detailElement = document.getElementById('status-detail');
  const songsElement = document.getElementById('songs-info');
  const portElement = document.getElementById('port-info');
  const chooseButton = document.getElementById('choose-songs');
  const quitButton = document.getElementById('quit-app');
  const splashElement = document.querySelector('.splash');
  const mascotElement = document.getElementById('mascot');

  let diagnosticsOverlayShown = false;

  function renderDiagnosticsOverlay(info) {
    if (diagnosticsOverlayShown) {
      return;
    }
    if (!info || typeof info !== 'object') {
      return;
    }

    const formatValue = (value) => {
      if (value === null) {
        return 'null';
      }
      if (typeof value === 'undefined') {
        return 'undefined';
      }
      if (typeof value === 'string') {
        return value;
      }
      try {
        return JSON.stringify(value);
      } catch (error) {
        return String(value);
      }
    };

    const titleInfo = info.resolved && typeof info.resolved === 'object' ? info.resolved.title : null;
    const mascotInfo = info.resolved && typeof info.resolved === 'object' ? info.resolved.mascot : null;

    const overlay = document.createElement('div');
    overlay.style.position = 'fixed';
    overlay.style.left = '0';
    overlay.style.right = '0';
    overlay.style.bottom = '0';
    overlay.style.padding = '4px 8px';
    overlay.style.background = 'rgba(0, 0, 0, 0.7)';
    overlay.style.color = '#ffffff';
    overlay.style.fontSize = '12px';
    overlay.style.fontFamily = 'monospace';
    overlay.style.pointerEvents = 'none';
    overlay.style.zIndex = '2147483647';
    overlay.textContent =
      `base=${formatValue(info.assetsBase)} | ` +
      `title.path=${formatValue(titleInfo && typeof titleInfo === 'object' ? titleInfo.path : undefined)} ` +
      `exists=${titleInfo && typeof titleInfo === 'object' && titleInfo.exists ? 'true' : 'false'} | ` +
      `mascot.path=${formatValue(mascotInfo && typeof mascotInfo === 'object' ? mascotInfo.path : undefined)} ` +
      `exists=${mascotInfo && typeof mascotInfo === 'object' && mascotInfo.exists ? 'true' : 'false'}`;

    const appendOverlay = () => {
      if (!document.body) {
        return;
      }
      document.body.appendChild(overlay);
    };

    const finalize = () => {
      diagnosticsOverlayShown = true;
    };

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', appendOverlay, { once: true });
      document.addEventListener('DOMContentLoaded', finalize, { once: true });
    } else {
      appendOverlay();
      finalize();
    }
  }

  function resolveAssetUrl(...segments) {
    const desktopApi = window.desktop;
    if (!desktopApi || typeof desktopApi.getAssetUrl !== 'function') {
      return null;
    }
    try {
      const url = desktopApi.getAssetUrl(...segments);
      return typeof url === 'string' && url.length > 0 ? url : null;
    } catch (error) {
      return null;
    }
  }

  const ASSET_RETRY_LIMIT = 40;
  const ASSET_RETRY_DELAY = 250;
  const ASSET_INITIAL_DELAY = 50;

  let appliedBackgroundUrl = null;
  let appliedMascotUrl = null;
  let assetRetryTimer = null;
  let assetsResolved = false;
  let assetRetryCompleted = false;

  function applyAssetImages(backgroundUrl, mascotUrl) {
    appliedBackgroundUrl = typeof backgroundUrl === 'string' && backgroundUrl.length > 0 ? backgroundUrl : null;
    appliedMascotUrl = typeof mascotUrl === 'string' && mascotUrl.length > 0 ? mascotUrl : null;
    assetsResolved = Boolean(appliedBackgroundUrl && appliedMascotUrl);

    if (splashElement) {
      if (appliedBackgroundUrl) {
        splashElement.style.backgroundImage = `url("${appliedBackgroundUrl}")`;
      } else {
        splashElement.style.removeProperty('background-image');
      }
    }

    if (mascotElement) {
      if (appliedMascotUrl) {
        mascotElement.src = appliedMascotUrl;
        mascotElement.hidden = false;
      } else {
        mascotElement.removeAttribute('src');
        mascotElement.hidden = true;
      }
    }
  }

  function scheduleAssetRetry(attempt) {
    const delay = attempt === 0 ? ASSET_INITIAL_DELAY : ASSET_RETRY_DELAY;
    assetRetryTimer = window.setTimeout(() => {
      const backgroundUrl = resolveAssetUrl('launcher', 'title-screen.png');
      const mascotUrl = resolveAssetUrl('launcher', 'dancing-don.gif');

      if (backgroundUrl && mascotUrl) {
        applyAssetImages(backgroundUrl, mascotUrl);
        assetRetryTimer = null;
        assetRetryCompleted = true;
        return;
      }

      if (attempt + 1 >= ASSET_RETRY_LIMIT) {
        assetRetryTimer = null;
        assetRetryCompleted = true;
        return;
      }

      scheduleAssetRetry(attempt + 1);
    }, delay);
  }

  function startAssetRetry() {
    if (assetRetryTimer !== null) {
      window.clearTimeout(assetRetryTimer);
      assetRetryTimer = null;
    }
    assetRetryCompleted = false;
    scheduleAssetRetry(0);
  }

  function setDetail(text) {
    if (!detailElement) {
      return;
    }
    if (text && text.length > 0) {
      detailElement.hidden = false;
      detailElement.textContent = text;
      if (quitButton) {
        quitButton.hidden = false;
      }
    } else {
      detailElement.hidden = true;
      detailElement.textContent = '';
      if (quitButton) {
        quitButton.hidden = true;
      }
    }
  }

  function updateStatus(payload) {
    if (!payload) {
      return;
    }
    if (statusElement && typeof payload.message === 'string' && payload.message.length > 0) {
      statusElement.textContent = payload.message;
    }
    if ('errorMessage' in payload) {
      const message = payload.errorMessage;
      if (typeof message === 'string' && message.length > 0) {
        setDetail(message);
      } else {
        setDetail('');
      }
    }
    if (Object.prototype.hasOwnProperty.call(payload, 'detail')) {
      const message = payload.detail;
      if (typeof message === 'string' && message.length > 0) {
        setDetail(message);
      }
    }
    if (songsElement && Object.prototype.hasOwnProperty.call(payload, 'songsPath')) {
      const songsPath = payload.songsPath;
      songsElement.textContent = `Папка песен: ${songsPath && songsPath.length > 0 ? songsPath : '—'}`;
    }
    if (portElement && Object.prototype.hasOwnProperty.call(payload, 'port')) {
      const portValue = payload.port;
      portElement.textContent = `Порт: ${typeof portValue === 'number' ? portValue : portValue ? portValue : '—'}`;
    }
  }

  async function handleChooseSongsClick() {
    if (!window.desktop || typeof window.desktop.chooseSongsDir !== 'function' || !chooseButton) {
      return;
    }
    chooseButton.disabled = true;
    try {
      const result = await window.desktop.chooseSongsDir();
      if (result && typeof result === 'object' && result.error) {
        setDetail(result.error);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setDetail(message);
    } finally {
      chooseButton.disabled = false;
    }
  }

  if (chooseButton) {
    if (!window.desktop || typeof window.desktop.chooseSongsDir !== 'function') {
      chooseButton.disabled = true;
    }
    chooseButton.addEventListener('click', handleChooseSongsClick);
  }

  if (quitButton) {
    quitButton.addEventListener('click', () => {
      if (window.desktop && typeof window.desktop.requestQuit === 'function') {
        window.desktop.requestQuit();
      }
    });
  }

  function refreshAssetImages(attempt = 0) {
    setAssetImages();
    const desktopReady = window.desktop && typeof window.desktop.getAssetUrl === 'function';
    if (!desktopReady && attempt < 40) {
      window.setTimeout(() => refreshAssetImages(attempt + 1), 250);
    }
  }

  const handleDiagnosticsResult = (result) => {
    if (!result) {
      return;
    }
    renderDiagnosticsOverlay(result);
  };

  if (window.desktop && typeof window.desktop.diagnoseAssets === 'function') {
    try {
      const result = window.desktop.diagnoseAssets();
      if (result && typeof result.then === 'function') {
        result.then(handleDiagnosticsResult).catch(() => undefined);
      } else {
        handleDiagnosticsResult(result);
      }
    } catch (error) {
      // Ignore diagnostics errors to avoid breaking splash screen.
    }
  }

  if (window.desktop && typeof window.desktop.onStatus === 'function') {
    window.desktop.onStatus((payload) => {
      updateStatus(payload);
      if (!assetsResolved && assetRetryTimer === null && !assetRetryCompleted) {
        startAssetRetry();
      }
    });
  }

  window.addEventListener('focus', () => {
    applyAssetImages(appliedBackgroundUrl, appliedMascotUrl);
  });

  startAssetRetry();
})();
