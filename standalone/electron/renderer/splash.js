(function () {
  const statusElement = document.getElementById('status');
  const detailElement = document.getElementById('status-detail');
  const songsElement = document.getElementById('songs-info');
  const portElement = document.getElementById('port-info');
  const chooseButton = document.getElementById('choose-songs');
  const quitButton = document.getElementById('quit-app');
  const splashElement = document.querySelector('.splash-root');
  const mascotElement = document.getElementById('mascot');

  let appliedBackgroundUrl = null;
  let appliedMascotUrl = null;
  let diagnosticsOverlayShown = false;

  function applyAssetImages(backgroundUrl, mascotUrl) {
    const nextBackgroundUrl =
      typeof backgroundUrl === 'string' && backgroundUrl.length > 0 ? backgroundUrl : null;
    const nextMascotUrl =
      typeof mascotUrl === 'string' && mascotUrl.length > 0 ? mascotUrl : null;

    appliedBackgroundUrl = nextBackgroundUrl;
    appliedMascotUrl = nextMascotUrl;

    const bgElement = document.querySelector('.bg');
    if (bgElement) {
      if (appliedBackgroundUrl) {
        bgElement.style.backgroundImage = `url("${appliedBackgroundUrl}")`;
        bgElement.style.backgroundSize = 'cover';
        bgElement.style.backgroundPosition = 'center center';
        bgElement.style.backgroundRepeat = 'no-repeat';
      } else {
        bgElement.style.removeProperty('background-image');
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

  function setAssetImages(providedBackgroundUrl, providedMascotUrl) {
    const backgroundUrl =
      typeof providedBackgroundUrl === 'undefined'
        ? resolveAssetUrl('launcher', 'title-screen.png')
        : providedBackgroundUrl;
    const mascotUrl =
      typeof providedMascotUrl === 'undefined'
        ? resolveAssetUrl('launcher', 'dancing-don.gif')
        : providedMascotUrl;

    applyAssetImages(backgroundUrl, mascotUrl);
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

  const ASSET_RETRY_LIMIT = 40;
  const ASSET_RETRY_INTERVAL = 250;
  const ASSET_INITIAL_DELAY = 50;

  let assetRetryAttempts = 0;
  let assetRetryTimeoutId = null;
  let assetRetryCompleted = false;

  function runAssetRetryIteration() {
    if (assetRetryCompleted || assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      return;
    }

    assetRetryTimeoutId = null;
    assetRetryAttempts += 1;

    const backgroundUrl = resolveAssetUrl('launcher', 'title-screen.png');
    const mascotUrl = resolveAssetUrl('launcher', 'dancing-don.gif');

    setAssetImages(backgroundUrl, mascotUrl);

    const hasBackground = typeof backgroundUrl === 'string' && backgroundUrl.length > 0;
    const hasMascot = typeof mascotUrl === 'string' && mascotUrl.length > 0;

    if (hasBackground && hasMascot) {
      assetRetryCompleted = true;
      return;
    }

    if (assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      return;
    }

    assetRetryTimeoutId = window.setTimeout(runAssetRetryIteration, ASSET_RETRY_INTERVAL);
  }

  function refreshAssetImages({ immediate = false } = {}) {
    if (assetRetryCompleted) {
      setAssetImages();
      return;
    }

    if (assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      if (immediate) {
        setAssetImages();
      }
      return;
    }

    if (immediate) {
      if (assetRetryTimeoutId !== null) {
        window.clearTimeout(assetRetryTimeoutId);
        assetRetryTimeoutId = null;
      }
      runAssetRetryIteration();
      return;
    }

    if (assetRetryTimeoutId === null) {
      assetRetryTimeoutId = window.setTimeout(runAssetRetryIteration, ASSET_INITIAL_DELAY);
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
      refreshAssetImages({ immediate: true });
      updateStatus(payload);
    });
  }

  window.addEventListener('focus', () => {
    setAssetImages(appliedBackgroundUrl, appliedMascotUrl);
  });

  refreshAssetImages();

  // Гарантированно подставляем фон после готовности DOM и логируем URL
  window.addEventListener('DOMContentLoaded', () => {
    const api = window.desktop;
    api?.log?.(`[splash] __dirname=${typeof __dirname !== 'undefined' ? __dirname : 'n/a'} location=${location.href}`);
    api?.log?.(`[splash] diagnose=${JSON.stringify(api?.diagnoseAssets?.(), null, 2)}`);
    const url = api?.getAssetUrl?.('launcher', 'title-screen.png');
    api?.log?.(`[splash] url=${url}`);
    try {
      const bg = document.querySelector('.bg');
      const getUrl = window.desktop && window.desktop.getAssetUrl;
      if (!bg || !getUrl) {
        console.warn('[splash] bg or window.desktop.getAssetUrl is missing', { hasBG: !!bg, hasGet: !!getUrl });
        return;
      }
      const url2 = getUrl('launcher', 'title-screen.png');
      console.log('[splash] background url =', url2);
      if (url2) {
        bg.style.backgroundImage = `url("${url2}")`;
        // на всякий случай усилим «cover»/позицию (если в css не применилось)
        bg.style.backgroundSize = 'cover';
        bg.style.backgroundPosition = 'center center';
        bg.style.backgroundRepeat = 'no-repeat';
      }
    } catch (e) {
      console.error('[splash] failed to set background', e);
    }
  });
})();
