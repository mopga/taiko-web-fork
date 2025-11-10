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
  let preloadBannerTimeoutId = null;
  let preloadBannerWatcherId = null;

  const PRELOAD_FAILURE_BANNER_ID = 'preload-failure-banner';
  const PRELOAD_FAILURE_MESSAGE = 'Preload not loaded…';
  const PRELOAD_POLL_INTERVAL = 500;
  const PRELOAD_CHECK_TIMEOUT = 600;

  function isDesktopAvailable() {
    return !!(window.desktop && typeof window.desktop === 'object');
  }

  function hidePreloadFailureBanner() {
    const existingBanner = document.getElementById(PRELOAD_FAILURE_BANNER_ID);
    if (existingBanner && existingBanner.parentNode) {
      existingBanner.parentNode.removeChild(existingBanner);
    }
  }

  function showPreloadFailureBanner() {
    if (isDesktopAvailable()) {
      hidePreloadFailureBanner();
      return;
    }

    const existingBanner = document.getElementById(PRELOAD_FAILURE_BANNER_ID);
    if (existingBanner) {
      return;
    }

    const banner = document.createElement('div');
    banner.id = PRELOAD_FAILURE_BANNER_ID;
    banner.textContent = PRELOAD_FAILURE_MESSAGE;
    banner.style.position = 'fixed';
    banner.style.top = '0';
    banner.style.left = '0';
    banner.style.right = '0';
    banner.style.padding = '8px 12px';
    banner.style.background = '#b00020';
    banner.style.color = '#ffffff';
    banner.style.fontFamily = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
    banner.style.fontSize = '14px';
    banner.style.fontWeight = '600';
    banner.style.textAlign = 'center';
    banner.style.zIndex = '2147483646';

    const append = () => {
      if (!document.body) {
        return;
      }
      document.body.appendChild(banner);
    };

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', append, { once: true });
    } else {
      append();
    }
  }

  function schedulePreloadDiagnosticsBanner() {
    if (preloadBannerTimeoutId !== null) {
      window.clearTimeout(preloadBannerTimeoutId);
      preloadBannerTimeoutId = null;
    }
    preloadBannerTimeoutId = window.setTimeout(() => {
      preloadBannerTimeoutId = null;
      showPreloadFailureBanner();
    }, PRELOAD_CHECK_TIMEOUT);

    if (preloadBannerWatcherId === null) {
      preloadBannerWatcherId = window.setInterval(() => {
        if (isDesktopAvailable()) {
          hidePreloadFailureBanner();
          if (preloadBannerWatcherId !== null) {
            window.clearInterval(preloadBannerWatcherId);
            preloadBannerWatcherId = null;
          }
        }
      }, PRELOAD_POLL_INTERVAL);
    }
  }

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

    const format = (value) => {
      if (value === null) {
        return 'null';
      }
      if (typeof value === 'undefined') {
        return 'undefined';
      }
      if (typeof value === 'boolean') {
        return value ? 'true' : 'false';
      }
      if (typeof value === 'string') {
        return value;
      }
      return String(value);
    };

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

    const pre = document.createElement('pre');
    pre.style.margin = '0';
    pre.style.whiteSpace = 'pre-wrap';
    pre.textContent = [
      `assetsBase = ${format(info.assetsBase)}`,
      `title.exists = ${format(info.titleExists)}`,
      `title.url = ${format(info.titleUrl)}`,
      `mascot.exists = ${format(info.mascotExists)}`,
      `mascot.url = ${format(info.mascotUrl)}`,
    ].join('\n');

    overlay.appendChild(pre);

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

  async function requestDiagnosticsOverlay() {
    if (diagnosticsOverlayShown) {
      return;
    }

    const desktopApi = window.desktop;
    if (!desktopApi || typeof desktopApi !== 'object') {
      return;
    }

    let ensuredBase = null;

    if (typeof desktopApi.ensureAssetsBase === 'function') {
      try {
        ensuredBase = await desktopApi.ensureAssetsBase();
      } catch (error) {
        console.warn('[splash] Error ensuring assets base for diagnostics:', error);
      }
    }

    let diagnosis = null;
    if (typeof desktopApi.diagnoseAssets === 'function') {
      try {
        const result = desktopApi.diagnoseAssets();
        diagnosis = result && typeof result.then === 'function' ? await result : result;
      } catch (error) {
        console.warn('[splash] Error running asset diagnostics:', error);
      }
    }

    const resolved = diagnosis && typeof diagnosis === 'object' && diagnosis.resolved && typeof diagnosis.resolved === 'object'
      ? diagnosis.resolved
      : null;

    const getResolvedEntry = (key) => {
      if (!resolved || typeof resolved !== 'object') {
        return null;
      }
      if (!Object.prototype.hasOwnProperty.call(resolved, key)) {
        return null;
      }
      const value = resolved[key];
      return value && typeof value === 'object' ? value : null;
    };

    const resolvedTitle = getResolvedEntry('title');
    const resolvedMascot = getResolvedEntry('mascot');

    const titleUrl = resolveAssetUrlSync('launcher', 'title-screen.png');
    const mascotUrl = resolveAssetUrlSync('launcher', 'dancing-don.gif');

    const assetsBaseValue = (() => {
      if (typeof ensuredBase === 'string' && ensuredBase.length > 0) {
        return ensuredBase;
      }
      if (diagnosis && typeof diagnosis === 'object') {
        if (typeof diagnosis.assetsBase === 'string' && diagnosis.assetsBase.length > 0) {
          return diagnosis.assetsBase;
        }
        if (typeof diagnosis.activeBase === 'string' && diagnosis.activeBase.length > 0) {
          return diagnosis.activeBase;
        }
      }
      return null;
    })();

    const titleExists = resolvedTitle && Object.prototype.hasOwnProperty.call(resolvedTitle, 'exists')
      ? !!resolvedTitle.exists
      : !!titleUrl;
    const mascotExists = resolvedMascot && Object.prototype.hasOwnProperty.call(resolvedMascot, 'exists')
      ? !!resolvedMascot.exists
      : !!mascotUrl;

    renderDiagnosticsOverlay({
      assetsBase: assetsBaseValue,
      titleExists,
      titleUrl,
      mascotExists,
      mascotUrl,
    });
  }

  async function resolveAssetUrl(...segments) {
    const desktopApi = window.desktop;
    if (!desktopApi || typeof desktopApi.getAssetUrl !== 'function') {
      return null;
    }
    
    // Сначала убедимся, что assetsBase инициализирован
    if (desktopApi.ensureAssetsBase && typeof desktopApi.ensureAssetsBase === 'function') {
      try {
        await desktopApi.ensureAssetsBase();
      } catch (error) {
        // Игнорируем ошибки, продолжаем попытку
      }
    }
    
    try {
      const url = desktopApi.getAssetUrl(...segments);
      return typeof url === 'string' && url.length > 0 ? url : null;
    } catch (error) {
      console.error('[splash] Error resolving asset URL:', error);
      return null;
    }
  }
  
  // Синхронная версия для обратной совместимости
  function resolveAssetUrlSync(...segments) {
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

  async function setAssetImages(providedBackgroundUrl, providedMascotUrl) {
    let backgroundUrl = providedBackgroundUrl;
    let mascotUrl = providedMascotUrl;
    
    if (typeof providedBackgroundUrl === 'undefined') {
      backgroundUrl = await resolveAssetUrl('launcher', 'title-screen.png');
    }
    if (typeof providedMascotUrl === 'undefined') {
      mascotUrl = await resolveAssetUrl('launcher', 'dancing-don.gif');
    }

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

  async function runAssetRetryIteration() {
    if (assetRetryCompleted || assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      return;
    }

    assetRetryTimeoutId = null;
    assetRetryAttempts += 1;

    const backgroundUrl = await resolveAssetUrl('launcher', 'title-screen.png');
    const mascotUrl = await resolveAssetUrl('launcher', 'dancing-don.gif');

    await setAssetImages(backgroundUrl, mascotUrl);

    const hasBackground = typeof backgroundUrl === 'string' && backgroundUrl.length > 0;
    const hasMascot = typeof mascotUrl === 'string' && mascotUrl.length > 0;

    if (hasBackground && hasMascot) {
      assetRetryCompleted = true;
      return;
    }

    if (assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      console.warn('[splash] Failed to load assets after', ASSET_RETRY_LIMIT, 'attempts');
      return;
    }

    assetRetryTimeoutId = window.setTimeout(() => {
      runAssetRetryIteration().catch(err => {
        console.error('[splash] Error in asset retry iteration:', err);
      });
    }, ASSET_RETRY_INTERVAL);
  }

  async function refreshAssetImages({ immediate = false } = {}) {
    if (assetRetryCompleted) {
      await setAssetImages();
      return;
    }

    if (assetRetryAttempts >= ASSET_RETRY_LIMIT) {
      if (immediate) {
        await setAssetImages();
      }
      return;
    }

    if (immediate) {
      if (assetRetryTimeoutId !== null) {
        window.clearTimeout(assetRetryTimeoutId);
        assetRetryTimeoutId = null;
      }
      await runAssetRetryIteration();
      return;
    }

    if (assetRetryTimeoutId === null) {
      assetRetryTimeoutId = window.setTimeout(() => {
        runAssetRetryIteration().catch(err => {
          console.error('[splash] Error in asset retry:', err);
        });
      }, ASSET_INITIAL_DELAY);
    }
  }

  window.addEventListener('keydown', (event) => {
    if (event.key !== 'F9' || event.repeat) {
      return;
    }
    event.preventDefault();
    requestDiagnosticsOverlay().catch(err => {
      console.error('[splash] Error rendering diagnostics overlay:', err);
    });
  });

  if (window.desktop && typeof window.desktop.onStatus === 'function') {
    window.desktop.onStatus((payload) => {
      hidePreloadFailureBanner();
      refreshAssetImages({ immediate: true }).catch(err => {
        console.error('[splash] Error refreshing assets on status update:', err);
      });
      updateStatus(payload);
    });
  }

  schedulePreloadDiagnosticsBanner();
  if (isDesktopAvailable()) {
    hidePreloadFailureBanner();
  }

  window.addEventListener('focus', () => {
    setAssetImages(appliedBackgroundUrl, appliedMascotUrl).catch(err => {
      console.error('[splash] Error setting assets on focus:', err);
    });
  });

  // Гарантированно подставляем фон после готовности DOM
  async function tryLoadAssetsOnReady() {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => {
        tryLoadAssetsOnReady().catch(err => {
          console.error('[splash] Error loading assets on DOM ready:', err);
        });
      }, { once: true });
      return;
    }

    // Попытка загрузить ассеты сразу при готовности DOM
    const backgroundUrl = await resolveAssetUrl('launcher', 'title-screen.png');
    const mascotUrl = await resolveAssetUrl('launcher', 'dancing-don.gif');
    
    if (backgroundUrl || mascotUrl) {
      await setAssetImages(backgroundUrl, mascotUrl);
    }
    
    // Запускаем механизм retry на случай если ассеты еще не готовы
    await refreshAssetImages();
  }

  // Запускаем попытку загрузки
  tryLoadAssetsOnReady().catch(err => {
    console.error('[splash] Error in initial asset load:', err);
  });
})();

