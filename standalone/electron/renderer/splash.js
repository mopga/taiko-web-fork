(function () {
  const statusElement = document.getElementById('status');
  const detailElement = document.getElementById('status-detail');
  const songsElement = document.getElementById('songs-info');
  const portElement = document.getElementById('port-info');
  const chooseButton = document.getElementById('choose-songs');
  const quitButton = document.getElementById('quit-app');
  const splashElement = document.querySelector('.splash');
  const mascotElement = document.getElementById('mascot');

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
