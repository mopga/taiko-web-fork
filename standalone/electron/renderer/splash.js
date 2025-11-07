(function () {
  const statusElement = document.getElementById('status');
  const detailElement = document.getElementById('status-detail');
  const songsElement = document.getElementById('songs-info');
  const portElement = document.getElementById('port-info');
  const chooseButton = document.getElementById('choose-songs');
  const quitButton = document.getElementById('quit-app');
  const splashElement = document.querySelector('.splash');
  const mascotElement = document.getElementById('mascot');

  function resolveAssetSegments(values) {
    const parts = [];
    const push = (value) => {
      if (typeof value !== 'string') {
        return;
      }
      const token = value.trim();
      if (token) {
        parts.push(token);
      }
    };
    const flatten = (segment) => {
      if (Array.isArray(segment)) {
        segment.forEach(flatten);
        return;
      }
      push(segment);
    };
    values.forEach(flatten);
    return parts;
  }

  function encodeAssetPath(segments) {
    return segments
      .map((segment) =>
        segment
          .split('/')
          .map((token) => encodeURIComponent(token))
          .join('/')
      )
      .join('/');
  }

  function buildUrlFromBase(baseUrl, segments) {
    if (!baseUrl || typeof baseUrl !== 'string') {
      return null;
    }
    try {
      const normalizedBase = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
      const encodedPath = encodeAssetPath(segments);
      const candidate = new URL(encodedPath, normalizedBase);
      return candidate.href;
    } catch (error) {
      return null;
    }
  }

  function fallbackAssetUrl(segments) {
    const desktopApi = window.desktop;
    const debugAssets = desktopApi && desktopApi.debugAssets;
    if (!debugAssets) {
      return null;
    }

    const attempted = new Set();
    const attemptBaseUrl = (baseUrl) => {
      if (!baseUrl || attempted.has(baseUrl)) {
        return null;
      }
      attempted.add(baseUrl);
      return buildUrlFromBase(baseUrl, segments);
    };

    if (Array.isArray(debugAssets.bases)) {
      for (const base of debugAssets.bases) {
        if (!base || typeof base.url !== 'string') {
          continue;
        }
        const result = attemptBaseUrl(base.url);
        if (result) {
          return result;
        }
      }
    }

    if (typeof debugAssets.activeBaseUrl === 'string') {
      const activeResult = attemptBaseUrl(debugAssets.activeBaseUrl);
      if (activeResult) {
        return activeResult;
      }
    }

    return null;
  }

  function resolveAssetUrl(...segments) {
    const parts = resolveAssetSegments(segments);
    if (!parts.length) {
      return null;
    }
    const desktopApi = window.desktop;
    const getAssetUrl =
      desktopApi && typeof desktopApi.getAssetUrl === 'function'
        ? desktopApi.getAssetUrl.bind(desktopApi)
        : null;
    let assetUrl = null;
    if (getAssetUrl) {
      try {
        assetUrl = getAssetUrl(...parts);
      } catch (error) {
        assetUrl = null;
      }
    }
    if (assetUrl) {
      return assetUrl;
    }
    return fallbackAssetUrl(parts);
  }

  function setAssetImages() {
    const backgroundUrl = resolveAssetUrl('launcher', 'title-screen.png');
    if (splashElement) {
      if (backgroundUrl) {
        const safeBackgroundUrl = String(backgroundUrl).replace(/"/g, '\\"');
        splashElement.style.backgroundImage = `url("${safeBackgroundUrl}")`;
      } else {
        splashElement.style.removeProperty('background-image');
      }
    }

    const mascotUrl = resolveAssetUrl('launcher', 'dancing-don.gif');
    if (mascotElement) {
      if (mascotUrl) {
        mascotElement.src = mascotUrl;
        mascotElement.hidden = false;
      } else {
        mascotElement.removeAttribute('src');
        mascotElement.hidden = true;
      }
    }
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

  if (window.desktop && typeof window.desktop.onStatus === 'function') {
    window.desktop.onStatus((payload) => {
      refreshAssetImages();
      updateStatus(payload);
    });
  }

  window.addEventListener('focus', () => {
    setAssetImages();
  });

  refreshAssetImages();
})();
