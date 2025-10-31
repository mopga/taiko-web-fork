(function () {
  const statusElement = document.getElementById('status');
  const detailElement = document.getElementById('status-detail');
  const songsElement = document.getElementById('songs-info');
  const portElement = document.getElementById('port-info');
  const chooseButton = document.getElementById('choose-songs');
  const splashElement = document.querySelector('.splash');
  const mascotElement = document.getElementById('mascot');

  function setAssetImages() {
    if (splashElement) {
      splashElement.style.backgroundImage = "url('../assets/launcher/title-screen.png')";
    }
    if (mascotElement) {
      mascotElement.src = '../assets/launcher/dancing-don.gif';
    }
    if (!window.desktop || typeof window.desktop.getAssetUrl !== 'function') {
      return;
    }
    const backgroundUrl = window.desktop.getAssetUrl('launcher/title-screen.png');
    if (backgroundUrl && splashElement) {
      splashElement.style.backgroundImage = `url('${backgroundUrl}')`;
    }

    const mascotUrl = window.desktop.getAssetUrl('launcher/dancing-don.gif');
    if (mascotUrl && mascotElement) {
      mascotElement.src = mascotUrl;
    }
  }

  function setDetail(text) {
    if (!detailElement) {
      return;
    }
    if (text && text.length > 0) {
      detailElement.hidden = false;
      detailElement.textContent = text;
    } else {
      detailElement.hidden = true;
      detailElement.textContent = '';
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

  if (window.desktop && typeof window.desktop.onStatus === 'function') {
    window.desktop.onStatus(updateStatus);
  }

  setAssetImages();
})();
