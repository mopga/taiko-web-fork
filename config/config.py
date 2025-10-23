import os


def getenv_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    val = val.strip().lower()
    return val in ("1", "true", "t", "yes", "y", "on")

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except (TypeError, ValueError):
        return default


# Logging flags for the song scanner. ``SCAN_LOG_LEVEL`` accepts any standard
# Python logging level (``DEBUG``, ``INFO``, etc). The default ``INFO`` logs
# aggregated scanner progress without per-file details, while ``DEBUG`` adds
# verbose diagnostics that avoid PII. ``SCAN_LOG_SUMMARY`` controls whether the
# final summary line is emitted after each scan; disable it in exceptionally
# latency-sensitive environments.
SCAN_LOG_SUMMARY = getenv_bool("SCAN_LOG_SUMMARY", True)
SCAN_LOG_LEVEL = os.getenv("SCAN_LOG_LEVEL", "INFO").upper()

# The base URL for Taiko Web, with trailing slash.
BASEDIR = '/'

# The full URL base asset URL, with trailing slash.
ASSETS_BASEURL = '/assets/'

# The full URL base song URL, with trailing slash.
SONGS_BASEURL = '/songs/'

# Multiplayer websocket URL. Defaults to /p2 if blank.
MULTIPLAYER_URL = ''

# Send static files for custom error pages
ERROR_PAGES = {
    404: ''
}

# The email address to display in the "About Simulator" menu.
EMAIL = None

# Whether to use the user account system.
ACCOUNTS = True

# Custom JavaScript file to load with the simulator.
CUSTOM_JS = ''

# Default plugins to load with the simulator.
PLUGINS = [{
    'url': '',
    'start': False,
    'hide': False
}]

# Filetype to use for song previews. (mp3/ogg)
PREVIEW_TYPE = 'mp3'

# MongoDB server settings.
MONGO = {
    'host': os.getenv("MONGO_HOSTS", "127.0.0.1:27017").split(","),
    'database': os.getenv("MONGO_DB", "taiko"),
}

# Redis server settings, used for sessions + cache.
REDIS = {
    'CACHE_TYPE': os.getenv("CACHE_TYPE", "redis"),
    'CACHE_REDIS_HOST': os.getenv("TAIKO_WEB_REDIS_HOST", "127.0.0.1"),
    'CACHE_REDIS_PORT': getenv_int("CACHE_REDIS_PORT", 6379),
    'CACHE_REDIS_PASSWORD': os.getenv("CACHE_REDIS_PASSWORD") or None,
    'CACHE_REDIS_DB': getenv_int("CACHE_REDIS_DB", 0),
}
REDIS_URI = os.getenv("REDIS_URI") 

# Secret key used for sessions.
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")

# Git repository base URL.
URL = 'https://github.com/bui/taiko-web/'

# Google Drive API.
GOOGLE_CREDENTIALS = {
    'gdrive_enabled': False,
    'api_key': '',
    'oauth_client_id': '',
    'project_number': '',
    'min_level': None
}

# Song scanning configuration
SONGS_DIR = '/app/public/songs'
SCAN_ON_START = os.getenv('SCAN_ON_START', 'auto') or 'auto'
SCAN_IGNORE_GLOBS = ['**/.DS_Store', '**/Thumbs.db']
ADMIN_SCAN_TOKEN = os.getenv("SECRET_KEY", "change-me")
ENABLE_SONG_WATCHER = True

# Optimistic catalog mode (0/1). When enabled the catalog assumes songs are
# playable unless explicitly marked otherwise.
CATALOG_ASSUME_VALID = 0
