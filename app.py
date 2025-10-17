#!/usr/bin/env python3

import base64
import bcrypt
import hashlib
import importlib
import importlib.util
import json
import mimetypes
import os
import re
import requests
import schema
import threading
import time
from collections import defaultdict
from pathlib import Path

# -- カスタム --
from datetime import datetime

import flask

# ----

from functools import wraps
from flask import Flask, g, jsonify, render_template, request, abort, redirect, session, flash, make_response, send_from_directory
from flask_caching import Cache
from flask_session import Session
from flask_wtf.csrf import CSRFProtect, generate_csrf, CSRFError
from ffmpy import FFmpeg
from pymongo import MongoClient
from redis import Redis

from songs_scanner import SongScanner


def _load_config_module():
    """Load configuration module from several possible locations."""

    module_name = os.environ.get("TAIKO_WEB_CONFIG_MODULE")
    search_order = []
    if module_name:
        search_order.append(module_name)
    search_order.extend(["config.config", "config"])

    for name in search_order:
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue

    path_candidates = [
        Path(os.environ.get("TAIKO_WEB_CONFIG_PATH", "config.py")),
        Path("config/config.py"),
    ]
    for config_path in path_candidates:
        if not config_path.exists():
            continue
        spec = importlib.util.spec_from_file_location("config", config_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
            return module

    raise FileNotFoundError('No such file or directory: \'config.py\'. Copy the example config file config.example.py to config.py')


config = _load_config_module()

mimetypes.add_type("audio/ogg", ".ogg")
mimetypes.add_type("audio/mpeg", ".mp3")

def take_config(name, required=False):
    if hasattr(config, name):
        return getattr(config, name)
    if required:
        raise ValueError('Required option is not defined in the config.py file: {}'.format(name))
    return None

app = Flask(__name__)

mongo_config = take_config('MONGO') or {}
mongo_uri = os.environ.get("TAIKO_WEB_MONGO_URI") or mongo_config.get('uri')
mongo_host = os.environ.get("TAIKO_WEB_MONGO_HOST") or mongo_config.get('host')

if mongo_uri:
    client = MongoClient(mongo_uri)
else:
    if not mongo_host:
        mongo_host = ['127.0.0.1:27017']
    client = MongoClient(host=mongo_host)

basedir = os.environ.get('BASEDIR') or take_config('BASEDIR') or '/'

app.secret_key = take_config('SECRET_KEY') or 'change-me'
app.config['SESSION_TYPE'] = 'redis'
redis_config = dict(take_config('REDIS', required=True))
redis_host_env = os.environ.get("TAIKO_WEB_REDIS_HOST")
if redis_host_env:
    redis_config['CACHE_REDIS_HOST'] = redis_host_env
redis_port_env = os.environ.get("TAIKO_WEB_REDIS_PORT")
if redis_port_env:
    redis_config['CACHE_REDIS_PORT'] = int(redis_port_env)
redis_password_env = os.environ.get("TAIKO_WEB_REDIS_PASSWORD")
if redis_password_env is not None:
    redis_config['CACHE_REDIS_PASSWORD'] = redis_password_env or None
redis_db_env = os.environ.get("TAIKO_WEB_REDIS_DB")
if redis_db_env is not None:
    redis_config['CACHE_REDIS_DB'] = int(redis_db_env)
app.config['SESSION_REDIS'] = Redis(
    host=redis_config['CACHE_REDIS_HOST'],
    port=redis_config['CACHE_REDIS_PORT'],
    password=redis_config.get('CACHE_REDIS_PASSWORD'),
    db=redis_config.get('CACHE_REDIS_DB'),
)
app.cache = Cache(app, config=redis_config)
sess = Session()
sess.init_app(app)
#csrf = CSRFProtect(app)

db_name = os.environ.get("TAIKO_WEB_MONGO_DB") or mongo_config.get('database') or 'taiko'
db = client[db_name]
db.users.create_index('username', unique=True)
db.songs.create_index('id', unique=True)
try:
    db.songs.create_index([('audioHash', 1), ('titleNormalized', 1)], unique=True, sparse=True)
except Exception:
    app.logger.debug('Could not ensure audioHash/titleNormalized index')
db.scores.create_index('username')
try:
    db.song_scanner_state.create_index('tja_path', unique=True)
except Exception:
    app.logger.debug('Could not ensure song_scanner_state index')


@app.route('/healthz')
def route_healthcheck():
    status = {'status': 'ok'}
    try:
        client.admin.command('ping')
        status['mongo'] = 'ok'
    except Exception:
        status['status'] = 'error'
        status['mongo'] = 'error'
        return jsonify(status), 503
    try:
        redis_client = app.config.get('SESSION_REDIS')
        if redis_client:
            redis_client.ping()
        status['redis'] = 'ok'
    except Exception:
        status['status'] = 'error'
        status['redis'] = 'error'
        return jsonify(status), 503
    return jsonify(status)

def _resolve_baseurl(value):
    if not value:
        return '/songs/'
    if value.startswith('http://') or value.startswith('https://') or value.startswith('/'):
        return value if value.endswith('/') else value + '/'
    resolved = basedir + value
    return resolved if resolved.endswith('/') else resolved + '/'


SONGS_DIR_PATH = Path(os.environ.get('SONGS_DIR') or take_config('SONGS_DIR') or os.path.join(os.getcwd(), 'public', 'songs')).expanduser().resolve()
SCAN_ON_START = take_config('SCAN_ON_START')
ENABLE_SONG_WATCHER_DEFAULT = True


def _coerce_bool(value, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return default
    return text not in {'0', 'false', 'no', 'off'}


ENABLE_SONG_WATCHER = _coerce_bool(
    os.environ.get('ENABLE_SONG_WATCHER'),
    _coerce_bool(take_config('ENABLE_SONG_WATCHER'), ENABLE_SONG_WATCHER_DEFAULT),
)
scan_env = os.environ.get('SCAN_ON_START')
if scan_env is not None:
    SCAN_ON_START = scan_env.lower() in ('1', 'true', 'yes', 'on')
elif SCAN_ON_START is None:
    SCAN_ON_START = True
SCAN_IGNORE_GLOBS = take_config('SCAN_IGNORE_GLOBS') or ['**/.DS_Store', '**/Thumbs.db']
ADMIN_SCAN_TOKEN = os.environ.get('ADMIN_SCAN_TOKEN') or take_config('ADMIN_SCAN_TOKEN') or 'change-me'
SONGS_BASEURL_VALUE = _resolve_baseurl(os.environ.get('SONGS_BASEURL') or take_config('SONGS_BASEURL'))
COERCE_UNKNOWN_COURSE = os.environ.get('COERCE_UNKNOWN_COURSE') or take_config('COERCE_UNKNOWN_COURSE')

song_scanner = SongScanner(
    db=db,
    songs_dir=SONGS_DIR_PATH,
    songs_baseurl=SONGS_BASEURL_VALUE,
    ignore_globs=SCAN_IGNORE_GLOBS,
    coerce_unknown_course=COERCE_UNKNOWN_COURSE,
)

_song_watcher_handle = None


class HashException(Exception):
    pass


def api_error(message):
    return jsonify({'status': 'error', 'message': message})


def generate_hash(id, form):
    md5 = hashlib.md5()
    if form['type'] == 'tja':
        urls = ['%s%s/main.tja' % (take_config('SONGS_BASEURL', required=True), id)]
    else:
        urls = []
        for diff in ['easy', 'normal', 'hard', 'oni', 'ura']:
            if form['course_' + diff]:
                urls.append('%s%s/%s.osu' % (take_config('SONGS_BASEURL', required=True), id, diff))

    for url in urls:
        if url.startswith("http://") or url.startswith("https://"):
            resp = requests.get(url)
            if resp.status_code != 200:
                raise HashException('Invalid response from %s (status code %s)' % (resp.url, resp.status_code))
            md5.update(resp.content)
        else:
            if url.startswith(basedir):
                url = url[len(basedir):]
            path = os.path.normpath(os.path.join("public", url))
            if not os.path.isfile(path):
                raise HashException("File not found: %s" % (os.path.abspath(path)))
            with open(path, "rb") as file:
                md5.update(file.read())

    return base64.b64encode(md5.digest())[:-2].decode('utf-8')


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('username'):
            return api_error('not_logged_in')
        return f(*args, **kwargs)
    return decorated_function


def admin_required(level):
    def decorated_function(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get('username'):
                return abort(403)
            
            user = db.users.find_one({'username': session.get('username')})
            if user['user_level'] < level:
                return abort(403)

            return f(*args, **kwargs)
        return wrapper
    return decorated_function


@app.errorhandler(CSRFError)
def handle_csrf_error(e):
    return api_error('invalid_csrf')


@app.before_request
def before_request_func():
    if session.get('session_id'):
        if not db.users.find_one({'session_id': session.get('session_id')}):
            session.clear()


def get_config(credentials=False):
    config_out = {
        'basedir': basedir,
        'songs_baseurl': SONGS_BASEURL_VALUE,
        'assets_baseurl': take_config('ASSETS_BASEURL', required=True),
        'email': take_config('EMAIL'),
        'accounts': take_config('ACCOUNTS'),
        'custom_js': take_config('CUSTOM_JS'),
        'plugins': take_config('PLUGINS') and [x for x in take_config('PLUGINS') if x['url']],
        'preview_type': take_config('PREVIEW_TYPE') or 'mp3',
        'multiplayer_url': take_config('MULTIPLAYER_URL')
    }
    relative_urls = ['songs_baseurl', 'assets_baseurl']
    for name in relative_urls:
        if not config_out[name].startswith("/") and not config_out[name].startswith("http://") and not config_out[name].startswith("https://"):
            config_out[name] = basedir + config_out[name]
    if credentials:
        google_credentials = take_config('GOOGLE_CREDENTIALS')
        min_level = google_credentials['min_level'] or 0
        if not session.get('username'):
            user_level = 0
        else:
            user = db.users.find_one({'username': session.get('username')})
            user_level = user['user_level']
        if user_level >= min_level:
            config_out['google_credentials'] = google_credentials
        else:
            config_out['google_credentials'] = {
                'gdrive_enabled': False
            }

    if not config_out.get('songs_baseurl'):
        config_out['songs_baseurl'] = ''.join([request.host_url, 'songs']) + '/'
    if not config_out.get('assets_baseurl'):
        config_out['assets_baseurl'] = ''.join([request.host_url, 'assets']) + '/'

    config_out['_version'] = get_version()
    return config_out

def get_version():
    version = {'commit': None, 'commit_short': '', 'version': None, 'url': take_config('URL')}
    if os.path.isfile('version.json'):
        try:
            ver = json.load(open('version.json', 'r'))
        except ValueError:
            print('Invalid version.json file')
            return version

        for key in version.keys():
            if ver.get(key):
                version[key] = ver.get(key)

    return version

def get_db_don(user):
    don_body_fill = user['don_body_fill'] if 'don_body_fill' in user else get_default_don('body_fill')
    don_face_fill = user['don_face_fill'] if 'don_face_fill' in user else get_default_don('face_fill')
    return {'body_fill': don_body_fill, 'face_fill': don_face_fill}

def get_default_don(part=None):
    if part == None:
        return {
            'body_fill': get_default_don('body_fill'),
            'face_fill': get_default_don('face_fill')
        }
    elif part == 'body_fill':
        return '#5fb7c1'
    elif part == 'face_fill':
        return '#ff5724'

def is_hex(input):
    try:
        int(input, 16)
        return True
    except ValueError:
        return False


@app.route(basedir)
def route_index():
    version = get_version()

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    return render_template('index.html', version=version, config=get_config(), year=year, month=month, day=day)


@app.route(basedir + 'api/csrftoken')
def route_csrftoken():
    return jsonify({'status': 'ok', 'token': generate_csrf()})


@app.route(basedir + 'admin')
@admin_required(level=50)
def route_admin():
    return redirect(basedir + 'admin/songs')


@app.route(basedir + 'admin/songs')
@admin_required(level=50)
def route_admin_songs():
    songs = sorted(list(db.songs.find({})), key=lambda x: x['id'])
    categories = db.categories.find({})
    user = db.users.find_one({'username': session['username']})
    return render_template('admin_songs.html', songs=songs, admin=user, categories=list(categories), config=get_config())


@app.route(basedir + 'admin/songs/<int:id>')
@admin_required(level=50)
def route_admin_songs_id(id):
    song = db.songs.find_one({'id': id})
    if not song:
        return abort(404)

    categories = list(db.categories.find({}))
    song_skins = list(db.song_skins.find({}))
    makers = list(db.makers.find({}))
    user = db.users.find_one({'username': session['username']})

    return render_template('admin_song_detail.html',
        song=song, categories=categories, song_skins=song_skins, makers=makers, admin=user, config=get_config())


def _get_next_song_id():
    seq = db.seq.find_one({'name': 'songs'})
    seq_value = seq['value'] if seq else 0

    highest_song = db.songs.find_one(sort=[('id', -1)])
    if highest_song and highest_song['id'] > seq_value:
        seq_value = highest_song['id']

    return seq_value + 1


@app.route(basedir + 'admin/songs/new')
@admin_required(level=100)
def route_admin_songs_new():
    categories = list(db.categories.find({}))
    song_skins = list(db.song_skins.find({}))
    makers = list(db.makers.find({}))
    seq_new = _get_next_song_id()

    return render_template('admin_song_new.html', categories=categories, song_skins=song_skins, makers=makers, config=get_config(), id=seq_new)


@app.route(basedir + 'admin/songs/new', methods=['POST'])
@admin_required(level=100)
def route_admin_songs_new_post():
    output = {'title_lang': {}, 'subtitle_lang': {}, 'courses': {}}
    output['enabled'] = True if request.form.get('enabled') else False
    output['title'] = request.form.get('title') or None
    output['subtitle'] = request.form.get('subtitle') or None
    for lang in ['ja', 'en', 'cn', 'tw', 'ko']:
        output['title_lang'][lang] = request.form.get('title_%s' % lang) or None
        output['subtitle_lang'][lang] = request.form.get('subtitle_%s' % lang) or None

    for course in ['easy', 'normal', 'hard', 'oni', 'ura']:
        if request.form.get('course_%s' % course):
            output['courses'][course] = {'stars': int(request.form.get('course_%s' % course)),
                                         'branch': True if request.form.get('branch_%s' % course) else False}
        else:
            output['courses'][course] = None
    
    output['category_id'] = int(request.form.get('category_id')) or None
    output['type'] = request.form.get('type')
    output['music_type'] = request.form.get('music_type')
    output['offset'] = float(request.form.get('offset')) or None
    output['skin_id'] = int(request.form.get('skin_id')) or None
    output['preview'] = float(request.form.get('preview')) or None
    output['volume'] = float(request.form.get('volume')) or None
    output['maker_id'] = int(request.form.get('maker_id')) or None
    output['lyrics'] = True if request.form.get('lyrics') else False
    output['hash'] = request.form.get('hash')
    
    seq_new = _get_next_song_id()
    
    hash_error = False
    if request.form.get('gen_hash'):
        try:
            output['hash'] = generate_hash(seq_new, request.form)
        except HashException as e:
            hash_error = True
            flash('An error occurred: %s' % str(e), 'error')
    
    output['id'] = seq_new
    output['order'] = seq_new
    
    db.songs.insert_one(output)
    if not hash_error:
        flash('Song created.')
    
    db.seq.update_one({'name': 'songs'}, {'$set': {'value': seq_new}}, upsert=True)
    
    return redirect(basedir + 'admin/songs/%s' % str(seq_new))


@app.route(basedir + 'admin/songs/<int:id>', methods=['POST'])
@admin_required(level=50)
def route_admin_songs_id_post(id):
    song = db.songs.find_one({'id': id})
    if not song:
        return abort(404)

    user = db.users.find_one({'username': session['username']})
    user_level = user['user_level']

    output = {'title_lang': {}, 'subtitle_lang': {}, 'courses': {}}
    if user_level >= 100:
        output['enabled'] = True if request.form.get('enabled') else False

    output['title'] = request.form.get('title') or None
    output['subtitle'] = request.form.get('subtitle') or None
    for lang in ['ja', 'en', 'cn', 'tw', 'ko']:
        output['title_lang'][lang] = request.form.get('title_%s' % lang) or None
        output['subtitle_lang'][lang] = request.form.get('subtitle_%s' % lang) or None

    for course in ['easy', 'normal', 'hard', 'oni', 'ura']:
        if request.form.get('course_%s' % course):
            output['courses'][course] = {'stars': int(request.form.get('course_%s' % course)),
                                         'branch': True if request.form.get('branch_%s' % course) else False}
        else:
            output['courses'][course] = None
    
    output['category_id'] = int(request.form.get('category_id')) or None
    output['type'] = request.form.get('type')
    output['music_type'] = request.form.get('music_type')
    output['offset'] = float(request.form.get('offset')) or None
    output['skin_id'] = int(request.form.get('skin_id')) or None
    output['preview'] = float(request.form.get('preview')) or None
    output['volume'] = float(request.form.get('volume')) or None
    output['maker_id'] = int(request.form.get('maker_id')) or None
    output['lyrics'] = True if request.form.get('lyrics') else False
    output['hash'] = request.form.get('hash')
    
    hash_error = False
    if request.form.get('gen_hash'):
        try:
            output['hash'] = generate_hash(id, request.form)
        except HashException as e:
            hash_error = True
            flash('An error occurred: %s' % str(e), 'error')
    
    db.songs.update_one({'id': id}, {'$set': output})
    if not hash_error:
        flash('Changes saved.')
    
    return redirect(basedir + 'admin/songs/%s' % id)


@app.route(basedir + 'admin/songs/<int:id>/delete', methods=['POST'])
@admin_required(level=100)
def route_admin_songs_id_delete(id):
    song = db.songs.find_one({'id': id})
    if not song:
        return abort(404)

    db.songs.delete_one({'id': id})
    flash('Song deleted.')
    return redirect(basedir + 'admin/songs')


@app.route(basedir + 'admin/users')
@admin_required(level=50)
def route_admin_users():
    user = db.users.find_one({'username': session.get('username')})
    max_level = user['user_level'] - 1
    return render_template('admin_users.html', config=get_config(), max_level=max_level, username='', level='')


@app.route(basedir + 'admin/users', methods=['POST'])
@admin_required(level=50)
def route_admin_users_post():
    admin_name = session.get('username')
    admin = db.users.find_one({'username': admin_name})
    max_level = admin['user_level'] - 1
    
    username = request.form.get('username')
    try:
        level = int(request.form.get('level')) or 0
    except ValueError:
        level = 0
    
    user = db.users.find_one({'username_lower': username.lower()})
    if not user:
        flash('Error: User was not found.')
    elif admin['username'] == user['username']:
        flash('Error: You cannot modify your own level.')
    else:
        user_level = user['user_level']
        if level < 0 or level > max_level:
            flash('Error: Invalid level.')
        elif user_level > max_level:
            flash('Error: This user has higher level than you.')
        else:
            output = {'user_level': level}
            db.users.update_one({'username': user['username']}, {'$set': output})
            flash('User updated.')
    
    return render_template('admin_users.html', config=get_config(), max_level=max_level, username=username, level=level)


@app.route(basedir + 'api/preview')
@app.cache.cached(timeout=15, query_string=True)
def route_api_preview():
    song_id = request.args.get('id', None)
    if not song_id or not re.match('^[0-9]{1,9}$', song_id):
        abort(400)

    song_id = int(song_id)
    song = db.songs.find_one({'id': song_id})
    if not song:
        abort(400)

    song_type = song['type']
    song_ext = song['music_type'] if song['music_type'] else "mp3"
    prev_path = make_preview(song_id, song_type, song_ext, song['preview'])
    if not prev_path:
        return redirect(get_config()['songs_baseurl'] + '%s/main.%s' % (song_id, song_ext))

    return redirect(get_config()['songs_baseurl'] + '%s/preview.mp3' % song_id)


@app.route(basedir + 'api/songs')
@app.cache.cached(timeout=60, query_string=True)
def route_api_songs():
    include_disabled = request.args.get('include_disabled', '').lower() in ('1', 'true', 'yes', 'all')
    query = {} if include_disabled else {'enabled': True}
    songs = list(db.songs.find(query, {'_id': False}))
    for song in songs:
        song.setdefault('titleJa', None)
        song.setdefault('subtitleJa', None)
        maker_id = song.get('maker_id')
        if maker_id is not None:
            if maker_id == 0:
                song['maker'] = 0
            else:
                song['maker'] = db.makers.find_one({'id': maker_id}, {'_id': False})
        else:
            song['maker'] = None
        song.pop('maker_id', None)

        category_id = song.get('category_id')
        genre_value = song.get('genre')
        category_value = None
        if isinstance(genre_value, str) and genre_value.strip():
            category_value = genre_value.strip()
        elif category_id is not None:
            category_doc = db.categories.find_one({'id': category_id})
            category_value = category_doc['title'] if category_doc else 'Unsorted'
        else:
            category_value = 'Unsorted'
        song['category'] = category_value

        skin_id = song.get('skin_id')
        if skin_id:
            song['song_skin'] = db.song_skins.find_one({'id': skin_id}, {'_id': False, 'id': False})
        else:
            song['song_skin'] = None
        song.pop('skin_id', None)
        song.pop('managed_by_scanner', None)

        paths = song.get('paths') or {}
        if 'tja_url' not in paths and song.get('type') == 'tja':
            paths['tja_url'] = '%s%s/main.tja' % (SONGS_BASEURL_VALUE, song['id'])
        if 'audio_url' not in paths:
            music_type = song.get('music_type')
            if music_type:
                paths['audio_url'] = '%s%s/main.%s' % (SONGS_BASEURL_VALUE, song['id'], music_type)
        if 'dir_url' not in paths:
            paths['dir_url'] = '%s%s/' % (SONGS_BASEURL_VALUE, song['id'])
        song['paths'] = paths
        song['song_path'] = paths.get('tja_url')
        song['audio_path'] = paths.get('audio_url')

        if not song.get('music_type') and paths.get('audio_url'):
            song['music_type'] = paths['audio_url'].split('.')[-1].lower()

    return cache_wrap(flask.jsonify(songs), 60)

@app.route(basedir + 'api/categories')
@app.cache.cached(timeout=15)
def route_api_categories():
    categories = list(db.categories.find({},{'_id': False}))
    return jsonify(categories)


@app.route(basedir + 'import/report')
def route_import_report():
    state_collection = getattr(db, 'song_scanner_state', None)
    if state_collection is None:
        abort(404)

    try:
        cursor = state_collection.find({}, {'_id': False})
    except Exception:
        app.logger.exception('Failed to load song scanner state for report')
        cursor = []

    grouped: defaultdict[str, list] = defaultdict(list)
    for doc in cursor:
        if not isinstance(doc, dict):
            continue
        key = doc.get('group_key') or doc.get('tja_path') or 'ungrouped'
        grouped[str(key)].append(doc)

    report_groups = []
    for key in sorted(grouped.keys()):
        docs = grouped[key]
        song_id = None
        title = None
        normalized_title = None
        audio_url = None
        issues: set[str] = set()
        diagnostics: set[str] = set()
        total_valid = 0
        total_charts = 0
        records = []

        for doc in docs:
            if song_id is None and isinstance(doc.get('song_id'), int):
                song_id = doc['song_id']
            record = doc.get('record') if isinstance(doc.get('record'), dict) else {}
            if not title and isinstance(record.get('title'), str):
                title = record['title']
            if not normalized_title and isinstance(record.get('normalized_title'), str):
                normalized_title = record['normalized_title']
            if not audio_url and isinstance(record.get('audio_url'), str) and record['audio_url']:
                audio_url = record['audio_url']

            record_issues = set(record.get('import_issues', []) or [])
            issues.update(record_issues)
            diagnostics.update(set(record.get('diagnostics', []) or []))

            charts_raw = record.get('charts', []) or []
            chart_entries = []
            for chart in charts_raw:
                if not isinstance(chart, dict):
                    continue
                chart_entry = {
                    'course': chart.get('course') or 'Unknown',
                    'level': chart.get('level'),
                    'valid': bool(chart.get('valid')),
                    'issues': list(chart.get('issues', []) or []),
                    'coerced': bool(chart.get('coerced')),
                    'tja_path': doc.get('tja_path'),
                }
                chart_entries.append(chart_entry)
                if chart_entry['valid']:
                    total_valid += 1
                total_charts += 1

            records.append({
                'tja_path': doc.get('tja_path'),
                'relative_dir': record.get('relative_dir'),
                'title': record.get('title'),
                'genre': record.get('genre'),
                'category_title': record.get('category_title'),
                'audio_url': record.get('audio_url'),
                'import_issues': sorted(record_issues),
                'diagnostics': sorted(set(record.get('diagnostics', []) or [])),
                'valid_charts': sum(1 for chart in chart_entries if chart['valid']),
                'charts': chart_entries,
            })

        group_entry = {
            'group_key': key,
            'song_id': song_id,
            'title': title,
            'normalized_title': normalized_title,
            'audio_url': audio_url,
            'issues': sorted(issues),
            'diagnostics': sorted(diagnostics),
            'valid_chart_count': total_valid,
            'total_charts': total_charts,
            'records': records,
        }
        report_groups.append(group_entry)

    generated_at = datetime.utcnow()
    summary = {
        'groups': len(report_groups),
        'records': sum(len(group['records']) for group in report_groups),
        'groups_with_issues': sum(1 for group in report_groups if group['issues']),
        'total_charts': sum(group['total_charts'] for group in report_groups),
        'valid_charts': sum(group['valid_chart_count'] for group in report_groups),
    }

    response_format = request.args.get('format', 'html').lower()
    if response_format == 'json':
        payload = {
            'generated_at': generated_at.isoformat() + 'Z',
            'summary': summary,
            'groups': report_groups,
        }
        return jsonify(payload)

    return render_template(
        'import_report.html',
        groups=report_groups,
        summary=summary,
        generated_at=generated_at,
    )


def invalidate_song_cache():
    try:
        app.cache.delete_memoized(route_api_songs)
    except Exception:
        pass
    try:
        app.cache.delete_memoized(route_api_categories)
    except Exception:
        pass


def perform_song_scan(*, full: bool = False):
    summary = song_scanner.scan(full=full)
    invalidate_song_cache()
    app.logger.info("Song scan finished: %s", summary)
    return summary


def _get_scan_token():
    header_token = request.headers.get('X-Scan-Token')
    if header_token:
        return header_token.strip()
    auth_header = request.headers.get('Authorization', '')
    if auth_header.lower().startswith('bearer '):
        return auth_header[7:].strip()
    request_json = request.get_json(silent=True) or {}
    if isinstance(request_json, dict) and request_json.get('token'):
        return str(request_json['token'])
    if request.form.get('token'):
        return request.form.get('token')
    return request.args.get('token')


def _should_run_full_scan(request_json):
    if isinstance(request_json, dict):
        mode = request_json.get('mode')
        if isinstance(mode, str) and mode.lower() in {'full', 'complete', 'all'}:
            return True
        if str(request_json.get('full', '')).lower() in {'1', 'true', 'yes'}:
            return True
    for source in (request.form, request.args):
        if source.get('mode', '').lower() in {'full', 'complete', 'all'}:
            return True
        if source.get('full', '').lower() in {'1', 'true', 'yes'}:
            return True
    return False


@app.route(basedir + 'api/admin/scan', methods=['POST'])
def route_admin_scan():
    token = _get_scan_token()
    if ADMIN_SCAN_TOKEN and token != ADMIN_SCAN_TOKEN:
        app.logger.warning('Unauthorized scan attempt')
        return abort(403)

    payload = request.get_json(silent=True) or {}
    summary = perform_song_scan(full=_should_run_full_scan(payload))
    return jsonify({'status': 'ok', 'summary': summary})

@app.route(basedir + 'api/config')
@app.cache.cached(timeout=15)
def route_api_config():
    config = get_config(credentials=True)
    return jsonify(config)


@app.route(basedir + 'api/register', methods=['POST'])
def route_api_register():
    data = request.get_json()
    if not schema.validate(data, schema.register):
        return abort(400)

    if session.get('username'):
        session.clear()

    username = data.get('username', '')
    if len(username) < 3 or len(username) > 20 or not re.match('^[a-zA-Z0-9_]{3,20}$', username):
        return api_error('invalid_username')

    if db.users.find_one({'username_lower': username.lower()}):
        return api_error('username_in_use')

    password = data.get('password', '').encode('utf-8')
    if not 6 <= len(password) <= 5000:
        return api_error('invalid_password')

    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password, salt)
    don = get_default_don()
    
    session_id = os.urandom(24).hex()
    db.users.insert_one({
        'username': username,
        'username_lower': username.lower(),
        'password': hashed,
        'display_name': username,
        'don': don,
        'user_level': 1,
        'session_id': session_id
    })

    session['session_id'] = session_id
    session['username'] = username
    session.permanent = True
    return jsonify({'status': 'ok', 'username': username, 'display_name': username, 'don': don})


@app.route(basedir + 'api/login', methods=['POST'])
def route_api_login():
    data = request.get_json()
    if not schema.validate(data, schema.login):
        return abort(400)

    if session.get('username'):
        session.clear()

    username = data.get('username', '')
    result = db.users.find_one({'username_lower': username.lower()})
    if not result:
        return api_error('invalid_username_password')

    password = data.get('password', '').encode('utf-8')
    if not bcrypt.checkpw(password, result['password']):
        return api_error('invalid_username_password')
    
    don = get_db_don(result)
    
    session['session_id'] = result['session_id']
    session['username'] = result['username']
    session.permanent = True if data.get('remember') else False

    return jsonify({'status': 'ok', 'username': result['username'], 'display_name': result['display_name'], 'don': don})


@app.route(basedir + 'api/logout', methods=['POST'])
@login_required
def route_api_logout():
    session.clear()
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/account/display_name', methods=['POST'])
@login_required
def route_api_account_display_name():
    data = request.get_json()
    if not schema.validate(data, schema.update_display_name):
        return abort(400)

    display_name = data.get('display_name', '').strip()
    if not display_name:
        display_name = session.get('username')
    elif len(display_name) > 25:
        return api_error('invalid_display_name')
    
    db.users.update_one({'username': session.get('username')}, {
        '$set': {'display_name': display_name}
    })

    return jsonify({'status': 'ok', 'display_name': display_name})


@app.route(basedir + 'api/account/don', methods=['POST'])
@login_required
def route_api_account_don():
    data = request.get_json()
    if not schema.validate(data, schema.update_don):
        return abort(400)
    
    don_body_fill = data.get('body_fill', '').strip()
    don_face_fill = data.get('face_fill', '').strip()
    if len(don_body_fill) != 7 or\
        not don_body_fill.startswith("#")\
        or not is_hex(don_body_fill[1:])\
        or len(don_face_fill) != 7\
        or not don_face_fill.startswith("#")\
        or not is_hex(don_face_fill[1:]):
        return api_error('invalid_don')
    
    db.users.update_one({'username': session.get('username')}, {'$set': {
        'don_body_fill': don_body_fill,
        'don_face_fill': don_face_fill,
    }})
    
    return jsonify({'status': 'ok', 'don': {'body_fill': don_body_fill, 'face_fill': don_face_fill}})


@app.route(basedir + 'api/account/password', methods=['POST'])
@login_required
def route_api_account_password():
    data = request.get_json()
    if not schema.validate(data, schema.update_password):
        return abort(400)

    user = db.users.find_one({'username': session.get('username')})
    current_password = data.get('current_password', '').encode('utf-8')
    if not bcrypt.checkpw(current_password, user['password']):
        return api_error('current_password_invalid')
    
    new_password = data.get('new_password', '').encode('utf-8')
    if not 6 <= len(new_password) <= 5000:
        return api_error('invalid_new_password')
    
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(new_password, salt)
    session_id = os.urandom(24).hex()

    db.users.update_one({'username': session.get('username')}, {
        '$set': {'password': hashed, 'session_id': session_id}
    })

    session['session_id'] = session_id
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/account/remove', methods=['POST'])
@login_required
def route_api_account_remove():
    data = request.get_json()
    if not schema.validate(data, schema.delete_account):
        return abort(400)

    user = db.users.find_one({'username': session.get('username')})
    password = data.get('password', '').encode('utf-8')
    if not bcrypt.checkpw(password, user['password']):
        return api_error('verify_password_invalid')

    db.scores.delete_many({'username': session.get('username')})
    db.users.delete_one({'username': session.get('username')})

    session.clear()
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/scores/save', methods=['POST'])
@login_required
def route_api_scores_save():
    data = request.get_json()
    if not schema.validate(data, schema.scores_save):
        return abort(400)

    username = session.get('username')
    if data.get('is_import'):
        db.scores.delete_many({'username': username})

    scores = data.get('scores', [])
    for score in scores:
        db.scores.update_one({'username': username, 'hash': score['hash']},
        {'$set': {
            'username': username,
            'hash': score['hash'],
            'score': score['score']
        }}, upsert=True)

    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/scores/get')
@login_required
def route_api_scores_get():
    username = session.get('username')

    scores = []
    for score in db.scores.find({'username': username}):
        scores.append({
            'hash': score['hash'],
            'score': score['score']
        })

    user = db.users.find_one({'username': username})
    don = get_db_don(user)
    return jsonify({'status': 'ok', 'scores': scores, 'username': user['username'], 'display_name': user['display_name'], 'don': don})


@app.route(basedir + 'privacy')
def route_api_privacy():
    last_modified = time.strftime('%d %B %Y', time.gmtime(os.path.getmtime('templates/privacy.txt')))
    integration = take_config('GOOGLE_CREDENTIALS')['gdrive_enabled'] if take_config('GOOGLE_CREDENTIALS') else False
    
    response = make_response(render_template('privacy.txt', last_modified=last_modified, config=get_config(), integration=integration))
    response.headers['Content-type'] = 'text/plain; charset=utf-8'
    return response


def make_preview(song_id, song_type, song_ext, preview):
    song_path = 'public/songs/%s/main.%s' % (song_id, song_ext)
    prev_path = 'public/songs/%s/preview.mp3' % song_id

    if os.path.isfile(song_path) and not os.path.isfile(prev_path):
        if not preview or preview <= 0:
            print('Skipping #%s due to no preview' % song_id)
            return False

        print('Making preview.mp3 for song #%s' % song_id)
        ff = FFmpeg(inputs={song_path: '-ss %s' % preview},
                    outputs={prev_path: '-codec:a libmp3lame -ar 32000 -b:a 92k -y -loglevel panic'})
        ff.run()

    return prev_path

error_pages = take_config('ERROR_PAGES') or {}

def create_error_page(code, url):
    if url.startswith("http://") or url.startswith("https://"):
        resp = requests.get(url)
        if resp.status_code == 200:
            app.register_error_handler(code, lambda e: (resp.content, code))
    else:
        if url.startswith(basedir):
            url = url[len(basedir):]
        path = os.path.normpath(os.path.join("public", url))
        if os.path.isfile(path):
            app.register_error_handler(code, lambda e: (send_from_directory(".", path), code))

for code in error_pages:
    if error_pages[code]:
        create_error_page(code, error_pages[code])

def cache_wrap(res_from, secs):
    res = flask.make_response(res_from)

    if os.environ.get("FLASK_ENV") == "production":
        res.headers["Cache-Control"] = f"public, max-age={secs}, s-maxage={secs}"
        res.headers["CDN-Cache-Control"] = f"max-age={secs}"
    else:
        res.headers["Cache-Control"] = "no-cache"

    return res

@app.route(basedir + "src/<path:ref>")
def send_src(ref):
    return cache_wrap(flask.send_from_directory("public/src", ref), 3600)

@app.route(basedir + "assets/<path:ref>")
def send_assets(ref):
    return cache_wrap(flask.send_from_directory("public/assets", ref), 3600)

@app.route(basedir + "songs/<path:ref>")
def send_songs(ref):
    return cache_wrap(flask.send_from_directory(str(SONGS_DIR_PATH), ref), 604800)

@app.route(basedir + "manifest.json")
def send_manifest():
    return cache_wrap(flask.send_from_directory("public", "manifest.json"), 3600)

if SCAN_ON_START:
    try:
        perform_song_scan()
    except Exception:
        app.logger.exception('Automatic song scan failed')


def _start_song_directory_watcher():
    global _song_watcher_handle
    if _song_watcher_handle is not None:
        return
    if not ENABLE_SONG_WATCHER:
        app.logger.info('Song directory watcher disabled')
        return
    if not song_scanner.watchdog_supported:
        app.logger.info('watchdog not available; live song updates disabled')
        return
    if not SONGS_DIR_PATH.exists():
        app.logger.warning('Songs directory %s missing; live song updates disabled', SONGS_DIR_PATH)
        return

    def _run_scan():
        with app.app_context():
            try:
                perform_song_scan(full=False)
            except Exception:
                app.logger.exception('Live song scan failed')

    try:
        handle = song_scanner.start_watcher(callback=_run_scan, debounce_seconds=0.75)
        if handle:
            app.logger.info('Song directory watcher started')
            _song_watcher_handle = handle
    except KeyboardInterrupt:
        raise
    except SystemExit as exc:
        app.logger.error('Failed to start song directory watcher (exiting): %s', exc, exc_info=True)
    except Exception:
        app.logger.exception('Failed to start song directory watcher')


# Flask 3 removed the ``before_serving`` decorator. Provide a compatible fallback
# that runs the hook before the first request is processed so that the song
# directory watcher still starts automatically.
if hasattr(app, "before_serving"):
    _song_watcher_hook = app.before_serving
else:
    def _song_watcher_hook(func):
        has_run = False
        lock = threading.Lock()

        @wraps(func)
        def _run_once():
            nonlocal has_run
            if has_run:
                return
            with lock:
                if has_run:
                    return
                has_run = True
            func()

        if hasattr(app, "before_first_request"):
            app.before_first_request(_run_once)
        else:
            app.before_request(_run_once)
        return func


@_song_watcher_hook
def _ensure_song_directory_watcher_started():
    _start_song_directory_watcher()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run the taiko-web development server.')
    parser.add_argument('port', type=int, metavar='PORT', nargs='?', default=34801, help='Port to listen on.')
    parser.add_argument('-b', '--bind-address', default='localhost', help='Bind server to address.')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode.')
    args = parser.parse_args()

    app.run(host=args.bind_address, port=args.port, debug=args.debug)

