# Базовый образ с Python
FROM python:3.13-slim

# ffmpeg для предпросмотров/сэмплов
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg git \
    && rm -rf /var/lib/apt/lists/*

# Аргументы для выбора форка/ветки
ARG TAIKO_REPO_URL="https://github.com/mopga/taiko-web-fork.git"

# Рабочая директория приложения
WORKDIR /app

# Клонируем исходники (форк yuuki/taiko-web)
RUN git clone "${TAIKO_REPO_URL}" /app

# Ставим зависимости Python
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Создаём точки монтирования для песен/ассетов и конфигурации
RUN mkdir -p /data/songs /app/config

# По умолчанию HTTP слушает 0.0.0.0:8000
EXPOSE 8000

COPY start.sh /app/start.sh
COPY entrypoint.py /app/entrypoint.py
RUN python - <<'PY'
from pathlib import Path
path = Path('/app/start.sh')
data = path.read_bytes()
normalized = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
if data != normalized:
    path.write_bytes(normalized)
    print('taiko-start-normalized: path=/app/start.sh removed-carriage-returns')
path.chmod(0o755)
PY

CMD ["python", "/app/entrypoint.py"]
