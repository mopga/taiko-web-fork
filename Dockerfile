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
RUN mkdir -p /data/songs /data/assets /app/config

# По умолчанию gunicorn слушает 0.0.0.0:8000
ENV GUNICORN_CMD_ARGS="--bind 0.0.0.0:8000 --workers 2 --threads 4"
EXPOSE 8000

# Запуск — gunicorn wsgi:app (если в проекте точка входа app.py с app=Flask(...), то wsgi может называться иначе)
# В форке yuuki/taiko-web Flask-приложение доступно как app в app.py — значит модуль 'app:app'
CMD ["gunicorn", "app:app"]
