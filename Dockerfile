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
RUN sed -i 's/\r$//' /app/start.sh \
    && chmod +x /app/start.sh

CMD ["/app/start.sh"]
