# Базовый образ с Python
FROM python:3.13-slim

# ffmpeg для предпросмотров/сэмплов
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg git \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория приложения
WORKDIR /app

# Ставим зависимости Python
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Копируем исходники приложения
COPY . /app

# Создаём точки монтирования для песен/ассетов и конфигурации
RUN mkdir -p /data/songs /app/config

# По умолчанию HTTP слушает 0.0.0.0:8000
EXPOSE 8000

RUN chmod 0755 /app/entrypoint.sh /app/start.sh \
 && sed -i 's/\r$//' /app/start.sh

ENTRYPOINT ["/bin/sh","/app/entrypoint.sh"]
