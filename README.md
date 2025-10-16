# TaikoWeb

Это улучшенная версия TaikoWeb.

## Начать отладку

Установить зависимости

```bash
pip install -r requirements.txt
```

Запустить базу данных

```bash
docker run --detach \
  --name taiko-web-mongo-debug \
  --volume taiko-web-mongo-debug:/data/db \
  --publish 27017:27017 \
  mongo
```

```bash
docker run --detach \
  --name taiko-web-redis-debug \
  --volume taiko-web-redis-debug:/data \
  --publish 6379:6379 \
  redis
```

Запустить сервер

```bash
flask run --host 0.0.0.0
```
