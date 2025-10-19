#!/bin/sh
set -eu

# на всякий случай нормализуем EOL на рантайме (если кто-то смонтировал CRLF)
# busybox/gnu sed оба понимают 's/\r$//'
tmp="/tmp/start.sh"
sed 's/\r$//' /app/start.sh > "$tmp"
chmod +x "$tmp"

# передаём управление (exec, чтобы корректно ловить SIGTERM/SIGINT)
exec "$tmp" "$@"
