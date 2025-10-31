from __future__ import annotations

import contextlib
import logging
from pathlib import Path

import websockets
from starlette.applications import Starlette
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.routing import Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from server import connection as legacy_connection


LOGGER = logging.getLogger("taiko.desktop.asgi")


class _StarletteWebSocketAdapter:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        client = websocket.client
        if client is not None:
            self.remote_address = (client.host, client.port)
        else:
            self.remote_address = None
        self.request_headers = websocket.headers
        self._closed = False

    async def accept(self) -> None:
        await self._websocket.accept()

    async def send(self, data: str) -> None:
        await self._websocket.send_text(data)

    async def recv(self) -> str:
        try:
            return await self._websocket.receive_text()
        except WebSocketDisconnect as exc:
            raise websockets.exceptions.ConnectionClosedOK(exc.code or 1000, 'disconnect') from exc

    async def ping(self):
        class _ImmediatePong:
            def __await__(self):
                if False:
                    yield None
                return None

        return _ImmediatePong()

    async def close(self, code: int = 1000) -> None:
        if self._closed:
            return
        self._closed = True
        await self._websocket.close(code=code)


class _DesktopWebSocketBridge:
    async def handle(self, websocket: WebSocket) -> None:
        adapter = _StarletteWebSocketAdapter(websocket)
        await adapter.accept()
        try:
            await legacy_connection(adapter, websocket.url.path)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Desktop websocket endpoint failed")
            with contextlib.suppress(Exception):
                await adapter.close(code=1011)
        finally:
            with contextlib.suppress(Exception):
                await adapter.close()


def create_desktop_asgi_app(flask_app, *, static_dir: Path) -> Starlette:
    bridge = _DesktopWebSocketBridge()

    async def desktop_multiplayer(websocket: WebSocket) -> None:
        await bridge.handle(websocket)

    static_directory = Path(static_dir)
    routes = [
        WebSocketRoute("/p2", desktop_multiplayer),
        Mount(
            "/static",
            app=StaticFiles(directory=str(static_directory), check_dir=False),
            name="static",
        ),
        Mount("/", app=WSGIMiddleware(flask_app)),
    ]
    return Starlette(routes=routes)
