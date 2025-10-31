from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

import websockets

try:  # pragma: no cover - starlette may be optional in some environments
    from starlette.applications import Starlette
    from starlette.middleware.wsgi import WSGIMiddleware as StarletteWSGIMiddleware
    from starlette.routing import Mount, WebSocketRoute
    from starlette.staticfiles import StaticFiles
    from starlette.websockets import WebSocket, WebSocketDisconnect
except ImportError:  # pragma: no cover - desktop fallback without starlette
    Starlette = None  # type: ignore[assignment]
    StarletteWSGIMiddleware = None  # type: ignore[assignment]
    Mount = None  # type: ignore[assignment]
    WebSocketRoute = None  # type: ignore[assignment]
    StaticFiles = None  # type: ignore[assignment]

    class WebSocket:  # type: ignore[override]
        """Sentinel stub used only when Starlette is unavailable."""

    class WebSocketDisconnect(Exception):  # pragma: no cover - stub for typing
        def __init__(self, code: Optional[int] = None) -> None:
            super().__init__("websocket disconnect")
            self.code = code


try:
    from server import connection as legacy_connection
except ImportError:  # pragma: no cover - optional legacy dependency
    legacy_connection = None


LOGGER = logging.getLogger("taiko.desktop.asgi")


class _ImmediatePong:
    def __await__(self):
        if False:  # pragma: no cover - ensures generator nature
            yield None
        return None


class _StarletteWebSocketAdapter:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        client = getattr(websocket, "client", None)
        if client is not None:
            self.remote_address = (getattr(client, "host", None), getattr(client, "port", None))
        else:
            self.remote_address = None
        self.request_headers = getattr(websocket, "headers", {})
        self._closed = False

    async def accept(self) -> None:
        await self._websocket.accept()

    async def send(self, data: str) -> None:
        await self._websocket.send_text(data)

    async def recv(self) -> str:
        try:
            return await self._websocket.receive_text()
        except WebSocketDisconnect as exc:
            raise websockets.exceptions.ConnectionClosedOK(exc.code or 1000, "disconnect") from exc

    async def ping(self):
        return _ImmediatePong()

    async def close(self, code: int = 1000) -> None:
        if self._closed:
            return
        self._closed = True
        await self._websocket.close(code=code)


class _ASGIWebSocketAdapter:
    def __init__(self, scope: Dict[str, Any], receive: Callable[[], Awaitable[Dict[str, Any]]], send: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self._scope = scope
        self._receive = receive
        self._send = send
        self._closed = False
        client = scope.get("client")
        if isinstance(client, (list, tuple)) and len(client) >= 2:
            self.remote_address = (client[0], client[1])
        else:
            self.remote_address = None
        headers = scope.get("headers") or []
        try:
            from websockets.datastructures import Headers as WSHeaders
        except Exception:  # pragma: no cover - fallback when websockets changes
            header_map: Dict[str, str] = {}
            for name_bytes, value_bytes in headers:
                name = name_bytes.decode("latin1") if isinstance(name_bytes, (bytes, bytearray)) else str(name_bytes)
                value = value_bytes.decode("latin1") if isinstance(value_bytes, (bytes, bytearray)) else str(value_bytes)
                header_map[name] = value
            self.request_headers = header_map
        else:
            pairs: Iterable[tuple[str, str]] = (
                (
                    name_bytes.decode("latin1") if isinstance(name_bytes, (bytes, bytearray)) else str(name_bytes),
                    value_bytes.decode("latin1") if isinstance(value_bytes, (bytes, bytearray)) else str(value_bytes),
                )
                for name_bytes, value_bytes in headers
            )
            self.request_headers = WSHeaders(pairs)

    async def accept(self) -> None:
        if self._closed:
            return
        await self._send({"type": "websocket.accept"})

    async def send(self, data: str) -> None:
        await self._send({"type": "websocket.send", "text": data})

    async def recv(self) -> str:
        while True:
            message = await self._receive()
            message_type = message.get("type")
            if message_type == "websocket.receive":
                text = message.get("text")
                if text is not None:
                    return text
            elif message_type == "websocket.disconnect":
                code = message.get("code") or 1000
                raise websockets.exceptions.ConnectionClosedOK(code, "disconnect")

    async def ping(self):
        return _ImmediatePong()

    async def close(self, code: int = 1000) -> None:
        if self._closed:
            return
        self._closed = True
        await self._send({"type": "websocket.close", "code": code})


class _DesktopWebSocketBridge:
    async def _dispatch(self, adapter, path: str) -> None:
        await adapter.accept()
        try:
            if legacy_connection is None:
                LOGGER.debug(
                    "desktop.websocket legacy handler unavailable; closing connection gracefully"
                )
                await adapter.close(code=1011)
                return
            await legacy_connection(adapter, path)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Desktop websocket endpoint failed")
            with contextlib.suppress(Exception):
                await adapter.close(code=1011)
        finally:
            with contextlib.suppress(Exception):
                await adapter.close()

    async def handle(self, websocket: WebSocket) -> None:
        adapter = _StarletteWebSocketAdapter(websocket)
        path = getattr(getattr(websocket, "url", None), "path", "")
        await self._dispatch(adapter, path)

    async def handle_asgi(
        self,
        scope: Dict[str, Any],
        receive: Callable[[], Awaitable[Dict[str, Any]]],
        send: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        adapter = _ASGIWebSocketAdapter(scope, receive, send)
        await self._dispatch(adapter, scope.get("path", ""))


def _create_minimal_asgi_app(flask_app, bridge: _DesktopWebSocketBridge) -> Callable[[Dict[str, Any], Callable[[], Awaitable[Dict[str, Any]]], Callable[[Dict[str, Any]], Awaitable[None]]], Awaitable[None]]:
    try:
        from uvicorn.middleware.wsgi import WSGIMiddleware as UvicornWSGIMiddleware
    except ImportError as exc:  # pragma: no cover - runtime requirement
        raise RuntimeError("uvicorn WSGI middleware is required for desktop fallback") from exc

    LOGGER.debug("desktop.asgi using minimal ASGI fallback")
    wsgi_app = UvicornWSGIMiddleware(flask_app)

    async def app(scope: Dict[str, Any], receive: Callable[[], Awaitable[Dict[str, Any]]], send: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            while True:
                message = await receive()
                message_type = message.get("type")
                if message_type == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message_type == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        elif scope_type == "websocket" and scope.get("path") == "/p2":
            await bridge.handle_asgi(scope, receive, send)
            return
        await wsgi_app(scope, receive, send)

    return app


def create_desktop_asgi_app(flask_app, *, static_dir: Path):
    bridge = _DesktopWebSocketBridge()
    if Starlette is None or WebSocketRoute is None or StaticFiles is None:
        return _create_minimal_asgi_app(flask_app, bridge)

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
        Mount("/", app=StarletteWSGIMiddleware(flask_app)),
    ]
    return Starlette(routes=routes)

