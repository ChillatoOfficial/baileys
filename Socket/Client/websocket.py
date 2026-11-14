from __future__ import annotations
import asyncio
from typing import Optional, Union

import websockets
from websockets.asyncio.client import connect as ws_connect

from ...Defaults import DEFAULT_ORIGIN
from ...Utils.event_emitter import EventEmitter


class AbstractSocketClient(EventEmitter):
    def __init__(self, url: str, config: dict):
        super().__init__()
        self.url = url
        self.config = config

    is_open = property(lambda self: False)
    is_closed = property(lambda self: True)
    is_closing = property(lambda self: False)
    is_connecting = property(lambda self: False)

    def connect(self): raise NotImplementedError
    async def close(self): raise NotImplementedError
    def send(self, data, cb=None) -> bool: raise NotImplementedError


class WebSocketClient(AbstractSocketClient):
    """WebSocket transport (asyncio `websockets`). Emits: open, message(bytes), close(code, reason), error(exc)."""

    def __init__(self, url: str, config: dict):
        super().__init__(url, config)
        self.socket = None
        self._task: Optional[asyncio.Task] = None
        self._connecting = False
        self._closing = False

    @property
    def is_open(self) -> bool:
        return self.socket is not None and not self._closing and self.socket.state.name == 'OPEN'

    @property
    def is_closed(self) -> bool:
        return self.socket is None and not self._connecting

    @property
    def is_closing(self) -> bool:
        return self._closing

    @property
    def is_connecting(self) -> bool:
        return self._connecting

    def connect(self):
        if self.socket or self._connecting:
            return
        self._connecting = True
        self._task = asyncio.ensure_future(self._run())

    async def _run(self):
        headers = dict((self.config.get('options') or {}).get('headers') or {})
        try:
            timeout = (self.config.get('connectTimeoutMs') or 20000) / 1000
            kwargs = dict(origin=DEFAULT_ORIGIN, additional_headers=headers or None, open_timeout=timeout,
                          max_size=None, ping_interval=None, close_timeout=5)
            proxy = self.config.get('agent')
            if proxy is not None:
                kwargs['proxy'] = proxy
            async with ws_connect(str(self.url), **kwargs) as ws:
                self.socket = ws
                self._connecting = False
                self.emit('open')
                try:
                    async for message in ws:
                        self.emit('message', message if isinstance(message, bytes) else message.encode())
                except websockets.ConnectionClosed:
                    pass
                code = ws.close_code or 1006
                reason = ws.close_reason or ''
        except Exception as err:
            self._connecting = False
            self.socket = None
            self._closing = False
            self.emit('error', err)
            self.emit('close', 1006, str(err))
            return
        self.socket = None
        self._closing = False
        self.emit('close', code, reason)

    async def close(self):
        if not self.socket:
            if self._task and not self._task.done():
                self._task.cancel()
            return
        self._closing = True
        try:
            await self.socket.close()
        except Exception:
            pass
        if self._task:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=5)
            except Exception:
                pass
        self.socket = None

    def send(self, data: Union[bytes, str], cb=None) -> bool:
        ws = self.socket
        if ws is None:
            return False

        async def _send():
            try:
                await ws.send(data)
                if cb:
                    cb(None)
            except Exception as e:
                if cb:
                    cb(e)
                else:
                    self.emit('error', e)
        asyncio.ensure_future(_send())
        return True
