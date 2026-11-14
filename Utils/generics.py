from __future__ import annotations
import asyncio
import base64
import hashlib
import json
import os
import re
import time
from typing import Any, Callable, Optional

from ..WAProto import WAProto_pb2 as proto
from ..Types import DisconnectReason
from ..WABinary import (Boom, get_all_binary_node_children, jid_decode)
from .crypto import sha256

baileys_version = [2, 3000, 1043857760]


class _BufferJSON:
    """JSON (de)serialisation of bytes values, compatible with Baileys' BufferJSON."""

    @staticmethod
    def replacer(value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {'type': 'Buffer', 'data': base64.b64encode(bytes(value)).decode()}
        raise TypeError(f'not serializable: {type(value)}')

    @staticmethod
    def reviver(value: dict):
        if value.get('type') == 'Buffer' and isinstance(value.get('data'), str):
            return base64.b64decode(value['data'])
        return value

    @staticmethod
    def dumps(obj) -> str:
        return json.dumps(obj, default=_BufferJSON.replacer)

    @staticmethod
    def loads(s: str):
        return json.loads(s, object_hook=_BufferJSON.reviver)


BufferJSON = _BufferJSON


def get_key_author(key, me_id: str = 'me') -> str:
    if key is None:
        return ''
    g = (lambda k: key.get(k)) if isinstance(key, dict) else (lambda k: getattr(key, k, None) or None)
    if g('fromMe'):
        return me_id
    return g('participantAlt') or g('remoteJidAlt') or g('participant') or g('remoteJid') or ''


def is_string_null_or_empty(value) -> bool:
    return value is None or value == ''


def write_random_pad_max16(msg: bytes) -> bytes:
    pad_length = (os.urandom(1)[0] & 0x0F) + 1
    return bytes(msg) + bytes([pad_length]) * pad_length


def unpad_random_max16(e: bytes) -> bytes:
    t = bytes(e)
    if len(t) == 0:
        raise ValueError('unpadPkcs7 given empty bytes')
    r = t[-1]
    if r > len(t):
        raise ValueError(f'unpad given {len(t)} bytes, but pad is {r}')
    return t[:len(t) - r]


def generate_participant_hash_v2(participants: list[str]) -> str:
    participants.sort()
    h = base64.b64encode(sha256(''.join(participants).encode())).decode()
    return '2:' + h[:6]


def encode_wa_message(message) -> bytes:
    from .proto_utils import from_object
    if isinstance(message, dict):
        message = from_object(proto.Message, message)
    return write_random_pad_max16(message.SerializeToString())


def generate_registration_id() -> int:
    return int.from_bytes(os.urandom(2), 'big') & 16383


def encode_big_endian(e: int, t: int = 4) -> bytes:
    a = bytearray(t)
    r = e
    for i in range(t - 1, -1, -1):
        a[i] = 255 & r
        r >>= 8
    return bytes(a)


def to_number(t) -> int:
    return int(t or 0)


def unix_timestamp_seconds(date: Optional[float] = None) -> int:
    """unix timestamp (seconds) of `date` (a datetime or epoch seconds); default now"""
    if date is None:
        return int(time.time())
    if hasattr(date, 'timestamp'):
        return int(date.timestamp())
    return int(date)


class DebouncedTimeout:
    def __init__(self, interval_ms: float = 1000, task: Optional[Callable] = None):
        self.interval_ms = interval_ms
        self.task = task
        self._handle: Optional[asyncio.TimerHandle] = None

    def start(self, new_interval_ms: Optional[float] = None, new_task: Optional[Callable] = None):
        self.task = new_task or self.task
        self.interval_ms = new_interval_ms or self.interval_ms
        self.cancel()
        loop = asyncio.get_running_loop()
        self._handle = loop.call_later(self.interval_ms / 1000, self._fire)

    def _fire(self):
        if self.task:
            r = self.task()
            if asyncio.iscoroutine(r):
                asyncio.ensure_future(r)

    def cancel(self):
        if self._handle:
            self._handle.cancel()
            self._handle = None

    def set_task(self, task):
        self.task = task

    def set_interval(self, ms):
        self.interval_ms = ms


def debounced_timeout(interval_ms: float = 1000, task: Optional[Callable] = None) -> DebouncedTimeout:
    return DebouncedTimeout(interval_ms, task)


async def delay(ms: float):
    await asyncio.sleep(ms / 1000)


def delay_cancellable(ms: float):
    """returns (awaitable, cancel) — cancel raises Boom('Cancelled') in the awaiter"""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    handle = loop.call_later(ms / 1000, lambda: fut.done() or fut.set_result(None))

    def cancel():
        handle.cancel()
        if not fut.done():
            fut.set_exception(Boom('Cancelled', status_code=500))
    return fut, cancel


async def promise_timeout(ms: Optional[float], promise: Callable):
    """promise: (resolve, reject) -> None. Resolves with the value, or raises Boom('Timed Out')."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()

    def resolve(v=None):
        if not fut.done():
            fut.set_result(v)

    def reject(e):
        if not fut.done():
            fut.set_exception(e if isinstance(e, BaseException) else Exception(str(e)))
    handle = None
    if ms:
        handle = loop.call_later(ms / 1000, lambda: reject(Boom('Timed Out', status_code=DisconnectReason.timedOut)))
    try:
        r = promise(resolve, reject)
        if asyncio.iscoroutine(r):
            asyncio.ensure_future(r).add_done_callback(
                lambda t: t.exception() and reject(t.exception()) if not t.cancelled() else None)
        return await fut
    finally:
        if handle:
            handle.cancel()


def generate_message_id_v2(user_id: Optional[str] = None) -> str:
    data = bytearray(8 + 20 + 16)
    data[0:8] = int(time.time()).to_bytes(8, 'big')
    if user_id:
        ident = jid_decode(user_id)
        if ident and ident.user:
            u = ident.user.encode()
            data[8:8 + len(u)] = u
            data[8 + len(u):8 + len(u) + 5] = b'@c.us'
    data[28:44] = os.urandom(16)
    return '3EB0' + hashlib.sha256(bytes(data)).hexdigest().upper()[:18]


def generate_message_id() -> str:
    return '3EB0' + os.urandom(18).hex().upper()


def bind_wait_for_event(ev, event: str):
    async def wait_for(check: Callable, timeout_ms: Optional[float] = None):
        listeners = {}

        def setup(resolve, reject):
            def close_listener(update):
                if update.get('connection') == 'close':
                    reject((update.get('lastDisconnect') or {}).get('error') or
                           Boom('Connection Closed', status_code=DisconnectReason.connectionClosed))

            async def listener(update):
                r = check(update)
                if asyncio.iscoroutine(r):
                    r = await r
                if r:
                    resolve(None)
            listeners['close'] = close_listener
            listeners['main'] = listener
            ev.on('connection.update', close_listener)
            ev.on(event, listener)
        try:
            await promise_timeout(timeout_ms, setup)
        finally:
            ev.off(event, listeners.get('main'))
            ev.off('connection.update', listeners.get('close'))
    return wait_for


def bind_wait_for_connection_update(ev):
    return bind_wait_for_event(ev, 'connection.update')


async def fetch_latest_baileys_version(options: Optional[dict] = None) -> dict:
    import httpx
    url = 'https://raw.githubusercontent.com/WhiskeySockets/Baileys/master/src/Defaults/index.ts'
    try:
        async with httpx.AsyncClient(**(options or {})) as c:
            r = await c.get(url)
        if r.status_code >= 400:
            raise Boom(f'Failed to fetch latest Baileys version: {r.reason_phrase}', status_code=r.status_code)
        line = r.text.split('\n')[6]
        m = re.search(r'const version = \[(\d+),\s*(\d+),\s*(\d+)\]', line)
        if m:
            return {'version': [int(m.group(1)), int(m.group(2)), int(m.group(3))], 'isLatest': True}
        raise ValueError('Could not parse version from Defaults/index.ts')
    except Exception as e:
        return {'version': baileys_version, 'isLatest': False, 'error': e}


async def fetch_latest_wa_web_version(options: Optional[dict] = None) -> dict:
    import httpx
    try:
        headers = {'sec-fetch-site': 'none',
                   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/131.0.0.0 Safari/537.36'}
        headers.update((options or {}).get('headers', {}))
        async with httpx.AsyncClient(follow_redirects=True) as c:
            r = await c.get('https://web.whatsapp.com/sw.js', headers=headers)
        if r.status_code >= 400:
            raise Boom(f'Failed to fetch sw.js: {r.reason_phrase}', status_code=r.status_code)
        m = re.search(r'\\?"client_revision\\?":\s*(\d+)', r.text)
        if not m:
            return {'version': baileys_version, 'isLatest': False,
                    'error': {'message': 'Could not find client revision in the fetched content'}}
        return {'version': [2, 3000, int(m.group(1))], 'isLatest': True}
    except Exception as e:
        return {'version': baileys_version, 'isLatest': False, 'error': e}


def generate_md_tag_prefix() -> str:
    b = os.urandom(4)
    return f'{int.from_bytes(b[:2], "big")}.{int.from_bytes(b[2:], "big")}-'


_STATUS_MAP = {
    'sender': proto.WebMessageInfo.SERVER_ACK,
    'played': proto.WebMessageInfo.PLAYED,
    'read': proto.WebMessageInfo.READ,
    'read-self': proto.WebMessageInfo.READ,
}


def get_status_from_receipt_type(type_: Optional[str]):
    if type_ is None:
        return proto.WebMessageInfo.DELIVERY_ACK
    return _STATUS_MAP.get(type_)


_CODE_MAP = {'conflict': DisconnectReason.connectionReplaced}


def get_error_code_from_stream_error(node: dict) -> dict:
    children = get_all_binary_node_children(node)
    reason = children[0]['tag'] if children else 'unknown'
    status_code = int(node['attrs'].get('code') or _CODE_MAP.get(reason) or DisconnectReason.badSession)
    if status_code == DisconnectReason.restartRequired:
        reason = 'restart required'
    return {'reason': reason, 'statusCode': status_code}


def get_call_status_from_node(node: dict) -> str:
    tag, attrs = node['tag'], node['attrs']
    if tag in ('offer', 'offer_notice'):
        return 'offer'
    if tag == 'terminate':
        return 'timeout' if attrs.get('reason') == 'timeout' else 'terminate'
    if tag in ('preaccept', 'transport', 'relaylatency', 'reject', 'accept'):
        return tag
    return 'ringing'


_UNEXPECTED = 'Unexpected server response: '


def get_code_from_ws_error(error: BaseException) -> int:
    status_code = 500
    msg = str(error)
    if _UNEXPECTED in msg:
        try:
            code = int(msg.split(_UNEXPECTED, 1)[1].strip().split()[0])
            if code >= 400:
                status_code = code
        except ValueError:
            pass
    elif isinstance(error, (TimeoutError, asyncio.TimeoutError, OSError)) or 'timed out' in msg:
        status_code = 408
    return status_code


def is_wa_business_platform(platform: str) -> bool:
    return platform in ('smbi', 'smba')


def trim_undefined(obj: dict) -> dict:
    for k in [k for k, v in obj.items() if v is None]:
        del obj[k]
    return obj


_CROCKFORD = '123456789ABCDEFGHJKLMNPQRSTVWXYZ'


def bytes_to_crockford(buffer: bytes) -> str:
    value = 0
    bit_count = 0
    out = []
    for b in bytes(buffer):
        value = ((value << 8) | (b & 0xFF)) & 0xFFFFFFFF
        bit_count += 8
        while bit_count >= 5:
            out.append(_CROCKFORD[(value >> (bit_count - 5)) & 31])
            bit_count -= 5
    if bit_count > 0:
        out.append(_CROCKFORD[(value << (5 - bit_count)) & 31])
    return ''.join(out)


def encode_newsletter_message(message) -> bytes:
    from .proto_utils import from_object
    if isinstance(message, dict):
        message = from_object(proto.Message, message)
    return message.SerializeToString()
