from __future__ import annotations
import struct
from .BinaryInfo import BinaryInfo
from .constants import FLAG_BYTE, FLAG_EVENT, FLAG_EXTENDED, FLAG_FIELD, FLAG_GLOBAL, WEB_EVENTS, WEB_GLOBALS


def _get_header_bit_length(key: int) -> int:
    return 2 if key < 256 else 3


def encode_wam(binary_info: BinaryInfo) -> bytes:
    binary_info.buffer = []
    _encode_wam_header(binary_info)
    _encode_events(binary_info)
    return b''.join(binary_info.buffer)


def _encode_wam_header(bi: BinaryInfo):
    header = bytearray(8)
    header[0:3] = b'WAM'
    header[3] = bi.protocolVersion
    header[4] = 1  # random flag
    header[5:7] = struct.pack('>H', bi.sequence)
    header[7] = 0  # regular channel
    bi.buffer.append(bytes(header))


def _encode_global_attributes(bi: BinaryInfo, globals_: dict):
    for key, value in globals_.items():
        id_ = next(a for a in WEB_GLOBALS if a and a['name'] == key)['id']
        if isinstance(value, bool):
            value = 1 if value else 0
        bi.buffer.append(_serialize_data(id_, value, FLAG_GLOBAL))


def _encode_events(bi: BinaryInfo):
    for ev in bi.events:
        name, data = next(iter(ev.items()))
        props, globals_ = data['props'], data['globals']
        _encode_global_attributes(bi, globals_)
        event = next(a for a in WEB_EVENTS if a['name'] == name)
        props_ = list(props.items())
        extended = False
        for _, value in props_:
            extended = extended or value is not None
        event_flag = FLAG_EVENT if extended else (FLAG_EVENT | FLAG_EXTENDED)
        bi.buffer.append(_serialize_data(event['id'], -event['weight'], event_flag))
        for i, (key, value) in enumerate(props_):
            id_ = event['props'][key][0]
            extended = i < len(props_) - 1
            if isinstance(value, bool):
                value = 1 if value else 0
            field_flag = FLAG_EVENT if extended else (FLAG_FIELD | FLAG_EXTENDED)
            bi.buffer.append(_serialize_data(id_, value, field_flag))


def _serialize_header(key: int, flag: int) -> bytes:
    if key < 256:
        return bytes([flag, key])
    return bytes([flag | FLAG_BYTE]) + struct.pack('<H', key)


def _serialize_data(key: int, value, flag: int) -> bytes:
    if value is None:
        if flag == FLAG_GLOBAL:
            return _serialize_header(key, flag)
    elif isinstance(value, int):
        if value in (0, 1):
            return _serialize_header(key, flag | ((value + 1) << 4))
        if -128 <= value < 128:
            return _serialize_header(key, flag | (3 << 4)) + struct.pack('<b', value)
        if -32768 <= value < 32768:
            return _serialize_header(key, flag | (4 << 4)) + struct.pack('<h', value)
        if -2147483648 <= value < 2147483648:
            return _serialize_header(key, flag | (5 << 4)) + struct.pack('<i', value)
        return _serialize_header(key, flag | (7 << 4)) + struct.pack('<d', float(value))
    elif isinstance(value, float):
        return _serialize_header(key, flag | (7 << 4)) + struct.pack('<d', value)
    elif isinstance(value, str):
        b = value.encode('utf-8')
        n = len(b)
        if n < 256:
            return _serialize_header(key, flag | (8 << 4)) + bytes([n]) + b
        if n < 65536:
            return _serialize_header(key, flag | (9 << 4)) + struct.pack('<H', n) + b
        return _serialize_header(key, flag | (10 << 4)) + struct.pack('<I', n) + b
    raise ValueError('missing')
