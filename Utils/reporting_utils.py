from __future__ import annotations
import hashlib
import hmac as _hmac
from typing import Optional
from .crypto import hkdf

# ReportingField: {'f': int, 'm': bool?, 's': [ReportingField]?}
_U = [{'f': 21}, {'f': 22}]
reporting_fields = [
    {'f': 1},
    {'f': 3, 's': [{'f': 2}, {'f': 3}, {'f': 8}, {'f': 11}, {'f': 17, 's': _U}, {'f': 25}]},
    {'f': 4, 's': [{'f': 1}, {'f': 16}, {'f': 17, 's': _U}]},
    {'f': 5, 's': [{'f': 3}, {'f': 4}, {'f': 5}, {'f': 16}, {'f': 17, 's': _U}]},
    {'f': 6, 's': [{'f': 1}, {'f': 17, 's': _U}, {'f': 30}]},
    {'f': 7, 's': [{'f': 2}, {'f': 7}, {'f': 10}, {'f': 17, 's': _U}, {'f': 20}]},
    {'f': 8, 's': [{'f': 2}, {'f': 7}, {'f': 9}, {'f': 17, 's': _U}, {'f': 21}]},
    {'f': 9, 's': [{'f': 2}, {'f': 6}, {'f': 7}, {'f': 13}, {'f': 17, 's': _U}, {'f': 20}]},
    {'f': 12, 's': [{'f': 1}, {'f': 2}, {'f': 14, 'm': True}, {'f': 15}]},
    {'f': 18, 's': [{'f': 6}, {'f': 16}, {'f': 17, 's': _U}]},
    {'f': 26, 's': [{'f': 4}, {'f': 5}, {'f': 8}, {'f': 13}, {'f': 17, 's': _U}]},
    {'f': 28, 's': [{'f': 1}, {'f': 2}, {'f': 4}, {'f': 5}, {'f': 6}, {'f': 7, 's': _U}]},
    {'f': 37, 's': [{'f': 1, 'm': True}]},
    {'f': 49, 's': [{'f': 2}, {'f': 3, 's': [{'f': 1}, {'f': 2}]}, {'f': 5, 's': _U},
                    {'f': 8, 's': [{'f': 1}, {'f': 2}]}]},
    {'f': 53, 's': [{'f': 1, 'm': True}]},
    {'f': 55, 's': [{'f': 1, 'm': True}]},
    {'f': 58, 's': [{'f': 1, 'm': True}]},
    {'f': 59, 's': [{'f': 1, 'm': True}]},
    {'f': 60, 's': [{'f': 2}, {'f': 3, 's': [{'f': 1}, {'f': 2}]}, {'f': 5, 's': _U},
                    {'f': 8, 's': [{'f': 1}, {'f': 2}]}]},
    {'f': 64, 's': [{'f': 2}, {'f': 3, 's': [{'f': 1}, {'f': 2}]}, {'f': 5, 's': _U},
                    {'f': 8, 's': [{'f': 1}, {'f': 2}]}]},
    {'f': 66, 's': [{'f': 2}, {'f': 6}, {'f': 7}, {'f': 13}, {'f': 17, 's': _U}, {'f': 20}]},
    {'f': 74, 's': [{'f': 1, 'm': True}]},
    {'f': 87, 's': [{'f': 1, 'm': True}]},
    {'f': 88, 's': [{'f': 1}, {'f': 2, 's': [{'f': 1}]}, {'f': 3, 's': _U}]},
    {'f': 92, 's': [{'f': 1, 'm': True}]},
    {'f': 93, 's': [{'f': 1, 'm': True}]},
    {'f': 94, 's': [{'f': 1, 'm': True}]},
]


def _compile(fields) -> dict:
    return {f['f']: {'m': f.get('m'), 'children': _compile(f['s']) if f.get('s') else None} for f in fields}


compiled_reporting_fields = _compile(reporting_fields)
_EMPTY: dict = {}
ENC_SECRET_REPORT_TOKEN = 'Report Token'
WIRE_VARINT, WIRE_FIXED64, WIRE_BYTES, WIRE_FIXED32 = 0, 1, 2, 5


def should_include_reporting_token(message: dict) -> bool:
    return not message.get('reactionMessage') and not message.get('encReactionMessage') and \
        not message.get('encEventResponseMessage') and not message.get('pollUpdateMessage')


def _generate_msg_secret_key(modification_type, orig_msg_id, orig_msg_sender, modification_sender, orig_msg_secret):
    use_case_secret = (orig_msg_id.encode() + orig_msg_sender.encode() + modification_sender.encode() +
                       modification_type.encode())
    return hkdf(bytes(orig_msg_secret), 32, info=use_case_secret.decode('latin1'))


def _decode_varint(buf: bytes, offset: int):
    value = bytes_ = shift = 0
    while offset + bytes_ < len(buf):
        cur = buf[offset + bytes_]
        value |= (cur & 0x7F) << shift
        bytes_ += 1
        if (cur & 0x80) == 0:
            return value, bytes_, True
        shift += 7
        if shift > 35:
            return 0, 0, False
    return 0, 0, False


def _encode_varint(value: int) -> bytes:
    parts = []
    remaining = value & 0xFFFFFFFF
    while remaining > 0x7F:
        parts.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    parts.append(remaining)
    return bytes(parts)


def _extract(data: bytes, cfg: dict) -> Optional[bytes]:
    out = []
    i = 0
    while i < len(data):
        tag_v, tag_b, ok = _decode_varint(data, i)
        if not ok:
            return None
        field_num, wire = tag_v >> 3, tag_v & 7
        field_start = i
        i += tag_b
        fcfg = cfg.get(field_num)
        if wire in (WIRE_VARINT, WIRE_FIXED64, WIRE_FIXED32):
            if wire == WIRE_VARINT:
                _, vb, ok = _decode_varint(data, i)
                if not ok:
                    return None
                end = i + vb
            else:
                end = i + (8 if wire == WIRE_FIXED64 else 4)
            if end > len(data):
                return None
            if fcfg:
                out.append((field_num, data[field_start:end]))
            i = end
            continue
        if wire == WIRE_BYTES:
            ln, lb, ok = _decode_varint(data, i)
            if not ok:
                return None
            val_start = i + lb
            val_end = val_start + ln
            if val_end > len(data):
                return None
            if not fcfg:
                i = val_end
                continue
            if fcfg.get('m') or fcfg.get('children'):
                sub = _extract(data[val_start:val_end], fcfg.get('children') or _EMPTY)
                if sub is None:
                    return None
                if len(sub) > 0:
                    out.append((field_num, _encode_varint(tag_v) + _encode_varint(len(sub)) + sub))
                i = val_end
                continue
            out.append((field_num, data[field_start:val_end]))
            i = val_end
            continue
        return None
    if not out:
        return b''
    out.sort(key=lambda x: x[0])
    return b''.join(b for _, b in out)


async def get_message_reporting_token(msg_protobuf: bytes, message: dict, key: dict) -> Optional[dict]:
    msg_secret = (message.get('messageContextInfo') or {}).get('messageSecret')
    if not msg_secret or not key.get('id'):
        return None
    from_ = key.get('remoteJid') if key.get('fromMe') else (key.get('participant') or key.get('remoteJid'))
    to = (key.get('participant') or key.get('remoteJid')) if key.get('fromMe') else key.get('remoteJid')
    reporting_secret = _generate_msg_secret_key(ENC_SECRET_REPORT_TOKEN, key['id'], from_, to, msg_secret)
    content = _extract(bytes(msg_protobuf), compiled_reporting_fields)
    if not content:
        return None
    token = _hmac.new(reporting_secret, content, hashlib.sha256).digest()[:16]
    return {'tag': 'reporting', 'attrs': {}, 'content': [
        {'tag': 'reporting_token', 'attrs': {'v': '2'}, 'content': token}]}
