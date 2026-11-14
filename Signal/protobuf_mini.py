"""Tiny protobuf codec for libsignal's WhisperTextProtocol messages."""
from __future__ import annotations


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _read_varint(buf: bytes, i: int):
    shift = 0
    result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def encode(fields: dict, schema: dict) -> bytes:
    """schema: {name: (field_no, 'bytes'|'uint32')}; encoded sorted by field number."""
    out = bytearray()
    for name, (no, typ) in sorted(schema.items(), key=lambda kv: kv[1][0]):
        v = fields.get(name)
        if v is None:
            continue
        if typ == 'uint32':
            out += _varint(no << 3) + _varint(int(v))
        else:
            v = bytes(v)
            out += _varint((no << 3) | 2) + _varint(len(v)) + v
    return bytes(out)


def decode(buf: bytes, schema: dict) -> dict:
    by_no = {no: (name, typ) for name, (no, typ) in schema.items()}
    out = {}
    i = 0
    buf = bytes(buf)
    while i < len(buf):
        tag, i = _read_varint(buf, i)
        no, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _read_varint(buf, i)
        elif wt == 2:
            ln, i = _read_varint(buf, i)
            v = buf[i:i + ln]
            i += ln
        elif wt == 1:
            v = buf[i:i + 8]; i += 8
        elif wt == 5:
            v = buf[i:i + 4]; i += 4
        else:
            raise ValueError('bad wire type')
        if no in by_no:
            out[by_no[no][0]] = v
    return out


WHISPER_MESSAGE = {'ephemeralKey': (1, 'bytes'), 'counter': (2, 'uint32'),
                   'previousCounter': (3, 'uint32'), 'ciphertext': (4, 'bytes')}
PREKEY_WHISPER_MESSAGE = {'registrationId': (5, 'uint32'), 'preKeyId': (1, 'uint32'),
                          'signedPreKeyId': (6, 'uint32'), 'baseKey': (2, 'bytes'),
                          'identityKey': (3, 'bytes'), 'message': (4, 'bytes')}
