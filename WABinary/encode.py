from __future__ import annotations
from . import constants
from .jid_utils import jid_decode


def encode_binary_node(node: dict, opts=constants, buffer: bytearray | None = None) -> bytes:
    if buffer is None:
        buffer = bytearray([0])
    _encode_inner(node, opts, buffer)
    return bytes(buffer)


def _is_bytes(x):
    return isinstance(x, (bytes, bytearray, memoryview))


def _encode_inner(node: dict, opts, buffer: bytearray):
    tag = node.get('tag')
    attrs = node.get('attrs') or {}
    content = node.get('content')
    TAGS = opts.TAGS
    TOKEN_MAP = opts.TOKEN_MAP

    push_byte = lambda v: buffer.append(v & 0xFF)

    def push_int(value, n):
        for i in range(n):
            buffer.append((value >> ((n - 1 - i) * 8)) & 0xFF)

    def push_bytes(b):
        buffer.extend(b)

    def push_int20(v):
        buffer.extend([(v >> 16) & 0x0F, (v >> 8) & 0xFF, v & 0xFF])

    def write_byte_length(length):
        if length >= 4294967296:
            raise ValueError(f'string too large to encode: {length}')
        if length >= 1 << 20:
            push_byte(TAGS['BINARY_32'])
            push_int(length, 4)
        elif length >= 256:
            push_byte(TAGS['BINARY_20'])
            push_int20(length)
        else:
            push_byte(TAGS['BINARY_8'])
            push_byte(length)

    def write_string_raw(s):
        b = s.encode('utf-8')
        write_byte_length(len(b))
        push_bytes(b)

    def write_jid(jid):
        if jid.device is not None:
            push_byte(TAGS['AD_JID'])
            push_byte(jid.domainType or 0)
            push_byte(jid.device or 0)
            write_string(jid.user)
        else:
            push_byte(TAGS['JID_PAIR'])
            if len(jid.user):
                write_string(jid.user)
            else:
                push_byte(TAGS['LIST_EMPTY'])
            write_string(jid.server)

    def pack_nibble(c):
        if c == '-':
            return 10
        if c == '.':
            return 11
        if c == '\0':
            return 15
        if '0' <= c <= '9':
            return ord(c) - 48
        raise ValueError(f'invalid byte for nibble "{c}"')

    def pack_hex(c):
        if '0' <= c <= '9':
            return ord(c) - 48
        if 'A' <= c <= 'F':
            return 10 + ord(c) - 65
        if 'a' <= c <= 'f':
            return 10 + ord(c) - 97
        if c == '\0':
            return 15
        raise ValueError(f'Invalid hex char "{c}"')

    def write_packed(s, typ):
        if len(s) > TAGS['PACKED_MAX']:
            raise ValueError('Too many bytes to pack')
        push_byte(TAGS['NIBBLE_8'] if typ == 'nibble' else TAGS['HEX_8'])
        rounded = (len(s) + 1) // 2
        if len(s) % 2 != 0:
            rounded |= 128
        push_byte(rounded)
        pack = pack_nibble if typ == 'nibble' else pack_hex
        for i in range(len(s) // 2):
            push_byte((pack(s[2 * i]) << 4) | pack(s[2 * i + 1]))
        if len(s) % 2 != 0:
            push_byte((pack(s[-1]) << 4) | pack('\0'))

    def is_nibble(s):
        if not s or len(s) > TAGS['PACKED_MAX']:
            return False
        return all(('0' <= c <= '9') or c in '-.' for c in s)

    def is_hex(s):
        if not s or len(s) > TAGS['PACKED_MAX']:
            return False
        return all(('0' <= c <= '9') or ('A' <= c <= 'F') for c in s)

    def write_string(s):
        if s is None:
            push_byte(TAGS['LIST_EMPTY'])
            return
        if s == '':
            write_string_raw(s)
            return
        tok = TOKEN_MAP.get(s)
        if tok:
            if isinstance(tok.get('dict'), int):
                push_byte(TAGS['DICTIONARY_0'] + tok['dict'])
            push_byte(tok['index'])
        elif is_nibble(s):
            write_packed(s, 'nibble')
        elif is_hex(s):
            write_packed(s, 'hex')
        else:
            jid = jid_decode(s)
            if jid:
                write_jid(jid)
            else:
                write_string_raw(s)

    def write_list_start(size):
        if size == 0:
            push_byte(TAGS['LIST_EMPTY'])
        elif size < 256:
            push_bytes([TAGS['LIST_8'], size])
        else:
            push_byte(TAGS['LIST_16'])
            push_bytes([(size >> 8) & 0xFF, size & 0xFF])

    if not tag:
        raise ValueError('Invalid node: tag cannot be undefined')

    valid_attrs = [k for k, v in attrs.items() if v is not None]
    write_list_start(2 * len(valid_attrs) + 1 + (1 if content is not None else 0))
    write_string(tag)
    for k in valid_attrs:
        v = attrs[k]
        if isinstance(v, str):
            write_string(k)
            write_string(v)

    if isinstance(content, str):
        write_string(content)
    elif _is_bytes(content):
        write_byte_length(len(content))
        push_bytes(content)
    elif isinstance(content, list):
        valid = [i for i in content if i and (isinstance(i, dict) and i.get('tag') or _is_bytes(i) or isinstance(i, str))]
        write_list_start(len(valid))
        for item in valid:
            _encode_inner(item, opts, buffer)
    elif content is None:
        pass
    else:
        raise ValueError(f'invalid children for header "{tag}": {content}')
