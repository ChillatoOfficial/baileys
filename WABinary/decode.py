from __future__ import annotations
import zlib
from . import constants
from .jid_utils import jid_encode, WAJIDDomains


def decompressing_if_required(buffer: bytes) -> bytes:
    if 2 & buffer[0]:
        return zlib.decompress(buffer[1:])
    # nodes with no compression have a 0x00 prefix, we remove that
    return buffer[1:]


def decode_decompressed_binary_node(buffer: bytes, opts=constants, index_ref: dict | None = None) -> dict:
    DOUBLE_BYTE_TOKENS = opts.DOUBLE_BYTE_TOKENS
    SINGLE_BYTE_TOKENS = opts.SINGLE_BYTE_TOKENS
    TAGS = opts.TAGS
    if index_ref is None:
        index_ref = {'index': 0}

    def check_eos(length):
        if index_ref['index'] + length > len(buffer):
            raise ValueError('end of stream')

    def read_byte():
        check_eos(1)
        v = buffer[index_ref['index']]
        index_ref['index'] += 1
        return v

    def read_bytes(n):
        check_eos(n)
        i = index_ref['index']
        v = bytes(buffer[i:i + n])
        index_ref['index'] += n
        return v

    def read_string_from_chars(length):
        return read_bytes(length).decode('utf-8', errors='replace')

    def read_int(n, little_endian=False):
        check_eos(n)
        val = 0
        for i in range(n):
            shift = i if little_endian else n - 1 - i
            val |= read_byte() << (shift * 8)
        return val

    def read_int20():
        check_eos(3)
        return ((read_byte() & 15) << 16) + (read_byte() << 8) + read_byte()

    def unpack_hex(v):
        if 0 <= v < 16:
            return ord('0') + v if v < 10 else ord('A') + v - 10
        raise ValueError(f'invalid hex: {v}')

    def unpack_nibble(v):
        if 0 <= v <= 9:
            return ord('0') + v
        if v == 10:
            return ord('-')
        if v == 11:
            return ord('.')
        if v == 15:
            return 0
        raise ValueError(f'invalid nibble: {v}')

    def unpack_byte(tag, v):
        if tag == TAGS['NIBBLE_8']:
            return unpack_nibble(v)
        if tag == TAGS['HEX_8']:
            return unpack_hex(v)
        raise ValueError(f'unknown tag: {tag}')

    def read_packed8(tag):
        start = read_byte()
        value = ''
        for _ in range(start & 127):
            cur = read_byte()
            value += chr(unpack_byte(tag, (cur & 0xF0) >> 4))
            value += chr(unpack_byte(tag, cur & 0x0F))
        if start >> 7 != 0:
            value = value[:-1]
        return value

    def is_list_tag(tag):
        return tag in (TAGS['LIST_EMPTY'], TAGS['LIST_8'], TAGS['LIST_16'])

    def read_list_size(tag):
        if tag == TAGS['LIST_EMPTY']:
            return 0
        if tag == TAGS['LIST_8']:
            return read_byte()
        if tag == TAGS['LIST_16']:
            return read_int(2)
        raise ValueError(f'invalid tag for list size: {tag}')

    def read_jid_pair():
        i = read_string(read_byte())
        j = read_string(read_byte())
        if j:
            return (i or '') + '@' + j
        raise ValueError(f'invalid jid pair: {i}, {j}')

    def read_ad_jid():
        domain_type = int(read_byte())
        device = read_byte()
        user = read_string(read_byte())
        server = 's.whatsapp.net'
        if domain_type == WAJIDDomains.LID:
            server = 'lid'
        elif domain_type == WAJIDDomains.HOSTED:
            server = 'hosted'
        elif domain_type == WAJIDDomains.HOSTED_LID:
            server = 'hosted.lid'
        return jid_encode(user, server, device)

    def read_fb_jid():
        user = read_string(read_byte())
        device = read_int(2)
        server = read_string(read_byte())
        return f'{user}:{device}@{server}'

    def read_interop_jid():
        user = read_string(read_byte())
        device = read_int(2)
        integrator = read_int(2)
        server = 'interop'
        before = index_ref['index']
        try:
            server = read_string(read_byte())
        except Exception:
            index_ref['index'] = before
        return f'{integrator}-{user}:{device}@{server}'

    def get_token_double(i1, i2):
        if i1 >= len(DOUBLE_BYTE_TOKENS):
            raise ValueError(f'Invalid double token dict ({i1})')
        d = DOUBLE_BYTE_TOKENS[i1]
        if i2 >= len(d):
            raise ValueError(f'Invalid double token ({i2})')
        return d[i2]

    def read_string(tag):
        if 1 <= tag < len(SINGLE_BYTE_TOKENS):
            return SINGLE_BYTE_TOKENS[tag] or ''
        if TAGS['DICTIONARY_0'] <= tag <= TAGS['DICTIONARY_3']:
            return get_token_double(tag - TAGS['DICTIONARY_0'], read_byte())
        if tag == TAGS['LIST_EMPTY']:
            return ''
        if tag == TAGS['BINARY_8']:
            return read_string_from_chars(read_byte())
        if tag == TAGS['BINARY_20']:
            return read_string_from_chars(read_int20())
        if tag == TAGS['BINARY_32']:
            return read_string_from_chars(read_int(4))
        if tag == TAGS['JID_PAIR']:
            return read_jid_pair()
        if tag == TAGS['FB_JID']:
            return read_fb_jid()
        if tag == TAGS['INTEROP_JID']:
            return read_interop_jid()
        if tag == TAGS['AD_JID']:
            return read_ad_jid()
        if tag in (TAGS['HEX_8'], TAGS['NIBBLE_8']):
            return read_packed8(tag)
        raise ValueError(f'invalid string with tag: {tag}')

    def read_list(tag):
        size = read_list_size(tag)
        return [decode_decompressed_binary_node(buffer, opts, index_ref) for _ in range(size)]

    list_size = read_list_size(read_byte())
    header = read_string(read_byte())
    if not list_size or not len(header):
        raise ValueError('invalid node')

    attrs = {}
    data = None
    for _ in range((list_size - 1) >> 1):
        key = read_string(read_byte())
        value = read_string(read_byte())
        attrs[key] = value

    if list_size % 2 == 0:
        tag = read_byte()
        if is_list_tag(tag):
            data = read_list(tag)
        elif tag == TAGS['BINARY_8']:
            data = read_bytes(read_byte())
        elif tag == TAGS['BINARY_20']:
            data = read_bytes(read_int20())
        elif tag == TAGS['BINARY_32']:
            data = read_bytes(read_int(4))
        else:
            data = read_string(tag)

    return {'tag': header, 'attrs': attrs, 'content': data}


def decode_binary_node(buff: bytes) -> dict:
    return decode_decompressed_binary_node(decompressing_if_required(buff), constants)
