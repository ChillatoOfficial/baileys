from __future__ import annotations
from typing import Optional
from ..WAProto import WAProto_pb2 as proto
from .types import BinaryNode


class Boom(Exception):
    """Equivalent of @hapi/boom errors: carries a status code, data and output payload."""

    def __init__(self, message: str = '', data=None, status_code: Optional[int] = None, is_boom=True):
        super().__init__(message)
        self.message = message
        self.data = data
        self.status_code = status_code if status_code is not None else (
            data.get('statusCode') if isinstance(data, dict) and 'statusCode' in data else 500)
        self.output = {'statusCode': self.status_code, 'payload': {'statusCode': self.status_code, 'message': message}}
        self.is_boom = True


def get_binary_node_children(node, child_tag: str) -> list:
    if not node or not isinstance(node.get('content'), list):
        return []
    return [c for c in node['content'] if c.get('tag') == child_tag]


def get_binary_node_child(node, child_tag: str):
    r = get_binary_node_children(node, child_tag)
    return r[0] if r else None


def get_all_binary_node_children(node) -> list:
    c = node.get('content')
    return c if isinstance(c, list) else []


def get_binary_node_child_buffer(node, child_tag: str):
    child = get_binary_node_child(node, child_tag)
    c = child.get('content') if child else None
    if isinstance(c, (bytes, bytearray)):
        return bytes(c)
    return None


def get_binary_node_child_string(node, child_tag: str):
    child = get_binary_node_child(node, child_tag)
    c = child.get('content') if child else None
    if isinstance(c, (bytes, bytearray)):
        return bytes(c).decode('utf-8')
    if isinstance(c, str):
        return c
    return None


def _buffer_to_uint(b: bytes, n: int) -> int:
    a = 0
    for i in range(n):
        a = 256 * a + b[i]
    return a


def get_binary_node_child_uint(node, child_tag: str, length: int):
    buff = get_binary_node_child_buffer(node, child_tag)
    if buff:
        return _buffer_to_uint(buff, length)
    return None


def assert_node_error_free(node):
    err = get_binary_node_child(node, 'error')
    if err:
        raise Boom(err['attrs'].get('text') or 'Unknown error', data=int(err['attrs'].get('code') or 0))


def reduce_binary_node_to_dictionary(node, tag: str) -> dict:
    d = {}
    for n in get_binary_node_children(node, tag):
        attrs = n['attrs']
        val = attrs.get('value') or attrs.get('config_value')
        if isinstance(attrs.get('name'), str):
            d[attrs['name']] = val
        else:
            d[attrs.get('config_code')] = val
    return d


def get_binary_node_messages(node) -> list:
    msgs = []
    if isinstance(node.get('content'), list):
        for item in node['content']:
            if item['tag'] == 'message':
                m = proto.WebMessageInfo()
                m.ParseFromString(item['content'])
                msgs.append(m)
    return msgs


def binary_node_to_string(node, i: int = 0) -> str:
    tabs = lambda n: '\t' * n
    if not node:
        return node
    if isinstance(node, str):
        return tabs(i) + node
    if isinstance(node, (bytes, bytearray)):
        return tabs(i) + bytes(node).hex()
    if isinstance(node, list):
        return '\n'.join(tabs(i + 1) + binary_node_to_string(x, i + 1) for x in node)
    children = binary_node_to_string(node.get('content'), i + 1)
    attrs = ' '.join(f"{k}='{v}'" for k, v in (node.get('attrs') or {}).items() if v is not None)
    tag = f"<{node['tag']} {attrs}"
    content = f">\n{children}\n{tabs(i)}</{node['tag']}>" if children else '/>'
    return tag + content
