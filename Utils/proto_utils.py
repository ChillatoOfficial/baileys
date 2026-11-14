"""Helpers to move between plain dicts (JS-like objects) and protobuf messages."""
from __future__ import annotations
from typing import Any
from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor


def _clean(v):
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items() if x is not None}
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v if x is not None]
    if isinstance(v, memoryview):
        return bytes(v)
    if isinstance(v, bytearray):
        return bytes(v)
    return v


def from_object(cls, obj) -> Message:
    """proto.X.fromObject(obj)"""
    if isinstance(obj, Message):
        m = cls()
        m.CopyFrom(obj)
        return m
    m = cls()
    _fill(m, obj)
    return m


def _fill(m: Message, obj: dict):
    desc = m.DESCRIPTOR.fields_by_name
    for k, v in (obj or {}).items():
        if v is None or k not in desc:
            continue
        f = desc[k]
        if f.type == FieldDescriptor.TYPE_MESSAGE:
            if f.message_type.GetOptions().map_entry:
                # map field
                tgt = getattr(m, k)
                vf = f.message_type.fields_by_name['value']
                for mk, mv in v.items():
                    if vf.type == FieldDescriptor.TYPE_MESSAGE:
                        tgt[mk].CopyFrom(mv if isinstance(mv, Message) else from_object(type(tgt[mk]), mv))
                    else:
                        tgt[mk] = mv
            elif f.is_repeated:
                tgt = getattr(m, k)
                for item in v:
                    sub = tgt.add()
                    if isinstance(item, Message):
                        sub.CopyFrom(item)
                    else:
                        _fill(sub, item)
            else:
                sub = getattr(m, k)
                if isinstance(v, Message):
                    sub.CopyFrom(v)
                else:
                    _fill(sub, v)
                sub.SetInParent()
        elif f.is_repeated:
            getattr(m, k).extend(_coerce(f, x) for x in _clean(v))
        else:
            setattr(m, k, _coerce(f, v))


_INT_TYPES = {FieldDescriptor.TYPE_INT32, FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT32,
              FieldDescriptor.TYPE_UINT64, FieldDescriptor.TYPE_SINT32, FieldDescriptor.TYPE_SINT64,
              FieldDescriptor.TYPE_FIXED32, FieldDescriptor.TYPE_FIXED64, FieldDescriptor.TYPE_SFIXED32,
              FieldDescriptor.TYPE_SFIXED64}


def _coerce(f, v):
    """Loose JS-like coercion (protobufjs fromObject semantics)."""
    t = f.type
    if t == FieldDescriptor.TYPE_BYTES:
        if isinstance(v, str):
            import base64
            return base64.b64decode(v)
        return bytes(v)
    if t in _INT_TYPES:
        if isinstance(v, (str, float, bool)):
            return int(float(v)) if isinstance(v, (str, float)) else int(v)
        return v
    if t == FieldDescriptor.TYPE_ENUM:
        if isinstance(v, str):
            try:
                return f.enum_type.values_by_name[v].number
            except KeyError:
                return int(v) if v.lstrip('-').isdigit() else 0
        return int(v)
    if t == FieldDescriptor.TYPE_BOOL:
        return bool(v)
    if t in (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE):
        return float(v)
    if t == FieldDescriptor.TYPE_STRING and not isinstance(v, str):
        return str(v)
    return v


def normalize(cls, obj) -> dict:
    """fromObject + back to plain dict: drops unknown fields and coerces values like protobufjs does."""
    return to_dict(from_object(cls, obj))


def decode(cls, data: bytes) -> Message:
    m = cls()
    m.ParseFromString(bytes(data))
    return m


def encode(msg) -> bytes:
    if isinstance(msg, dict):
        raise TypeError('encode() needs a protobuf message; use from_object first')
    return msg.SerializeToString()


def has(msg: Message, field: str) -> bool:
    """True if an optional/message field is explicitly set."""
    try:
        return msg.HasField(field)
    except ValueError:
        return bool(getattr(msg, field, None))


def to_dict(msg: Message) -> dict:
    """Message -> plain dict with only the set fields (bytes preserved)."""
    out: dict[str, Any] = {}
    for f, v in msg.ListFields():
        if f.type == FieldDescriptor.TYPE_MESSAGE:
            if f.message_type.GetOptions().map_entry:
                vf = f.message_type.fields_by_name['value']
                out[f.name] = {k: (to_dict(x) if vf.type == FieldDescriptor.TYPE_MESSAGE else x) for k, x in v.items()}
            elif f.is_repeated:
                out[f.name] = [to_dict(x) for x in v]
            else:
                out[f.name] = to_dict(v)
        elif f.is_repeated:
            out[f.name] = list(v)
        else:
            out[f.name] = v
    return out


def set_fields(msg: Message) -> list[str]:
    return [f.name for f, _ in msg.ListFields()]


def copy_message(msg: Message) -> Message:
    m = type(msg)()
    m.CopyFrom(msg)
    return m
