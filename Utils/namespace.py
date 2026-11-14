from __future__ import annotations
import re
from typing import Any, Callable

_CAMEL = re.compile(r'(?<!^)(?=[A-Z])')


def camel_to_snake(name: str) -> str:
    return _CAMEL.sub('_', name).lower()


class Namespace:
    """JS-object-like bag of functions/values (the socket layers do `{...sock, newFn}` in the original).

    Attribute access falls back from camelCase to snake_case, so both `sock.sendMessage` and `sock.send_message`
    work. `props` are lazily evaluated getters (JS `get user() {}`)."""

    def __init__(self, _props: dict | None = None, **kwargs):
        object.__setattr__(self, '_props', dict(_props or {}))
        for k, v in kwargs.items():
            object.__setattr__(self, k, v)

    def extend(self, _props: dict | None = None, **kwargs) -> 'Namespace':
        data = {k: v for k, v in self.__dict__.items() if k != '_props'}
        data.update(kwargs)
        props = dict(self._props)
        props.update(_props or {})
        return Namespace(props, **data)

    def __getattr__(self, name: str) -> Any:
        props = object.__getattribute__(self, '_props')
        if name in props:
            return props[name]()
        snake = camel_to_snake(name)
        if snake != name:
            if snake in props:
                return props[snake]()
            d = object.__getattribute__(self, '__dict__')
            if snake in d:
                return d[snake]
        # tolerant fallback: compare names ignoring case and underscores (onWhatsApp -> on_whatsapp)
        d = object.__getattribute__(self, '__dict__')
        flat = name.replace('_', '').lower()
        for k in list(props) + list(d):
            if k != '_props' and k.replace('_', '').lower() == flat:
                return props[k]() if k in props else d[k]
        raise AttributeError(name)

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __contains__(self, key: str) -> bool:
        try:
            getattr(self, key)
            return True
        except AttributeError:
            return False
