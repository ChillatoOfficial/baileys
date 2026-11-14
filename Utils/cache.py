"""Small TTL cache standing in for node-cache / lru-cache."""
from __future__ import annotations
import time
from collections import OrderedDict
from typing import Any, Optional


class TTLCache:
    def __init__(self, ttl: Optional[float] = None, max_size: Optional[int] = None, update_age_on_get: bool = False):
        self.ttl = ttl
        self.max_size = max_size
        self.update_age_on_get = update_age_on_get
        self._d: OrderedDict[str, tuple[Any, Optional[float]]] = OrderedDict()

    def _expired(self, exp) -> bool:
        return exp is not None and exp < time.monotonic()

    def get(self, key: str, default=None):
        item = self._d.get(key)
        if item is None:
            return default
        value, exp = item
        if self._expired(exp):
            del self._d[key]
            return default
        if self.update_age_on_get and self.ttl:
            self._d[key] = (value, time.monotonic() + self.ttl)
        self._d.move_to_end(key)
        return value

    def has(self, key: str) -> bool:
        return self.get(key, _MISSING) is not _MISSING

    def set(self, key: str, value, ttl: Optional[float] = None) -> bool:
        t = ttl if ttl is not None else self.ttl
        self._d[key] = (value, time.monotonic() + t if t else None)
        self._d.move_to_end(key)
        if self.max_size and len(self._d) > self.max_size:
            self._d.popitem(last=False)
        return True

    def delete(self, key: str):
        return self._d.pop(key, None) is not None

    del_ = delete

    def keys(self):
        return [k for k in list(self._d) if self.has(k)]

    def flush_all(self):
        self._d.clear()

    clear = flush_all

    def close(self):
        self._d.clear()

    def __len__(self):
        return len(self._d)


_MISSING = object()
