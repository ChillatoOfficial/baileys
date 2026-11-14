from __future__ import annotations
import asyncio
import inspect
from collections import defaultdict
from typing import Any, Callable


class EventEmitter:
    """Minimal Node.js-like EventEmitter. Listeners may be sync functions or coroutine functions."""

    def __init__(self):
        self._listeners: dict[str, list] = defaultdict(list)

    def on(self, event: str, listener: Callable):
        self._listeners[event].append(listener)
        return self

    add_listener = on

    def once(self, event: str, listener: Callable):
        def wrapper(*args):
            self.off(event, wrapper)
            return listener(*args)
        wrapper._orig = listener
        return self.on(event, wrapper)

    def off(self, event: str, listener: Callable):
        lst = self._listeners.get(event)
        if not lst:
            return self
        for i, l in enumerate(lst):
            if l is listener or getattr(l, '_orig', None) is listener:
                lst.pop(i)
                break
        return self

    remove_listener = off

    def remove_all_listeners(self, event: str | None = None):
        if event is None:
            self._listeners.clear()
        else:
            self._listeners.pop(event, None)
        return self

    def listener_count(self, event: str) -> int:
        return len(self._listeners.get(event, []))

    def emit(self, event: str, *args) -> bool:
        listeners = list(self._listeners.get(event, []))
        for l in listeners:
            try:
                r = l(*args)
                if inspect.isawaitable(r):
                    fut = asyncio.ensure_future(r)
                    fut.add_done_callback(_log_task_error)
            except Exception:  # a failing listener must not break the emitter
                import traceback
                traceback.print_exc()
        return bool(listeners)


def _log_task_error(fut: asyncio.Future):
    if fut.cancelled():
        return
    exc = fut.exception()
    if exc:
        import traceback
        traceback.print_exception(type(exc), exc, exc.__traceback__)
