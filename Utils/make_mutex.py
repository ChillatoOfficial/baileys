from __future__ import annotations
import asyncio
import inspect


async def _run(lock: asyncio.Lock, code):
    async with lock:
        r = code()
        if inspect.isawaitable(r):
            r = await r
        return r


def make_mutex():
    lock = asyncio.Lock()

    class _M:
        async def mutex(self, code):
            return await _run(lock, code)
    return _M()


def make_keyed_mutex():
    entries: dict = {}

    class _K:
        async def mutex(self, key: str, task):
            entry = entries.get(key)
            if entry is None:
                entry = {'lock': asyncio.Lock(), 'ref': 0}
                entries[key] = entry
            entry['ref'] += 1
            try:
                return await _run(entry['lock'], task)
            finally:
                entry['ref'] -= 1
                if entry['ref'] == 0 and entries.get(key) is entry:
                    del entries[key]
    return _K()
