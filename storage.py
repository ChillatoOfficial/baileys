"""Auth-state backends besides the default multi-file folder.

All of them return `(state, save_creds)` exactly like `use_multi_file_auth_state`:

    state, save = await use_sqlite_auth_state('auth.db')
    state, save = await use_redis_auth_state(redis.asyncio.Redis(), prefix='bot1')
    state, save = await use_custom_auth_state(MyBackend())      # any storage you like

A backend only needs two coroutines (see `AuthBackend`).
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from typing import Optional, Protocol

from .Utils.auth_utils import init_auth_creds
from .Utils.generics import BufferJSON
from .Utils.use_multi_file_auth_state import _to_plain


class AuthBackend(Protocol):
    async def get_many(self, keys: list[str]) -> dict[str, Optional[str]]:
        """Return {key: serialized value or None}."""

    async def set_many(self, items: dict[str, Optional[str]]) -> None:
        """Write the given keys atomically if you can. A value of None means delete."""


async def use_custom_auth_state(backend: AuthBackend):
    """Build an auth state on top of any key/value backend (JSON strings in, JSON strings out)."""
    dumps = lambda v: BufferJSON.dumps(_to_plain(v))
    raw = (await backend.get_many(['creds'])).get('creds')
    creds = BufferJSON.loads(raw) if raw else init_auth_creds()

    class _Keys:
        async def get(self, type_, ids):
            res = await backend.get_many([f'{type_}-{i}' for i in ids])
            return {i: (BufferJSON.loads(res[f'{type_}-{i}']) if res.get(f'{type_}-{i}') else None) for i in ids}

        async def set(self, data):
            items = {}
            for category, entries in data.items():
                for i, value in entries.items():
                    items[f'{category}-{i}'] = dumps(value) if value else None
            if items:
                await backend.set_many(items)

    async def save_creds():
        await backend.set_many({'creds': dumps(creds)})

    return {'creds': creds, 'keys': _Keys()}, save_creds


class SQLiteBackend:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute('CREATE TABLE IF NOT EXISTS auth (k TEXT PRIMARY KEY, v TEXT NOT NULL)')

    def _get(self, keys):
        out = {}
        with self._lock:
            for i in range(0, len(keys), 500):  # stay below SQLite's variable limit
                chunk = keys[i:i + 500]
                q = f'SELECT k, v FROM auth WHERE k IN ({",".join("?" * len(chunk))})'
                out.update(dict(self._db.execute(q, chunk).fetchall()))
        return out

    def _set(self, items):
        with self._lock:
            self._db.execute('BEGIN')
            try:
                for k, v in items.items():
                    if v is None:
                        self._db.execute('DELETE FROM auth WHERE k=?', (k,))
                    else:
                        self._db.execute('INSERT INTO auth(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v', (k, v))
                self._db.execute('COMMIT')
            except BaseException:
                self._db.execute('ROLLBACK')
                raise

    async def get_many(self, keys):
        return await asyncio.get_running_loop().run_in_executor(None, self._get, list(keys))

    async def set_many(self, items):
        await asyncio.get_running_loop().run_in_executor(None, self._set, items)

    def close(self):
        self._db.close()


async def use_sqlite_auth_state(path: str = 'baileys_auth.db'):
    """Whole auth state in one SQLite file (stdlib only). Good default for a single bot."""
    return await use_custom_auth_state(SQLiteBackend(path))


class RedisBackend:
    """Works with `redis.asyncio.Redis` (or anything exposing awaitable mget / mset / delete / pipeline)."""

    def __init__(self, redis, prefix: str = 'baileys'):
        self.r, self.prefix = redis, prefix

    def _k(self, k: str) -> str:
        return f'{self.prefix}:{k}'

    async def get_many(self, keys):
        keys = list(keys)
        if not keys:
            return {}
        vals = await self.r.mget([self._k(k) for k in keys])
        return {k: (v.decode() if isinstance(v, (bytes, bytearray)) else v) for k, v in zip(keys, vals)}

    async def set_many(self, items):
        sets = {self._k(k): v for k, v in items.items() if v is not None}
        dels = [self._k(k) for k, v in items.items() if v is None]
        if sets:
            await self.r.mset(sets)
        if dels:
            await self.r.delete(*dels)


async def use_redis_auth_state(redis, prefix: str = 'baileys'):
    """Auth state in Redis; use a different `prefix` per account when sharing one Redis."""
    return await use_custom_auth_state(RedisBackend(redis, prefix))
