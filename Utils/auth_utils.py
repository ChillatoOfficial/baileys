from __future__ import annotations
import asyncio
import base64
import contextvars
import inspect
import os
from typing import Optional

from ..Defaults import DEFAULT_CACHE_TTLS
from ..WABinary import Boom
from .cache import TTLCache
from .crypto import Curve, signed_key_pair
from .generics import delay, generate_registration_id
from .pre_key_manager import PreKeyManager


async def maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x


def make_cacheable_signal_key_store(store, logger=None, _cache=None):
    """Adds caching capability to a SignalKeyStore."""
    cache = _cache or TTLCache(ttl=DEFAULT_CACHE_TTLS['SIGNAL_STORE'])
    lock = asyncio.Lock()
    uid = lambda t, i: f'{t}.{i}'

    class _Store:
        async def get(self, type_, ids):
            async with lock:
                data, to_fetch = {}, []
                for i in ids:
                    item = await maybe_await(cache.get(uid(type_, i)))
                    if item is not None:
                        data[i] = item
                    else:
                        to_fetch.append(i)
                if to_fetch:
                    if logger:
                        logger.trace({'items': len(to_fetch)}, 'loading from store')
                    fetched = await maybe_await(store.get(type_, to_fetch))
                    for i in to_fetch:
                        item = fetched.get(i)
                        if item:
                            data[i] = item
                            await maybe_await(cache.set(uid(type_, i), item))
                return data

        async def set(self, data):
            async with lock:
                keys = 0
                for type_, entries in data.items():
                    for i, v in entries.items():
                        await maybe_await(cache.set(uid(type_, i), v))
                        keys += 1
                if logger:
                    logger.trace({'keys': keys}, 'updated cache')
                await maybe_await(store.set(data))

        async def clear(self):
            await maybe_await(cache.flush_all())
            if hasattr(store, 'clear'):
                await maybe_await(store.clear())
    return _Store()


def add_transaction_capability(state, logger, max_commit_retries: int = 10, delay_between_tries_ms: int = 3000,
                               **_kw):
    """Adds DB-like transaction capability to the SignalKeyStore (contextvars replace AsyncLocalStorage)."""
    # one ContextVar per store (JS: one AsyncLocalStorage per addTransactionCapability call)
    _tx_ctx: contextvars.ContextVar = contextvars.ContextVar('baileys_tx_ctx', default=None)
    key_queues: dict[str, asyncio.Lock] = {}
    tx_mutexes: dict[str, asyncio.Lock] = {}
    tx_ref_counts: dict[str, int] = {}
    pre_key_manager = PreKeyManager(state, logger)

    def get_queue(key):
        return key_queues.setdefault(key, asyncio.Lock())

    def get_tx_mutex(key):
        if key not in tx_mutexes:
            tx_mutexes[key] = asyncio.Lock()
            tx_ref_counts[key] = 0
        return tx_mutexes[key]

    def release_ref(key):
        count = tx_ref_counts.get(key, 1) - 1
        tx_ref_counts[key] = count
        if count <= 0:
            m = tx_mutexes.get(key)
            if m is not None and not m.locked():
                tx_mutexes.pop(key, None)
                tx_ref_counts.pop(key, None)

    async def commit_with_retry(mutations: dict):
        if not mutations:
            logger.trace('no mutations in transaction')
            return
        logger.trace('committing transaction')
        for attempt in range(max_commit_retries):
            try:
                await maybe_await(state.set(mutations))
                logger.trace({'mutationCount': len(mutations)}, 'committed transaction')
                return
            except Exception:
                retries_left = max_commit_retries - attempt - 1
                logger.warn(f'failed to commit mutations, retries left={retries_left}')
                if retries_left == 0:
                    raise
                await delay(delay_between_tries_ms)

    class _TxStore:
        def is_in_transaction(self) -> bool:
            return _tx_ctx.get() is not None

        async def get(self, type_, ids):
            ctx = _tx_ctx.get()
            if ctx is None:
                return await maybe_await(state.get(type_, ids))
            cached = ctx['cache'].get(type_, {})
            missing = [i for i in ids if i not in cached]
            if missing:
                ctx['dbQueries'] += 1
                logger.trace({'type': type_, 'count': len(missing)}, 'fetching missing keys in transaction')
                async with get_tx_mutex(type_):
                    fetched = await maybe_await(state.get(type_, missing))
                ctx['cache'].setdefault(type_, {}).update(fetched)
            result = {}
            for i in ids:
                v = ctx['cache'].get(type_, {}).get(i)
                if v is not None:
                    result[i] = v
            return result

        async def set(self, data):
            ctx = _tx_ctx.get()
            if ctx is None:
                for type_ in list(data.keys()):
                    if type_ == 'pre-key':
                        await pre_key_manager.validate_deletions(data, type_)

                async def _write(type_):
                    async with get_queue(type_):
                        await maybe_await(state.set({type_: data[type_]}))
                await asyncio.gather(*[_write(t) for t in list(data.keys())])
                return
            logger.trace({'types': list(data.keys())}, 'caching in transaction')
            for key, entries in data.items():
                ctx['cache'].setdefault(key, {})
                ctx['mutations'].setdefault(key, {})
                if key == 'pre-key':
                    await pre_key_manager.process_operations(data, key, ctx['cache'], ctx['mutations'], True)
                else:
                    ctx['cache'][key].update(entries)
                    ctx['mutations'][key].update(entries)

        async def clear(self):
            if hasattr(state, 'clear'):
                await maybe_await(state.clear())

        async def transaction(self, work, key: str):
            existing = _tx_ctx.get()
            if existing is not None:
                logger.trace('reusing existing transaction context')
                return await maybe_await(work())
            mutex = get_tx_mutex(key)
            tx_ref_counts[key] = tx_ref_counts.get(key, 0) + 1
            try:
                async with mutex:
                    ctx = {'cache': {}, 'mutations': {}, 'dbQueries': 0}
                    logger.trace('entering transaction')
                    token = _tx_ctx.set(ctx)
                    try:
                        result = await maybe_await(work())
                    except Exception as error:
                        logger.error({'error': error}, 'transaction failed, rolling back')
                        raise
                    finally:
                        _tx_ctx.reset(token)
                    await commit_with_retry(ctx['mutations'])
                    logger.trace({'dbQueries': ctx['dbQueries']}, 'transaction completed')
                    return result
            finally:
                release_ref(key)

        # JS-style aliases
        isInTransaction = is_in_transaction
    return _TxStore()


def assert_me_id(creds: dict) -> str:
    me = (creds.get('me') or {}).get('id')
    if not me:
        raise Boom('Cannot proceed: socket is not authenticated yet (creds.me.id is missing)', status_code=401)
    return me


def init_auth_creds() -> dict:
    identity_key = Curve.generate_key_pair()
    return {
        'noiseKey': Curve.generate_key_pair(),
        'pairingEphemeralKeyPair': Curve.generate_key_pair(),
        'signedIdentityKey': identity_key,
        'signedPreKey': signed_key_pair(identity_key, 1),
        'registrationId': generate_registration_id(),
        'advSecretKey': base64.b64encode(os.urandom(32)).decode(),
        'processedHistoryMessages': [],
        'nextPreKeyId': 1,
        'firstUnuploadedPreKeyId': 1,
        'accountSyncCounter': 0,
        'accountSettings': {'unarchiveChats': False},
        'registered': False,
        'pairingCode': None,
        'lastPropHash': None,
        'routingInfo': None,
        'additionalData': None,
    }
