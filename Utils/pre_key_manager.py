from __future__ import annotations
import asyncio


class PreKeyManager:
    def __init__(self, store, logger):
        self.store = store
        self.logger = logger
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, key_type: str) -> asyncio.Lock:
        return self._locks.setdefault(key_type, asyncio.Lock())

    async def process_operations(self, data: dict, key_type: str, transaction_cache: dict, mutations: dict,
                                 is_in_transaction: bool):
        key_data = data.get(key_type)
        if not key_data:
            return
        async with self._lock(key_type):
            transaction_cache.setdefault(key_type, {})
            mutations.setdefault(key_type, {})
            deletions, updates = [], {}
            for key_id, v in key_data.items():
                if v is None:
                    deletions.append(key_id)
                else:
                    updates[key_id] = v
            if updates:
                transaction_cache[key_type].update(updates)
                mutations[key_type].update(updates)
            if deletions:
                await self._process_deletions(key_type, deletions, transaction_cache, mutations, is_in_transaction)

    async def _process_deletions(self, key_type, ids, transaction_cache, mutations, is_in_transaction):
        if is_in_transaction:
            for key_id in ids:
                if transaction_cache.get(key_type, {}).get(key_id):
                    transaction_cache[key_type][key_id] = None
                    mutations[key_type][key_id] = None
                else:
                    self.logger.warn(f'Skipping deletion of non-existent {key_type} in transaction: {key_id}')
        else:
            existing = await _maybe_await(self.store.get(key_type, ids))
            for key_id in ids:
                if existing.get(key_id):
                    transaction_cache[key_type][key_id] = None
                    mutations[key_type][key_id] = None
                else:
                    self.logger.warn(f'Skipping deletion of non-existent {key_type}: {key_id}')

    async def validate_deletions(self, data: dict, key_type: str):
        key_data = data.get(key_type)
        if not key_data:
            return
        async with self._lock(key_type):
            deletion_ids = [i for i, v in key_data.items() if v is None]
            if not deletion_ids:
                return
            existing = await _maybe_await(self.store.get(key_type, deletion_ids))
            for key_id in deletion_ids:
                if not existing.get(key_id):
                    self.logger.warn(f'Skipping deletion of non-existent {key_type}: {key_id}')
                    del data[key_type][key_id]


async def _maybe_await(x):
    if asyncio.iscoroutine(x) or isinstance(x, asyncio.Future):
        return await x
    return x
