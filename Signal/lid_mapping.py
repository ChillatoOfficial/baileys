from __future__ import annotations
import asyncio
from typing import Optional
from ..Utils.cache import TTLCache
from ..WABinary import (is_hosted_pn_user, is_lid_user, is_pn_user, jid_decode, jid_normalized_user, WAJIDDomains)


class LIDMappingStore:
    def __init__(self, keys, logger, pn_to_lid_func=None):
        self.mapping_cache = TTLCache(ttl=3 * 24 * 60 * 60, update_age_on_get=True)
        self.keys = keys
        self.logger = logger
        self.pn_to_lid_func = pn_to_lid_func
        self._inflight_lid: dict[str, asyncio.Future] = {}
        self._inflight_pn: dict[str, asyncio.Future] = {}

    async def store_lid_pn_mappings(self, pairs: list):
        if not pairs:
            return
        validated = []
        for p in pairs:
            lid, pn = p['lid'], p['pn']
            if not ((is_lid_user(lid) and is_pn_user(pn)) or (is_pn_user(lid) and is_lid_user(pn))):
                self.logger.warn(f'Invalid LID-PN mapping: {lid}, {pn}')
                continue
            lid_d, pn_d = jid_decode(lid), jid_decode(pn)
            if not lid_d or not pn_d:
                continue
            validated.append({'pnUser': pn_d.user, 'lidUser': lid_d.user})
        if not validated:
            return
        cache_miss = []
        existing = {}
        for v in validated:
            cached = self.mapping_cache.get(f"pn:{v['pnUser']}")
            if cached:
                existing[v['pnUser']] = cached
            elif v['pnUser'] not in cache_miss:
                cache_miss.append(v['pnUser'])
        if cache_miss:
            self.logger.trace(f'Batch fetching {len(cache_miss)} LID mappings from database')
            stored = await self.keys.get('lid-mapping', cache_miss)
            for pn_user in cache_miss:
                lid_user = stored.get(pn_user)
                if lid_user:
                    existing[pn_user] = lid_user
                    self.mapping_cache.set(f'pn:{pn_user}', lid_user)
                    self.mapping_cache.set(f'lid:{lid_user}', pn_user)
        pair_map = {}
        for v in validated:
            if existing.get(v['pnUser']) == v['lidUser']:
                self.logger.debug({'pnUser': v['pnUser'], 'lidUser': v['lidUser']}, 'LID mapping already exists, skipping')
                continue
            pair_map[v['pnUser']] = v['lidUser']
        if not pair_map:
            return
        self.logger.trace({'pairMap': pair_map}, f'Storing {len(pair_map)} pn mappings')
        batch = {}
        for pn_user, lid_user in pair_map.items():
            batch[pn_user] = lid_user
            batch[f'{lid_user}_reverse'] = pn_user

        async def _store():
            await self.keys.set({'lid-mapping': batch})
        await self.keys.transaction(_store, 'lid-mapping')
        for pn_user, lid_user in pair_map.items():
            self.mapping_cache.set(f'pn:{pn_user}', lid_user)
            self.mapping_cache.set(f'lid:{lid_user}', pn_user)

    async def get_lid_for_pn(self, pn: str) -> Optional[str]:
        r = await self.get_lids_for_pns([pn])
        return r[0]['lid'] if r else None

    async def get_lids_for_pns(self, pns: list):
        if not pns:
            return None
        cache_key = ','.join(sorted(set(pns)))
        inflight = self._inflight_lid.get(cache_key)
        if inflight:
            self.logger.trace(f'Coalescing getLIDsForPNs request for {len(set(pns))} PNs')
            return await inflight
        fut = asyncio.ensure_future(self._get_lids_for_pns_impl(pns))
        self._inflight_lid[cache_key] = fut
        try:
            return await fut
        finally:
            self._inflight_lid.pop(cache_key, None)

    async def _get_lids_for_pns_impl(self, pns: list):
        usync_fetch: dict[str, list] = {}
        successful: dict[str, dict] = {}
        pending = []

        def add_resolved(pn, decoded, lid_user) -> bool:
            norm = str(lid_user)
            if not norm:
                self.logger.warn(f'Invalid or empty LID user for PN {pn}: lidUser = "{lid_user}"')
                return False
            pn_device = decoded.device if decoded.device is not None else 0
            server = 'hosted.lid' if decoded.server == 'hosted' else 'lid'
            lid = f"{norm}{f':{pn_device}' if pn_device else ''}@{server}"
            self.logger.trace(f'getLIDForPN: {pn} → {lid} (user mapping with device {pn_device})')
            successful[pn] = {'lid': lid, 'pn': pn}
            return True

        for pn in pns:
            if not is_pn_user(pn) and not is_hosted_pn_user(pn):
                continue
            decoded = jid_decode(pn)
            if not decoded:
                continue
            cached = self.mapping_cache.get(f'pn:{decoded.user}')
            if cached and isinstance(cached, str):
                if not add_resolved(pn, decoded, cached):
                    self.logger.warn(f'Invalid entry for {pn} (pair not resolved)')
                continue
            pending.append((pn, decoded.user, decoded))

        if pending:
            pn_users = list(dict.fromkeys(p[1] for p in pending))
            stored = await self.keys.get('lid-mapping', pn_users)
            for pn_user in pn_users:
                lid_user = stored.get(pn_user)
                if lid_user and isinstance(lid_user, str):
                    self.mapping_cache.set(f'pn:{pn_user}', lid_user)
                    self.mapping_cache.set(f'lid:{lid_user}', pn_user)
            for pn, pn_user, decoded in pending:
                cached = self.mapping_cache.get(f'pn:{pn_user}')
                if cached and isinstance(cached, str):
                    if not add_resolved(pn, decoded, cached):
                        self.logger.warn(f'Invalid entry for {pn} (pair not resolved)')
                else:
                    self.logger.trace(f'No LID mapping found for PN user {pn_user}; batch getting from USync')
                    device = decoded.device or 0
                    normalized = jid_normalized_user(pn)
                    if is_hosted_pn_user(normalized):
                        normalized = f'{pn_user}@s.whatsapp.net'
                    usync_fetch.setdefault(normalized, []).append(device)

        if usync_fetch:
            result = await self.pn_to_lid_func(list(usync_fetch.keys())) if self.pn_to_lid_func else None
            if result:
                await self.store_lid_pn_mappings(result)
                for pair in result:
                    pn_decoded = jid_decode(pair['pn'])
                    pn_user = pn_decoded.user if pn_decoded else None
                    if not pn_user:
                        continue
                    lid_d = jid_decode(pair['lid'])
                    lid_user = lid_d.user if lid_d else None
                    if not lid_user:
                        continue
                    for device in usync_fetch.get(pair['pn'], []):
                        lid = f"{lid_user}{f':{device}' if device else ''}@{'hosted.lid' if device == 99 else 'lid'}"
                        self.logger.trace(f'getLIDForPN: USYNC success for {pair["pn"]} → {lid}')
                        pnj = f"{pn_user}{f':{device}' if device else ''}@{'hosted' if device == 99 else 's.whatsapp.net'}"
                        successful[pnj] = {'lid': lid, 'pn': pnj}
            else:
                self.logger.warn('USync fetch yielded no results for pending PNs')
        return list(successful.values()) if successful else None

    async def get_pn_for_lid(self, lid: str) -> Optional[str]:
        r = await self.get_pns_for_lids([lid])
        return r[0]['pn'] if r else None

    async def get_pns_for_lids(self, lids: list):
        if not lids:
            return None
        cache_key = ','.join(sorted(set(lids)))
        inflight = self._inflight_pn.get(cache_key)
        if inflight:
            return await inflight
        fut = asyncio.ensure_future(self._get_pns_for_lids_impl(lids))
        self._inflight_pn[cache_key] = fut
        try:
            return await fut
        finally:
            self._inflight_pn.pop(cache_key, None)

    async def _get_pns_for_lids_impl(self, lids: list):
        successful: dict[str, dict] = {}
        pending = []

        def add_resolved(lid, decoded, pn_user) -> bool:
            if not pn_user or not isinstance(pn_user, str):
                return False
            lid_device = decoded.device if decoded.device is not None else 0
            server = 'hosted' if decoded.domainType == WAJIDDomains.HOSTED_LID else 's.whatsapp.net'
            pn_jid = f'{pn_user}:{lid_device}@{server}'
            self.logger.trace(f'Found reverse mapping: {lid} → {pn_jid}')
            successful[lid] = {'lid': lid, 'pn': pn_jid}
            return True

        for lid in lids:
            if not is_lid_user(lid):
                continue
            decoded = jid_decode(lid)
            if not decoded:
                continue
            cached = self.mapping_cache.get(f'lid:{decoded.user}')
            if cached and isinstance(cached, str):
                add_resolved(lid, decoded, cached)
                continue
            pending.append((lid, decoded.user, decoded))

        if pending:
            reverse_keys = list(dict.fromkeys(f'{p[1]}_reverse' for p in pending))
            stored = await self.keys.get('lid-mapping', reverse_keys)
            for lid, lid_user, decoded in pending:
                pn_user = self.mapping_cache.get(f'lid:{lid_user}')
                if not pn_user or not isinstance(pn_user, str):
                    pn_user = stored.get(f'{lid_user}_reverse')
                    if pn_user and isinstance(pn_user, str):
                        self.mapping_cache.set(f'lid:{lid_user}', pn_user)
                        self.mapping_cache.set(f'pn:{pn_user}', lid_user)
                if pn_user and isinstance(pn_user, str):
                    add_resolved(lid, decoded, pn_user)
                else:
                    self.logger.trace(f'No reverse mapping found for LID user: {lid_user}')
        return list(successful.values()) if successful else None

    def close(self):
        self.mapping_cache.clear()
