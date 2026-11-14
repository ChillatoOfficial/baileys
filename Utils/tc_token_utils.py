from __future__ import annotations
import json
import re
import time
from typing import Callable, Optional
from ..WABinary import (get_binary_node_child, get_binary_node_children, is_hosted_lid_user, is_hosted_pn_user,
                        is_jid_meta_ai, is_lid_user, is_pn_user, jid_normalized_user)

# Same phone-number pattern as WABinary's isJidBot, applied against the user part
_BOT_PHONE_REGEX = re.compile(r'^1313555\d{4}$|^131655500\d{2}$')


def _is_regular_user(jid: Optional[str]) -> bool:
    """Mirrors WA Web's Wid.isRegularUser() (user ∧ ¬PSA ∧ ¬Bot)."""
    if not jid:
        return False
    user = jid.split('@')[0]
    if user == '0':
        return False  # PSA
    if _BOT_PHONE_REGEX.match(user):
        return False  # Bot by phone pattern
    if is_jid_meta_ai(jid):
        return False
    return bool(is_pn_user(jid) or is_lid_user(jid) or is_hosted_pn_user(jid) or is_hosted_lid_user(jid)
                or jid.endswith('@c.us'))


TC_TOKEN_BUCKET_DURATION = 604800  # 7 days
TC_TOKEN_NUM_BUCKETS = 4  # ~28-day rolling window
TC_TOKEN_INDEX_KEY = '__index'


async def read_tc_token_index(keys) -> list:
    data = await keys.get('tctoken', [TC_TOKEN_INDEX_KEY])
    entry = data.get(TC_TOKEN_INDEX_KEY)
    if not entry or not entry.get('token'):
        return []
    try:
        parsed = json.loads(bytes(entry['token']).decode())
        if not isinstance(parsed, list):
            return []
        return [j for j in parsed if isinstance(j, str) and len(j) > 0 and j != TC_TOKEN_INDEX_KEY]
    except Exception:
        return []


async def build_merged_tc_token_index_write(keys, added_jids) -> dict:
    persisted = await read_tc_token_index(keys)
    merged = dict.fromkeys(persisted)
    for jid in added_jids:
        if jid and jid != TC_TOKEN_INDEX_KEY:
            merged[jid] = None
    return {TC_TOKEN_INDEX_KEY: {'token': json.dumps(list(merged)).encode()}}


def is_tc_token_expired(timestamp) -> bool:
    if timestamp is None:
        return True
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return True
    now = int(time.time())
    current_bucket = now // TC_TOKEN_BUCKET_DURATION
    cutoff_bucket = current_bucket - (TC_TOKEN_NUM_BUCKETS - 1)
    return ts < cutoff_bucket * TC_TOKEN_BUCKET_DURATION


def should_send_new_tc_token(sender_timestamp: Optional[int]) -> bool:
    if sender_timestamp is None:
        return True
    now = int(time.time())
    return now // TC_TOKEN_BUCKET_DURATION > sender_timestamp // TC_TOKEN_BUCKET_DURATION


async def resolve_tc_token_jid(jid: str, get_lid_for_pn: Callable) -> str:
    """Resolve JID to LID for tctoken storage (WA Web stores under LID)"""
    if is_lid_user(jid):
        return jid
    lid = await get_lid_for_pn(jid)
    return lid or jid


async def resolve_issuance_jid(jid: str, issue_to_lid: bool, get_lid_for_pn: Callable,
                               get_pn_for_lid: Optional[Callable] = None) -> str:
    if issue_to_lid:
        if is_lid_user(jid):
            return jid
        return (await get_lid_for_pn(jid)) or jid
    if not is_lid_user(jid):
        return jid
    if get_pn_for_lid:
        return (await get_pn_for_lid(jid)) or jid
    return jid


async def build_tc_token_from_jid(auth_state: dict, jid: str, get_lid_for_pn: Callable,
                                  base_content: Optional[list] = None) -> Optional[list]:
    base_content = [] if base_content is None else base_content
    try:
        storage_jid = await resolve_tc_token_jid(jid, get_lid_for_pn)
        tc = await auth_state['keys'].get('tctoken', [storage_jid])
        entry = (tc or {}).get(storage_jid)
        token_buf = entry.get('token') if entry else None
        timestamp = entry.get('timestamp') if entry else None
        if not token_buf or timestamp is None or is_tc_token_expired(timestamp):
            if token_buf is not None:
                # Preserve senderTimestamp so should_send_new_tc_token() keeps its dedupe state
                cleared = {'token': b'', 'senderTimestamp': entry['senderTimestamp']} \
                    if entry.get('senderTimestamp') is not None else None
                await auth_state['keys'].set({'tctoken': {storage_jid: cleared}})
            return base_content if len(base_content) > 0 else None
        base_content.append({'tag': 'tctoken', 'attrs': {'t': str(timestamp)}, 'content': token_buf})
        return base_content
    except Exception:
        return base_content if len(base_content) > 0 else None


async def store_tc_tokens_from_iq_result(result: dict, fallback_jid: str, keys, get_lid_for_pn: Callable,
                                         on_new_jid_stored: Optional[Callable] = None):
    tokens_node = get_binary_node_child(result, 'tokens')
    if not tokens_node:
        return
    for token_node in get_binary_node_children(tokens_node, 'token'):
        if token_node['attrs'].get('type') != 'trusted_contact' or not isinstance(token_node.get('content'),
                                                                                  (bytes, bytearray)):
            continue
        # In notifications tokenNode.attrs.jid is your own device JID, not the sender's
        raw_jid = jid_normalized_user(fallback_jid or token_node['attrs'].get('jid'))
        if not _is_regular_user(raw_jid):
            continue
        storage_jid = await resolve_tc_token_jid(raw_jid, get_lid_for_pn)
        existing_entry = (await keys.get('tctoken', [storage_jid])).get(storage_jid)
        existing_ts = int(existing_entry['timestamp']) if existing_entry and existing_entry.get('timestamp') else 0
        incoming_ts = int(token_node['attrs']['t']) if token_node['attrs'].get('t') else 0
        if not incoming_ts:
            continue  # timestamp-less tokens would be immediately expired
        if existing_ts > 0 and existing_ts > incoming_ts:
            continue
        await keys.set({'tctoken': {storage_jid: {**(existing_entry or {}), 'token': bytes(token_node['content']),
                                                  'timestamp': token_node['attrs'].get('t')}}})
        if on_new_jid_stored:
            on_new_jid_stored(storage_jid)
