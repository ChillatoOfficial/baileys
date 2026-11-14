from __future__ import annotations
from typing import Callable, Optional
from ..WABinary import are_jids_same_user, get_binary_node_child, jid_decode
from .generics import is_string_null_or_empty


async def handle_identity_change(node: dict, ctx: dict) -> dict:
    """ctx: meId, meLid, validateSession, assertSessions, debounceCache, logger, onBeforeSessionRefresh?"""
    logger = ctx['logger']
    from_ = node['attrs'].get('from')
    if not from_:
        return {'action': 'invalid_notification'}
    identity_node = get_binary_node_child(node, 'identity')
    if not identity_node:
        return {'action': 'no_identity_node'}
    logger.info({'jid': from_}, 'identity changed')
    decoded = jid_decode(from_)
    if decoded and decoded.device and decoded.device != 0:
        logger.debug({'jid': from_, 'device': decoded.device}, 'ignoring identity change from companion device')
        return {'action': 'skipped_companion_device', 'device': decoded.device}
    me_id, me_lid = ctx.get('meId'), ctx.get('meLid')
    is_self_primary = me_id and (are_jids_same_user(from_, me_id) or (me_lid and are_jids_same_user(from_, me_lid)))
    if is_self_primary:
        logger.info({'jid': from_}, 'self primary identity changed')
        return {'action': 'skipped_self_primary'}
    if ctx['debounceCache'].get(from_):
        logger.debug({'jid': from_}, 'skipping identity assert (debounced)')
        return {'action': 'debounced'}
    ctx['debounceCache'].set(from_, True)
    is_offline_notification = not is_string_null_or_empty(node['attrs'].get('offline'))
    has_existing_session = await ctx['validateSession'](from_)
    if not has_existing_session.get('exists'):
        logger.debug({'jid': from_}, 'no old session, skipping session refresh')
        return {'action': 'skipped_no_session'}
    logger.debug({'jid': from_}, 'old session exists, will refresh session')
    if is_offline_notification:
        logger.debug({'jid': from_}, 'skipping session refresh during offline processing')
        return {'action': 'skipped_offline'}
    if ctx.get('onBeforeSessionRefresh'):
        ctx['onBeforeSessionRefresh'](from_)
    try:
        await ctx['assertSessions']([from_], True)
        return {'action': 'session_refreshed'}
    except Exception as error:
        logger.warn({'error': error, 'jid': from_}, 'failed to assert sessions after identity change')
        return {'action': 'session_refresh_failed', 'error': error}
