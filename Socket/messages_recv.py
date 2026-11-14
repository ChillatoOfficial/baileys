from __future__ import annotations
import asyncio
import json
import time
from datetime import datetime
from typing import Optional

from ..Defaults import (DEFAULT_CACHE_TTLS, KEY_BUNDLE_TYPE, MIN_PREKEY_COUNT, PLACEHOLDER_MAX_AGE_SECONDS,
                        STATUS_EXPIRY_SECONDS)
from ..Types import ReachoutTimelockEnforcementType, WAMessageStubType
from ..Utils.cache import TTLCache
import os
from ..Utils.crypto import (Curve, aes_decrypt_ctr, aes_encrypt_gcm, derive_pairing_code_key, generate_signal_pub_key,
                            hkdf)
from ..Utils.decode_wa_message import (ACCOUNT_RESTRICTED_TEXT, decode_message_node, decrypt_message_node,
                                       MISSING_KEYS_ERROR_TEXT, NACK_REASONS, NO_MESSAGE_FOUND_ERROR_TEXT,
                                       SERVER_ERROR_CODES)
from ..Utils.generics import (delay, encode_big_endian, generate_message_id_v2, get_call_status_from_node,
                              get_key_author, get_status_from_receipt_type, to_number, unix_timestamp_seconds)
from ..Utils.history import get_history_msg
from ..Utils.identity_change_handler import handle_identity_change
from ..Utils.make_mutex import make_mutex
from ..Utils.message_retry_manager import RetryReason
from ..Utils.messages import get_content_type, normalize_message_content, extract_message_content
from ..Utils.messages_media import decode_media_retry_node
from ..Utils.offline_node_processor import make_offline_node_processor
from ..Utils.process_message import clean_message, get_chat_id, is_real_message, should_increment_chat_unread
from ..Utils.proto_utils import from_object, normalize, to_dict
from ..Utils.decode_wa_message import extract_addressing_context
from ..Utils.signal import extract_e2e_session_from_retry_receipt, xmpp_pre_key, xmpp_signed_pre_key
from ..Utils.stanza_ack import build_ack_stanza
from ..Utils.signal import get_next_pre_keys
from ..Utils.tc_token_utils import (build_merged_tc_token_index_write, read_tc_token_index, TC_TOKEN_INDEX_KEY, is_tc_token_expired, resolve_issuance_jid,
                                    resolve_tc_token_jid, store_tc_tokens_from_iq_result)
from ..Utils.validate_connection import encode_signed_device_identity
from ..WABinary import (Boom, are_jids_same_user, binary_node_to_string, get_all_binary_node_children,
                        get_binary_node_child, get_binary_node_child_buffer, get_binary_node_child_string, get_binary_node_child_uint,
                        get_binary_node_children, is_jid_group, is_jid_newsletter, is_jid_status_broadcast,
                        is_lid_user, is_pn_user, jid_decode, jid_encode, jid_normalized_user, S_WHATSAPP_NET)
from ..WABinary.jid_utils import FullJid
from ..WAProto import WAProto_pb2 as proto
from .groups import extract_group_metadata
from .messages_send import make_messages_socket

_ENFORCEMENT_TYPE_VALUES = {e.value for e in ReachoutTimelockEnforcementType}


def _str(content) -> str:
    """Buffer.toString()"""
    if content is None:
        return ''
    if isinstance(content, (bytes, bytearray)):
        return bytes(content).decode('utf-8', errors='replace')
    return str(content)


def _enc_type(t):
    return t.value if hasattr(t, 'value') else t


def make_messages_recv_socket(config: dict):
    logger = config['logger']
    retry_request_delay_ms = config['retryRequestDelayMs']
    max_msg_retry_count = config['maxMsgRetryCount']
    get_message = config.get('getMessage')
    should_ignore_jid = config['shouldIgnoreJid']
    enable_auto_session_recreation = config['enableAutoSessionRecreation']

    sock = make_messages_socket(config)
    user_devices_cache, devices_mutex = sock.user_devices_cache, sock.devices_mutex
    ev, auth_state, ws = sock.ev, sock.auth_state, sock.ws
    message_mutex, notification_mutex, receipt_mutex = sock.message_mutex, sock.notification_mutex, sock.receipt_mutex
    signal_repository, query, upsert_message = sock.signal_repository, sock.query, sock.upsert_message
    resync_app_state, on_unexpected_error = sock.resync_app_state, sock.on_unexpected_error
    assert_sessions, send_node, relay_message = sock.assert_sessions, sock.send_node, sock.relay_message
    send_receipt, upload_pre_keys = sock.send_receipt, sock.upload_pre_keys
    send_peer_data_operation_message = sock.send_peer_data_operation_message
    message_retry_manager = sock.message_retry_manager
    register_socket_end_handler = sock.register_socket_end_handler
    issue_privacy_tokens = sock.issue_privacy_tokens
    fetch_account_reachout_timelock = sock.fetch_account_reachout_timelock
    placeholder_resend_cache = sock.placeholder_resend_cache

    get_lid_for_pn = signal_repository.lid_mapping.get_lid_for_pn

    retry_mutex = make_mutex()  # ensures that each retryRequest will wait for the previous one to finish

    msg_retry_cache = config.get('msgRetryCounterCache') or TTLCache(ttl=DEFAULT_CACHE_TTLS['MSG_RETRY'])
    call_offer_cache = config.get('callOfferCache') or TTLCache(ttl=DEFAULT_CACHE_TTLS['CALL_OFFER'])
    # Debounce identity-change session refreshes per JID to avoid bursts
    identity_assert_debounce = TTLCache(ttl=5)
    st = {'sendActiveReceipts': False}

    async def _maybe(x):
        return await x if hasattr(x, '__await__') else x

    async def fetch_message_history(count: int, oldest_msg_key: dict, oldest_msg_timestamp) -> str:
        if not (auth_state['creds'].get('me') or {}).get('id'):
            raise Boom('Not authenticated')
        pdo_message = {
            'historySyncOnDemandRequest': {
                'chatJid': oldest_msg_key.get('remoteJid'), 'oldestMsgFromMe': oldest_msg_key.get('fromMe'),
                'oldestMsgId': oldest_msg_key.get('id'), 'oldestMsgTimestampMs': oldest_msg_timestamp,
                'onDemandMsgCount': count},
            'peerDataOperationRequestType': proto.Message.HISTORY_SYNC_ON_DEMAND}
        return await send_peer_data_operation_message(pdo_message)

    async def request_placeholder_resend(message_key: dict, msg_data: Optional[dict] = None) -> Optional[str]:
        if not (auth_state['creds'].get('me') or {}).get('id'):
            raise Boom('Not authenticated')
        if await _maybe(placeholder_resend_cache.get(message_key.get('id'))):
            logger.debug({'messageKey': message_key}, 'already requested resend')
            return None
        # Store original message data so PDO response handler can preserve metadata (LID details, timestamps, etc.)
        await _maybe(placeholder_resend_cache.set(message_key.get('id'), msg_data or True))
        await delay(2000)
        if not await _maybe(placeholder_resend_cache.get(message_key.get('id'))):
            logger.debug({'messageKey': message_key}, 'message received while resend requested')
            return 'RESOLVED'
        pdo_message = {'placeholderMessageResendRequest': [{'messageKey': message_key}],
                       'peerDataOperationRequestType': proto.Message.PLACEHOLDER_MESSAGE_RESEND}

        async def check_no_response():
            await asyncio.sleep(8)
            if await _maybe(placeholder_resend_cache.get(message_key.get('id'))):
                logger.debug({'messageKey': message_key}, 'PDO message without response after 8 seconds. '
                                                           'Phone possibly offline')
                r = placeholder_resend_cache.delete(message_key.get('id')) if hasattr(placeholder_resend_cache, 'delete') \
                    else placeholder_resend_cache.del_(message_key.get('id'))
                await _maybe(r)
        asyncio.ensure_future(check_no_response())
        return await send_peer_data_operation_message(pdo_message)

    async def handle_mex_notification(node: dict):
        update_node = get_binary_node_child(node, 'update')
        if update_node:
            op_name = (update_node.get('attrs') or {}).get('op_name')
            if not op_name:
                logger.warn({'node': binary_node_to_string(node)}, 'mex notification missing op_name, fallback to legacy')
                await handle_legacy_mex_newsletter_notification(node)
                return
            try:
                mex_response = json.loads(_str(update_node.get('content')))
            except Exception as error:
                logger.error({'err': error, 'opName': op_name}, 'failed to parse mex notification JSON')
                return
            if mex_response.get('errors'):
                logger.warn({'errors': mex_response['errors'], 'opName': op_name}, 'mex notification has GQL errors')
                return
            data = mex_response.get('data')
            if not data:
                logger.warn({'opName': op_name}, 'mex notification has null data')
                return
            logger.debug({'opName': op_name}, 'processing mex notification')
            if op_name == 'NotificationUserReachoutTimelockUpdate':
                handle_reachout_timelock_notification(data)
            elif op_name == 'MessageCappingInfoNotification':
                handle_message_capping_notification(data)
            elif op_name in ('NotificationNewsletterUpdate', 'NotificationLinkedProfilesUpdates',
                             'NotificationNewsletterAdminPromote', 'NotificationNewsletterAdminDemote',
                             'NotificationNewsletterUserSettingChange', 'NotificationNewsletterJoin',
                             'NotificationNewsletterLeave', 'NotificationNewsletterStateChange',
                             'NotificationNewsletterAdminMetadataUpdate', 'NotificationNewsletterOwnerUpdate',
                             'NotificationNewsletterAdminInviteRevoke', 'NotificationNewsletterWamoSubStatusChange',
                             'NotificationNewsletterBlockUser', 'NotificationNewsletterPaidPartnership',
                             'NotificationNewsletterMilestone', 'NewsletterResponseStateUpdate'):
                # newsletter ops still use the legacy <mex> child structure
                await handle_legacy_mex_newsletter_notification(node)
            else:
                logger.debug({'opName': op_name}, 'unhandled mex notification')
            return
        await handle_legacy_mex_newsletter_notification(node)

    def handle_reachout_timelock_notification(data: dict):
        payload = data.get('xwa2_notify_account_reachout_timelock')
        if not payload:
            logger.warn('reachout timelock notification missing payload')
            return
        if not payload.get('is_active'):
            logger.info('reachout timelock restriction lifted')
            ev.emit('connection.update', {'reachoutTimeLock': {
                'isActive': False, 'enforcementType': ReachoutTimelockEnforcementType.DEFAULT}})
            return
        # WA Web defaults to now+60s when the server omits the expiry
        time_enforcement_ends = datetime.fromtimestamp(int(payload['time_enforcement_ends'])) \
            if payload.get('time_enforcement_ends') else datetime.fromtimestamp(time.time() + 60)
        et = payload.get('enforcement_type')
        enforcement_type = ReachoutTimelockEnforcementType(et) if et in _ENFORCEMENT_TYPE_VALUES \
            else ReachoutTimelockEnforcementType.DEFAULT
        logger.info({'enforcementType': enforcement_type, 'timeEnforcementEnds': time_enforcement_ends},
                    'reachout timelock restriction set')
        ev.emit('connection.update', {'reachoutTimeLock': {
            'isActive': True, 'timeEnforcementEnds': time_enforcement_ends, 'enforcementType': enforcement_type}})

    def handle_message_capping_notification(data: dict):
        payload = data.get('xwa2_notify_new_chat_messages_capping_info_update')
        if not payload:
            logger.warn('message capping notification missing payload')
            return
        logger.info({'payload': payload}, 'received message capping update')
        ev.emit('message-capping.update', payload)

    async def handle_legacy_mex_newsletter_notification(node: dict):
        mex_node = get_binary_node_child(node, 'mex')
        update_node = None if (mex_node and mex_node.get('content')) else (
            get_binary_node_child(node, 'update') or (get_all_binary_node_children(node) or [None])[0])
        payload_node = mex_node if (mex_node and mex_node.get('content')) else update_node
        if not payload_node or not payload_node.get('content'):
            logger.warn({'node': binary_node_to_string(node)}, 'invalid mex newsletter notification')
            return
        try:
            payload_content = payload_node['content']
            if isinstance(payload_content, list):
                logger.warn({'payloadNode': payload_node}, 'invalid mex newsletter notification payload format')
                return
            data = json.loads(_str(payload_content))
        except Exception as error:
            logger.error({'err': error, 'node': binary_node_to_string(node)},
                         'failed to parse mex newsletter notification')
            return
        operation = (data or {}).get('operation') or (payload_node.get('attrs') or {}).get('op_name')
        updates = (data or {}).get('updates')
        if not updates:
            linked_profiles = ((data or {}).get('data') or {}).get('xwa2_notify_linked_profiles')
            if linked_profiles:
                updates = [linked_profiles]
        if not updates or not operation:
            logger.warn({'data': data}, 'invalid mex newsletter notification content')
            return
        logger.info({'operation': operation, 'updates': updates}, 'got mex newsletter notification')
        if operation == 'NotificationNewsletterUpdate':
            for update in updates:
                if update.get('jid') and update.get('settings') and len(update['settings']) > 0:
                    ev.emit('newsletter-settings.update', {'id': update['jid'], 'update': update['settings']})
        elif operation == 'NotificationNewsletterAdminPromote':
            for update in updates:
                if update.get('jid') and update.get('user'):
                    ev.emit('newsletter-participants.update', {
                        'id': update['jid'], 'author': node['attrs'].get('from'), 'user': update['user'],
                        'new_role': 'ADMIN', 'action': 'promote'})
        elif operation == 'NotificationLinkedProfilesUpdates':
            for update in updates:
                lid = (update or {}).get('jid')
                added_profiles = update.get('added_profiles') if isinstance((update or {}).get('added_profiles'), list) \
                    else []
                mappings = []
                for profile in added_profiles:
                    pn = profile if isinstance(profile, str) else ((profile or {}).get('pn') or
                                                                    (profile or {}).get('jid'))
                    if lid and pn:
                        mapping = {'lid': lid, 'pn': pn}
                        ev.emit('lid-mapping.update', mapping)
                        mappings.append(mapping)
                await signal_repository.lid_mapping.store_lid_pn_mappings(mappings)
        else:
            logger.info({'operation': operation, 'data': data}, 'unhandled mex newsletter notification')

    # Handles newsletter notifications
    async def handle_newsletter_notification(node: dict):
        from_ = node['attrs'].get('from')
        children = get_all_binary_node_children(node)
        author = node['attrs'].get('participant')
        for child in children:
            logger.debug({'from': from_, 'child': child}, 'got newsletter notification')
            tag = child['tag']
            if tag == 'reaction':
                ev.emit('newsletter.reaction', {
                    'id': from_, 'server_id': child['attrs'].get('message_id'),
                    'reaction': {'code': get_binary_node_child_string(child, 'reaction'), 'count': 1}})
            elif tag == 'view':
                ev.emit('newsletter.view', {'id': from_, 'server_id': child['attrs'].get('message_id'),
                                            'count': int(_str(child.get('content')) or '0')})
            elif tag == 'participant':
                ev.emit('newsletter-participants.update', {
                    'id': from_, 'author': author, 'user': child['attrs'].get('jid'),
                    'action': child['attrs'].get('action'), 'new_role': child['attrs'].get('role')})
            elif tag == 'update':
                settings_node = get_binary_node_child(child, 'settings')
                if settings_node:
                    update = {}
                    name_node = get_binary_node_child(settings_node, 'name')
                    if name_node and name_node.get('content'):
                        update['name'] = _str(name_node['content'])
                    description_node = get_binary_node_child(settings_node, 'description')
                    if description_node and description_node.get('content'):
                        update['description'] = _str(description_node['content'])
                    ev.emit('newsletter-settings.update', {'id': from_, 'update': update})
            elif tag == 'message':
                plaintext_node = get_binary_node_child(child, 'plaintext')
                if plaintext_node and plaintext_node.get('content'):
                    try:
                        content = plaintext_node['content']
                        content_buf = content.encode('latin-1') if isinstance(content, str) else bytes(content)
                        m = proto.Message()
                        m.ParseFromString(content_buf)
                        message_proto = to_dict(m)
                        full_message = normalize(proto.WebMessageInfo, {
                            'key': {'remoteJid': from_, 'id': child['attrs'].get('message_id') or
                                    child['attrs'].get('server_id'), 'fromMe': False},
                            'message': message_proto,
                            'messageTimestamp': int(child['attrs'].get('t') or 0)})
                        full_message['key']['server_id'] = child['attrs'].get('server_id')
                        await upsert_message(full_message, 'append')
                        logger.debug('Processed plaintext newsletter message')
                    except Exception as error:
                        logger.error({'error': error}, 'Failed to decode plaintext newsletter message')
            else:
                logger.warn({'node': node}, 'Unknown newsletter notification')

    async def send_message_ack(node: dict, error_code: Optional[int] = None):
        stanza = build_ack_stanza(node, error_code, auth_state['creds']['me']['id'])
        logger.debug({'recv': {'tag': node['tag'], 'attrs': node['attrs']}, 'sent': stanza['attrs']}, 'sent ack')
        await send_node(stanza)

    async def reject_call(call_id: str, call_from: str):
        stanza = {'tag': 'call', 'attrs': {'from': auth_state['creds']['me']['id'], 'to': call_from},
                  'content': [{'tag': 'reject', 'attrs': {'call-id': call_id, 'call-creator': call_from, 'count': '0'},
                               'content': None}]}
        await query(stanza)

    async def send_retry_request(node: dict, force_include_keys: bool = False):
        me = auth_state['creds']['me']
        full_message = decode_message_node(node, me['id'], me.get('lid') or '')['fullMessage']
        msg_key = full_message['key']
        msg_id = msg_key['id']

        if message_retry_manager:
            # Check if we've exceeded max retries using the new system
            if message_retry_manager.has_exceeded_max_retries(msg_id):
                logger.debug({'msgId': msg_id}, 'reached retry limit with new retry manager, clearing')
                message_retry_manager.mark_retry_failed(msg_id)
                return
            # Increment retry count using new system
            retry_count = message_retry_manager.increment_retry_count(msg_id)
            # Use the new retry count for the rest of the logic
            await _maybe(msg_retry_cache.set(f"{msg_id}:{msg_key.get('participant')}", retry_count))
        else:
            # Fallback to old system
            key = f"{msg_id}:{msg_key.get('participant')}"
            retry_count = (await _maybe(msg_retry_cache.get(key))) or 0
            if retry_count >= max_msg_retry_count:
                logger.debug({'retryCount': retry_count, 'msgId': msg_id}, 'reached retry limit, clearing')
                await _maybe(msg_retry_cache.delete(key))
                return
            retry_count += 1
            await _maybe(msg_retry_cache.set(key, retry_count))

        key = f"{msg_id}:{msg_key.get('participant')}"
        retry_count = (await _maybe(msg_retry_cache.get(key))) or 1

        creds = auth_state['creds']
        account, signed_pre_key, identity_key = creds.get('account'), creds['signedPreKey'], creds['signedIdentityKey']
        from_jid = node['attrs']['from']

        # Check if we should recreate the session
        should_recreate_session = False
        recreate_reason = ''
        if enable_auto_session_recreation and message_retry_manager and retry_count > 1:
            try:
                # Check if we have a session with this JID
                session_id = signal_repository.jid_to_signal_protocol_address(from_jid)
                has_session = await signal_repository.validate_session(from_jid)
                result = message_retry_manager.should_recreate_session(from_jid, has_session['exists'])
                should_recreate_session = result['recreate']
                recreate_reason = result['reason']
                if should_recreate_session:
                    logger.debug({'fromJid': from_jid, 'retryCount': retry_count, 'reason': recreate_reason},
                                 'recreating session for retry')
                    # Delete existing session to force recreation
                    await auth_state['keys'].set({'session': {session_id: None}})
                    force_include_keys = True
            except Exception as error:
                logger.warn({'error': error, 'fromJid': from_jid}, 'failed to check session recreation')

        if retry_count <= 2:
            # Use new retry manager for phone requests if available
            if message_retry_manager:
                # Schedule phone request with delay (like whatsmeow)
                async def phone_request():
                    try:
                        request_id = await request_placeholder_resend(msg_key)
                        logger.debug(f'sendRetryRequest: requested placeholder resend ({request_id}) for message '
                                     f'{msg_id} (scheduled)')
                    except Exception as error:
                        logger.warn({'error': error, 'msgId': msg_id}, 'failed to send scheduled phone request')
                message_retry_manager.schedule_phone_request(msg_id, phone_request)
            else:
                # Fallback to immediate request
                request_id = await request_placeholder_resend(msg_key)
                logger.debug(f'sendRetryRequest: requested placeholder resend for message {request_id}')

        device_identity = encode_signed_device_identity(account, True)

        async def tx():
            receipt = {'tag': 'receipt', 'attrs': {'id': msg_id, 'type': 'retry', 'to': node['attrs']['from']},
                       'content': [
                           {'tag': 'retry', 'attrs': {'count': str(retry_count), 'id': node['attrs'].get('id'),
                                                      't': node['attrs'].get('t'), 'v': '1',
                                                      'error': '0'}},  # ADD ERROR FIELD
                           {'tag': 'registration', 'attrs': {}, 'content': encode_big_endian(creds['registrationId'])}]}
            if node['attrs'].get('recipient'):
                receipt['attrs']['recipient'] = node['attrs']['recipient']
            if node['attrs'].get('participant'):
                receipt['attrs']['participant'] = node['attrs']['participant']
            if retry_count > 1 or force_include_keys or should_recreate_session:
                r = await get_next_pre_keys(auth_state, 1)
                update, pre_keys = r['update'], r['preKeys']
                key_id = next(iter(pre_keys))
                pre_key = pre_keys[key_id]
                receipt['content'].append({'tag': 'keys', 'attrs': {}, 'content': [
                    {'tag': 'type', 'attrs': {}, 'content': bytes(KEY_BUNDLE_TYPE)},
                    {'tag': 'identity', 'attrs': {}, 'content': identity_key['public']},
                    xmpp_pre_key(pre_key, int(key_id)),
                    xmpp_signed_pre_key(signed_pre_key),
                    {'tag': 'device-identity', 'attrs': {}, 'content': device_identity}]})
                ev.emit('creds.update', update)
            await send_node(receipt)
            logger.info({'msgAttrs': node['attrs'], 'retryCount': retry_count}, 'sent retry receipt')
        await auth_state['keys'].transaction(tx, ((auth_state.get('creds') or {}).get('me') or {}).get('id')
                                             or 'sendRetryRequest')

    # Mirrors WAWeb/Handle/PreKeyLow.js: skip a re-issued notification with the same stanza id.
    in_flight_pre_key_low: set = set()

    def reissue_tc_token_after_identity_change(from_: str):
        """Fire-and-forget tctoken re-issuance after a peer's device identity changed."""
        async def run():
            try:
                normalized_jid = jid_normalized_user(from_)
                tc_jid = await resolve_tc_token_jid(normalized_jid, get_lid_for_pn)
                tc_token_data = await auth_state['keys'].get('tctoken', [tc_jid])
                sender_ts = (tc_token_data.get(tc_jid) or {}).get('senderTimestamp')
                if sender_ts is None or is_tc_token_expired(sender_ts):
                    return
                logger.debug({'jid': normalized_jid, 'senderTimestamp': sender_ts}, 'identity changed, re-issuing tctoken')
                issue_jid = await resolve_issuance_jid(normalized_jid, sock.server_props['lidTrustedTokenIssueToLid'],
                                                       get_lid_for_pn, signal_repository.lid_mapping.get_pn_for_lid)
                result = await issue_privacy_tokens([issue_jid], sender_ts)
                await store_tc_tokens_from_iq_result(result, tc_jid, auth_state['keys'], get_lid_for_pn,
                                                     track_tc_token_jid)
            except Exception as err:
                logger.debug({'jid': from_, 'err': str(err)}, 'failed to re-issue tctoken after identity change')
        asyncio.ensure_future(run())

    async def handle_encrypt_notification(node: dict):
        from_ = node['attrs'].get('from')
        if from_ == S_WHATSAPP_NET:
            stanza_id = node['attrs'].get('id')
            if stanza_id and stanza_id in in_flight_pre_key_low:
                return
            count_child = get_binary_node_child(node, 'count')
            count = int(count_child['attrs']['value'])
            should_upload_more_pre_keys = count < MIN_PREKEY_COUNT
            logger.debug({'count': count, 'shouldUploadMorePreKeys': should_upload_more_pre_keys}, 'recv pre-key count')
            if should_upload_more_pre_keys:
                if stanza_id:
                    in_flight_pre_key_low.add(stanza_id)
                try:
                    await upload_pre_keys()
                finally:
                    if stanza_id:
                        in_flight_pre_key_low.discard(stanza_id)
        else:
            result = await handle_identity_change(node, {
                'meId': (auth_state['creds'].get('me') or {}).get('id'),
                'meLid': (auth_state['creds'].get('me') or {}).get('lid'),
                'validateSession': signal_repository.validate_session, 'assertSessions': assert_sessions,
                'debounceCache': identity_assert_debounce, 'logger': logger,
                'onBeforeSessionRefresh': reissue_tc_token_after_identity_change})
            if result['action'] == 'no_identity_node':
                logger.info({'node': node}, 'unknown encrypt notification')

    def handle_group_notification(full_node: dict, child: dict, msg: dict):
        # TODO: Support PN/LID (Here is only LID now)
        acting_participant_lid = full_node['attrs'].get('participant')
        acting_participant_pn = full_node['attrs'].get('participant_pn')
        acting_participant_username = full_node['attrs'].get('participant_username')
        part_child = get_binary_node_child(child, 'participant')
        affected_participant_lid = ((part_child or {}).get('attrs') or {}).get('jid') or acting_participant_lid
        affected_participant_pn = ((part_child or {}).get('attrs') or {}).get('phone_number') or acting_participant_pn
        tag = child.get('tag') if child else None

        if tag == 'create':
            metadata = extract_group_metadata(child)
            msg['messageStubType'] = WAMessageStubType.GROUP_CREATE
            msg['messageStubParameters'] = [metadata['subject']]
            msg['key'] = {'participant': metadata.get('owner'), 'participantAlt': metadata.get('ownerPn')}
            ev.emit('chats.upsert', [{'id': metadata['id'], 'name': metadata['subject'],
                                      'conversationTimestamp': metadata.get('creation')}])
            ev.emit('groups.upsert', [{**metadata, 'author': acting_participant_lid, 'authorPn': acting_participant_pn,
                                       'authorUsername': acting_participant_username}])
        elif tag in ('ephemeral', 'not_ephemeral'):
            msg['message'] = {'protocolMessage': {'type': proto.Message.ProtocolMessage.EPHEMERAL_SETTING,
                                                  'ephemeralExpiration': int(child['attrs'].get('expiration') or 0)}}
        elif tag == 'modify':
            old_number = [p['attrs'].get('jid') for p in get_binary_node_children(child, 'participant')]
            msg['messageStubParameters'] = old_number or []
            msg['messageStubType'] = WAMessageStubType.GROUP_PARTICIPANT_CHANGE_NUMBER
        elif tag in ('promote', 'demote', 'remove', 'add', 'leave'):
            msg['messageStubType'] = getattr(WAMessageStubType, f'GROUP_PARTICIPANT_{tag.upper()}')
            participants = []
            for p in get_binary_node_children(child, 'participant'):
                a = p['attrs']
                # TODO: Store LID MAPPINGS
                participants.append({
                    'id': a.get('jid'),
                    'phoneNumber': a.get('phone_number') if is_lid_user(a.get('jid')) and is_pn_user(a.get('phone_number'))
                    else None,
                    'lid': a.get('lid') if is_pn_user(a.get('jid')) and is_lid_user(a.get('lid')) else None,
                    'username': a.get('participant_username') or a.get('username') or None,
                    'admin': a.get('type') or None})
            if (len(participants) == 1 and
                    # if recv. "remove" message and sender removed themselves mark as left
                    (are_jids_same_user(participants[0]['id'], acting_participant_lid) or
                     are_jids_same_user(participants[0]['id'], acting_participant_pn)) and tag == 'remove'):
                msg['messageStubType'] = WAMessageStubType.GROUP_PARTICIPANT_LEAVE
            msg['messageStubParameters'] = [json.dumps({k: v for k, v in p.items() if v is not None}) for p in participants]
        elif tag == 'subject':
            msg['messageStubType'] = WAMessageStubType.GROUP_CHANGE_SUBJECT
            msg['messageStubParameters'] = [child['attrs'].get('subject')]
        elif tag == 'description':
            body = get_binary_node_child(child, 'body')
            description = _str(body.get('content')) if body and body.get('content') is not None else None
            msg['messageStubType'] = WAMessageStubType.GROUP_CHANGE_DESCRIPTION
            msg['messageStubParameters'] = [description] if description else None
        elif tag in ('announcement', 'not_announcement'):
            msg['messageStubType'] = WAMessageStubType.GROUP_CHANGE_ANNOUNCE
            msg['messageStubParameters'] = ['on' if tag == 'announcement' else 'off']
        elif tag in ('locked', 'unlocked'):
            msg['messageStubType'] = WAMessageStubType.GROUP_CHANGE_RESTRICT
            msg['messageStubParameters'] = ['on' if tag == 'locked' else 'off']
        elif tag == 'invite':
            msg['messageStubType'] = WAMessageStubType.GROUP_CHANGE_INVITE_LINK
            msg['messageStubParameters'] = [child['attrs'].get('code')]
        elif tag == 'member_add_mode':
            add_mode = child.get('content')
            if add_mode:
                msg['messageStubType'] = WAMessageStubType.GROUP_MEMBER_ADD_MODE
                msg['messageStubParameters'] = [_str(add_mode)]
        elif tag == 'membership_approval_mode':
            approval_mode = get_binary_node_child(child, 'group_join')
            if approval_mode:
                msg['messageStubType'] = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE
                msg['messageStubParameters'] = [approval_mode['attrs'].get('state')]
        elif tag == 'created_membership_requests':
            msg['messageStubType'] = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD
            msg['messageStubParameters'] = [json.dumps({'lid': affected_participant_lid, 'pn': affected_participant_pn}),
                                            'created', child['attrs'].get('request_method')]
        elif tag == 'revoked_membership_requests':
            is_denied = are_jids_same_user(affected_participant_lid, acting_participant_lid)
            # TODO: LIDMAPPING SUPPORT
            msg['messageStubType'] = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD
            msg['messageStubParameters'] = [json.dumps({'lid': affected_participant_lid, 'pn': affected_participant_pn}),
                                            'revoked' if is_denied else 'rejected']

    async def handle_devices_notification(node: dict):
        children = get_all_binary_node_children(node)
        child = children[0] if children else None
        from_ = jid_normalized_user(node['attrs'].get('from'))
        if not child:
            logger.debug({'from': from_}, 'devices notification missing child, skipping')
            return
        tag = child['tag']
        device_hash = child['attrs'].get('device_hash')
        devices = get_binary_node_children(child, 'device')
        me = auth_state['creds']['me']
        if are_jids_same_user(from_, me['id']) or are_jids_same_user(from_, me.get('lid')):
            logger.info({'deviceJids': [d['attrs'].get('jid') for d in devices]}, 'got my own devices')
        if not devices:
            logger.debug({'from': from_, 'tag': tag}, 'no devices in notification, skipping')
            return

        decoded = []
        for d in devices:
            jid = d['attrs'].get('jid')
            if not jid:
                continue
            parts = jid_decode(jid)
            if not parts:
                logger.debug({'jid': jid}, 'failed to decode device jid, skipping')
                continue
            decoded.append({'jid': jid, 'user': parts.user, 'server': parts.server, 'device': parts.device})
        if not decoded:
            return

        async def work():
            by_user: dict = {}
            for d in decoded:
                by_user.setdefault(d['user'], []).append(d)
            for user, entries in by_user.items():
                if tag == 'update':
                    logger.debug({'user': user}, f"{user}'s device list updated, dropping cached devices")
                    await _maybe(user_devices_cache.delete(user) if hasattr(user_devices_cache, 'delete')
                                 else user_devices_cache.del_(user))
                    continue
                if tag == 'remove':
                    await signal_repository.delete_session([e['jid'] for e in entries])
                existing_cache = (await _maybe(user_devices_cache.get(user))) or []
                if not existing_cache:
                    # No baseline yet; skip applying the delta so getUSyncDevices can later fetch the full device
                    # list. Caching just the notification entries would make a partial list look authoritative.
                    logger.debug({'user': user, 'tag': tag}, 'device list not cached, deferring to USync refresh')
                    continue
                affected = {e['device'] for e in entries}
                if tag == 'add':
                    logger.info({'deviceHash': device_hash, 'count': len(entries)}, 'devices added')
                    updated_devices = [*[d for d in existing_cache if d['device'] not in affected],
                                       *[FullJid(user=e['user'], server=e['server'], device=e['device'])
                                         for e in entries]]
                elif tag == 'remove':
                    logger.info({'deviceHash': device_hash, 'count': len(entries)}, 'devices removed')
                    updated_devices = [d for d in existing_cache if d['device'] not in affected]
                else:
                    logger.debug({'tag': tag}, 'Unknown device list change tag')
                    continue
                if len(updated_devices) == 0:
                    await _maybe(user_devices_cache.delete(user) if hasattr(user_devices_cache, 'delete')
                                 else user_devices_cache.del_(user))
                else:
                    await _maybe(user_devices_cache.set(user, updated_devices))
        await devices_mutex.mutex(work)

    # In-memory cache of storage JIDs with stored tctokens, seeded from the persisted index.
    tc_token_known_jids: set = set()
    tc_state = {'indexTimer': None}

    async def _load_tc_token_index():
        try:
            jids = await read_tc_token_index(auth_state['keys'])
            for jid in jids:
                tc_token_known_jids.add(jid)
            logger.debug({'count': len(tc_token_known_jids)}, 'loaded tctoken index')
        except Exception as err:
            logger.warn({'err': str(err)}, 'failed to load tctoken index')
    tc_token_index_loaded = asyncio.ensure_future(_load_tc_token_index())

    async def flush_tc_token_index():
        if tc_state['indexTimer']:
            tc_state['indexTimer'].cancel()
            tc_state['indexTimer'] = None
        # Merge with whatever is already persisted so we don't clobber writes from other paths
        write = await build_merged_tc_token_index_write(auth_state['keys'], list(tc_token_known_jids))
        return await auth_state['keys'].set({'tctoken': write})

    def schedule_tc_token_index_save():
        if tc_state['indexTimer']:
            tc_state['indexTimer'].cancel()

        def fire():
            tc_state['indexTimer'] = None

            async def go():
                try:
                    await flush_tc_token_index()
                except Exception as err:
                    logger.warn({'err': str(err)}, 'failed to save tctoken index')
            asyncio.ensure_future(go())
        tc_state['indexTimer'] = asyncio.get_running_loop().call_later(5, fire)

    def track_tc_token_jid(jid: str):
        if jid and jid != TC_TOKEN_INDEX_KEY and jid not in tc_token_known_jids:
            tc_token_known_jids.add(jid)
            schedule_tc_token_index_save()

    async def handle_privacy_token_notification(node: dict):
        tokens_node = get_binary_node_child(node, 'tokens')
        if not tokens_node:
            return
        from_ = jid_normalized_user(node['attrs'].get('from'))
        # WA Web uses: senderLid ?? toLid(from) for the storage key
        sender_lid_attr = node['attrs'].get('sender_lid')
        sender_lid = jid_normalized_user(sender_lid_attr) if sender_lid_attr and \
            is_lid_user(jid_normalized_user(sender_lid_attr)) else None
        fallback_jid = sender_lid or await resolve_tc_token_jid(from_, get_lid_for_pn)
        logger.debug({'from': from_, 'storageJid': fallback_jid}, 'processing privacy token notification')
        await store_tc_tokens_from_iq_result(node, fallback_jid, auth_state['keys'], get_lid_for_pn, track_tc_token_jid)

    def to_required_buffer(data) -> bytes:
        if data is None:
            raise Boom('Invalid buffer', status_code=400)
        return bytes(data)

    async def decipher_link_public_key(data: bytes) -> bytes:
        buffer = to_required_buffer(data)
        salt = buffer[:32]
        secret_key = await derive_pairing_code_key(auth_state['creds']['pairingCode'], salt)
        iv = buffer[32:48]
        payload = buffer[48:80]
        return aes_decrypt_ctr(payload, secret_key, iv)

    async def process_notification(node: dict) -> Optional[dict]:
        result: dict = {}
        children = get_all_binary_node_children(node)
        child = children[0] if children else None
        node_type = node['attrs'].get('type')
        from_ = jid_normalized_user(node['attrs'].get('from'))

        if node_type == 'newsletter':
            await handle_newsletter_notification(node)
        elif node_type == 'mex':
            await handle_mex_notification(node)
        elif node_type == 'w:gp2':
            # TODO: HANDLE PARTICIPANT_PN
            handle_group_notification(node, child, result)
        elif node_type == 'mediaretry':
            ev.emit('messages.media-update', [decode_media_retry_node(node)])
        elif node_type == 'encrypt':
            await handle_encrypt_notification(node)
        elif node_type == 'devices':
            try:
                await handle_devices_notification(node)
            except Exception as error:
                logger.error({'error': error, 'node': node}, 'failed to handle devices notification')
        elif node_type == 'server_sync':
            update = get_binary_node_child(node, 'collection')
            if update:
                await resync_app_state([update['attrs'].get('name')], False)
        elif node_type == 'picture':
            set_picture = get_binary_node_child(node, 'set')
            del_picture = get_binary_node_child(node, 'delete')
            # TODO: WAJIDHASH stuff proper support inhouse
            ev.emit('contacts.update', [{
                'id': jid_normalized_user((node.get('attrs') or {}).get('from')) or
                ((set_picture or del_picture or {}).get('attrs') or {}).get('hash') or '',
                'imgUrl': 'changed' if set_picture else 'removed'}])
            if is_jid_group(from_):
                pic_node = set_picture or del_picture
                result['messageStubType'] = WAMessageStubType.GROUP_CHANGE_ICON
                if set_picture:
                    result['messageStubParameters'] = [set_picture['attrs'].get('id')]
                result['participant'] = (pic_node or {}).get('attrs', {}).get('author')
                result['key'] = {**(result.get('key') or {}),
                                 'participant': (set_picture or {}).get('attrs', {}).get('author')}
        elif node_type == 'account_sync':
            if child['tag'] == 'disappearing_mode':
                new_duration = int(child['attrs'].get('duration'))
                timestamp = int(child['attrs'].get('t'))
                logger.info({'newDuration': new_duration}, 'updated account disappearing mode')
                ev.emit('creds.update', {'accountSettings': {
                    **(auth_state['creds'].get('accountSettings') or {}),
                    'defaultDisappearingMode': {'ephemeralExpiration': new_duration,
                                                'ephemeralSettingTimestamp': timestamp}}})
            elif child['tag'] == 'blocklist':
                for item in get_binary_node_children(child, 'item'):
                    attrs = item['attrs']
                    ev.emit('blocklist.update', {'blocklist': [attrs.get('jid')],
                                                 'type': 'add' if attrs.get('action') == 'block' else 'remove'})
        elif node_type == 'link_code_companion_reg':
            creds = auth_state['creds']
            link_code_companion_reg = get_binary_node_child(node, 'link_code_companion_reg')
            ref = to_required_buffer(get_binary_node_child_buffer(link_code_companion_reg, 'link_code_pairing_ref'))
            primary_identity_public_key = to_required_buffer(
                get_binary_node_child_buffer(link_code_companion_reg, 'primary_identity_pub'))
            primary_ephemeral_public_key_wrapped = to_required_buffer(get_binary_node_child_buffer(
                link_code_companion_reg, 'link_code_pairing_wrapped_primary_ephemeral_pub'))
            code_pairing_public_key = await decipher_link_public_key(primary_ephemeral_public_key_wrapped)
            companion_shared_key = Curve.shared_key(creds['pairingEphemeralKeyPair']['private'], code_pairing_public_key)
            random = os.urandom(32)
            link_code_salt = os.urandom(32)
            link_code_pairing_expanded = hkdf(companion_shared_key, 32, salt=link_code_salt,
                                              info='link_code_pairing_key_bundle_encryption_key')
            encrypt_payload = bytes(creds['signedIdentityKey']['public']) + primary_identity_public_key + random
            encrypt_iv = os.urandom(12)
            encrypted = aes_encrypt_gcm(encrypt_payload, link_code_pairing_expanded, encrypt_iv, b'')
            encrypted_payload = link_code_salt + encrypt_iv + encrypted
            identity_shared_key = Curve.shared_key(creds['signedIdentityKey']['private'], primary_identity_public_key)
            identity_payload = companion_shared_key + identity_shared_key + random
            import base64
            creds['advSecretKey'] = base64.b64encode(hkdf(identity_payload, 32, info='adv_secret')).decode()
            await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'id': sock.generate_message_tag(),
                                                'xmlns': 'md'},
                         'content': [{'tag': 'link_code_companion_reg',
                                      'attrs': {'jid': creds['me']['id'], 'stage': 'companion_finish'},
                                      'content': [
                                          {'tag': 'link_code_pairing_wrapped_key_bundle', 'attrs': {},
                                           'content': encrypted_payload},
                                          {'tag': 'companion_identity_public', 'attrs': {},
                                           'content': creds['signedIdentityKey']['public']},
                                          {'tag': 'link_code_pairing_ref', 'attrs': {}, 'content': ref}]}]})
            creds['registered'] = True
            ev.emit('creds.update', creds)
        elif node_type == 'privacy_token':
            await handle_privacy_token_notification(node)

        return result if result else None

    async def will_send_message_again(id_: str, participant: str) -> bool:
        retry_count = (await _maybe(msg_retry_cache.get(f'{id_}:{participant}'))) or 0
        return retry_count < max_msg_retry_count

    async def update_send_message_again_count(id_: str, participant: str):
        key = f'{id_}:{participant}'
        new_value = ((await _maybe(msg_retry_cache.get(key))) or 0) + 1
        await _maybe(msg_retry_cache.set(key, new_value))

    async def send_messages_again(key: dict, ids: list, retry_node: dict, receipt_node: dict):
        remote_jid = key['remoteJid']
        participant = key.get('participant') or remote_jid
        retry_count = int(retry_node['attrs'].get('count') or 0) or 1
        msg_id = ids[0] if ids else None

        # Try to get messages from cache first, then fallback to getMessage
        msgs = []
        for id_ in ids:
            msg = None
            # Try to get from retry cache first if enabled
            if message_retry_manager:
                cached_msg = message_retry_manager.get_recent_message(remote_jid, id_)
                if cached_msg:
                    msg = cached_msg['message']
                    logger.debug({'jid': remote_jid, 'id': id_}, 'found message in retry cache')
                    # Mark retry as successful since we found the message
                    message_retry_manager.mark_retry_success(id_)
            # Fallback to getMessage if not found in cache
            if not msg and get_message:
                msg = await get_message({**key, 'id': id_})
                if msg:
                    logger.debug({'jid': remote_jid, 'id': id_}, 'found message via getMessage')
                    # Also mark as successful if found via getMessage
                    if message_retry_manager:
                        message_retry_manager.mark_retry_success(id_)
            msgs.append(msg)

        # if it's the primary jid sending the request just re-send the message to everyone;
        # prevents the first message decryption failure
        pd = jid_decode(participant)
        send_to_all = not (pd.device if pd else None)

        session_id = signal_repository.jid_to_signal_protocol_address(participant)
        injected_from_bundle = False

        bundle = extract_e2e_session_from_retry_receipt(receipt_node)
        if bundle:
            try:
                await signal_repository.inject_e2e_session(jid=participant, session=bundle)
                injected_from_bundle = True
                logger.debug({'participant': participant, 'retryCount': retry_count},
                             'injected session from retry receipt key bundle')
            except Exception as error:
                logger.warn({'error': error, 'participant': participant}, 'failed to inject session from retry receipt')

        if not injected_from_bundle:
            received_reg_id = get_binary_node_child_uint(receipt_node, 'registration', 4)
            if isinstance(received_reg_id, int):
                info = await signal_repository.get_session_info(participant)
                if info and info['registrationId'] != 0 and info['registrationId'] != received_reg_id:
                    logger.info({'participant': participant, 'stored': info['registrationId'],
                                 'received': received_reg_id}, 'reg id mismatch on retry without bundle, deleting session')
                    await auth_state['keys'].set({'session': {session_id: None}})

        BASE_KEY_CHECK_RETRY = 2
        if msg_id and message_retry_manager:
            info = await signal_repository.get_session_info(participant)
            if info:
                if retry_count == BASE_KEY_CHECK_RETRY:
                    message_retry_manager.save_base_key(session_id, msg_id, info['baseKey'])
                elif retry_count > BASE_KEY_CHECK_RETRY:
                    if message_retry_manager.has_same_base_key(session_id, msg_id, info['baseKey']):
                        logger.warn({'participant': participant, 'retryCount': retry_count},
                                    'base key collision on retry, forcing fresh session')
                        await auth_state['keys'].set({'session': {session_id: None}})
                    message_retry_manager.delete_base_key(session_id, msg_id)

        should_recreate_session = False
        recreate_reason = ''
        if enable_auto_session_recreation and message_retry_manager and retry_count > 1 and not injected_from_bundle:
            try:
                has_session = await signal_repository.validate_session(participant)
                result = message_retry_manager.should_recreate_session(participant, has_session['exists'])
                should_recreate_session = result['recreate']
                recreate_reason = result['reason']
                if should_recreate_session:
                    logger.debug({'participant': participant, 'retryCount': retry_count, 'reason': recreate_reason},
                                 'recreating session for outgoing retry')
                    await auth_state['keys'].set({'session': {session_id: None}})
            except Exception as error:
                logger.warn({'error': error, 'participant': participant},
                            'failed to check session recreation for outgoing retry')

        if not injected_from_bundle:
            await assert_sessions([participant], True)

        if is_jid_group(remote_jid):
            await auth_state['keys'].set({'sender-key-memory': {remote_jid: None}})

        logger.debug({'participant': participant, 'sendToAll': send_to_all, 'shouldRecreateSession': should_recreate_session,
                      'recreateReason': recreate_reason, 'injectedFromBundle': injected_from_bundle},
                     'prepared session for retry resend')

        for i, msg in enumerate(msgs):
            if i >= len(ids) or not ids[i]:
                continue
            if msg and await will_send_message_again(ids[i], participant):
                await update_send_message_again_count(ids[i], participant)
                msg_relay_opts = {'messageId': ids[i]}
                if send_to_all:
                    msg_relay_opts['useUserDevicesCache'] = False
                else:
                    msg_relay_opts['participant'] = {'jid': participant, 'count': int(retry_node['attrs'].get('count') or 0)}
                await relay_message(key['remoteJid'], msg, msg_relay_opts)
            else:
                logger.debug({'jid': key.get('remoteJid'), 'id': ids[i]}, 'recv retry request, but message not available')

    async def handle_receipt(node: dict):
        attrs, content = node['attrs'], node.get('content')
        is_lid = 'lid' in attrs['from']
        me = auth_state['creds'].get('me') or {}
        is_node_from_me = are_jids_same_user(attrs.get('participant') or attrs.get('from'),
                                             me.get('lid') if is_lid else me.get('id'))
        remote_jid = attrs['from'] if (not is_node_from_me or is_jid_group(attrs.get('from'))) else attrs.get('recipient')
        from_me = not attrs.get('recipient') or (attrs.get('type') in ('retry', 'sender') and is_node_from_me)
        key = {'remoteJid': remote_jid, 'id': '', 'fromMe': from_me, 'participant': attrs.get('participant')}
        ids = [attrs.get('id')]
        if isinstance(content, list):
            items = get_binary_node_children(content[0], 'item')
            ids.extend(i['attrs'].get('id') for i in items)
        try:
            async def under_mutex():
                status = get_status_from_receipt_type(attrs.get('type'))
                # we only want to know when a message from us has been delivered to/read by the other person
                # or another device of ours has read some messages
                if status is not None and (status >= proto.WebMessageInfo.SERVER_ACK or not is_node_from_me):
                    if is_jid_group(remote_jid) or is_jid_status_broadcast(remote_jid):
                        if attrs.get('participant'):
                            update_key = 'receiptTimestamp' if status == proto.WebMessageInfo.DELIVERY_ACK \
                                else 'readTimestamp'
                            ev.emit('message-receipt.update', [
                                {'key': {**key, 'id': id_},
                                 'receipt': {'userJid': jid_normalized_user(attrs['participant']),
                                             update_key: int(attrs.get('t') or 0)}} for id_ in ids])
                    else:
                        ev.emit('messages.update', [
                            {'key': {**key, 'id': id_},
                             'update': {'status': status, 'messageTimestamp': to_number(int(attrs.get('t') or 0))}}
                            for id_ in ids])
                if attrs.get('type') == 'retry':
                    # correctly set who is asking for the retry
                    key['participant'] = key.get('participant') or attrs.get('from')
                    retry_node = get_binary_node_child(node, 'retry')
                    if ids[0] and key['participant'] and await will_send_message_again(ids[0], key['participant']):
                        if key['fromMe']:
                            try:
                                await update_send_message_again_count(ids[0], key['participant'])
                                logger.debug({'attrs': attrs, 'key': key}, 'recv retry request')
                                await send_messages_again(key, ids, retry_node, node)
                            except Exception as error:
                                logger.error({'key': key, 'ids': ids, 'trace': str(error)},
                                             'error in sending message again')
                        else:
                            logger.info({'attrs': attrs, 'key': key}, 'recv retry for not fromMe message')
                    else:
                        logger.info({'attrs': attrs, 'key': key}, 'will not send message again, as sent too many times')
            await receipt_mutex.mutex(under_mutex)
        finally:
            try:
                await send_message_ack(node)
            except Exception as ack_err:
                logger.error({'ackErr': ack_err}, 'failed to ack receipt')

    async def handle_notification(node: dict):
        remote_jid = node['attrs'].get('from')
        try:
            async def under_mutex():
                msg = await process_notification(node)
                if msg:
                    from_me = are_jids_same_user(node['attrs'].get('participant') or remote_jid,
                                                 auth_state['creds']['me']['id'])
                    ctx = extract_addressing_context(node)
                    msg['key'] = {'remoteJid': remote_jid, 'fromMe': from_me, 'participant': node['attrs'].get('participant'),
                                  'participantAlt': ctx['senderAlt'],
                                  'participantUsername': node['attrs'].get('participant_username'),
                                  'addressingMode': ctx['addressingMode'], 'id': node['attrs'].get('id'),
                                  **(msg.get('key') or {})}
                    if msg.get('participant') is None:
                        msg['participant'] = node['attrs'].get('participant')
                    msg['messageTimestamp'] = int(node['attrs'].get('t') or 0)
                    full_msg = normalize(proto.WebMessageInfo, msg)
                    # keep the extra key fields that don't exist in the proto (Alt / username / addressingMode)
                    full_msg['key'] = {**msg['key'], **{k: v for k, v in full_msg.get('key', {}).items()}}
                    full_msg['key'] = {k: v for k, v in full_msg['key'].items() if v is not None}
                    await upsert_message(full_msg, 'append')
            await notification_mutex.mutex(under_mutex)
        finally:
            try:
                await send_message_ack(node)
            except Exception as ack_err:
                logger.error({'ackErr': ack_err}, 'failed to ack notification')

    async def handle_message(node: dict):
        enc_node = get_binary_node_child(node, 'enc')
        # TODO: temporary fix for crashes and issues resulting of failed msmsg decryption
        if enc_node and enc_node['attrs'].get('type') == 'msmsg':
            logger.debug({'key': node['attrs'].get('key')}, 'ignored msmsg')
            await send_message_ack(node, NACK_REASONS['MissingMessageSecret'])
            return

        flags = {'acked': False}
        try:
            me = auth_state['creds']['me']
            d = decrypt_message_node(node, me['id'], me.get('lid') or '', signal_repository, logger)
            msg, category, author, decrypt = d['fullMessage'], d['category'], d['author'], d['decrypt']

            alt = msg['key'].get('participantAlt') or msg['key'].get('remoteJidAlt')
            # store new mappings we didn't have before
            if alt:
                alt_decoded = jid_decode(alt)
                alt_server = alt_decoded.server if alt_decoded else None
                primary_jid = msg['key'].get('participant') or msg['key'].get('remoteJid')
                if alt_server == 'lid':
                    if not await signal_repository.lid_mapping.get_pn_for_lid(alt):
                        await signal_repository.lid_mapping.store_lid_pn_mappings([{'lid': alt, 'pn': primary_jid}])
                        await signal_repository.migrate_session(primary_jid, alt)
                else:
                    await signal_repository.lid_mapping.store_lid_pn_mappings([{'lid': primary_jid, 'pn': alt}])
                    await signal_repository.migrate_session(alt, primary_jid)

            async def under_mutex():
                await decrypt()
                if msg['key'].get('remoteJid') and msg['key'].get('id') and msg.get('message') and message_retry_manager:
                    message_retry_manager.add_recent_message(msg['key']['remoteJid'], msg['key']['id'], msg['message'])

                # message failed to decrypt
                if msg.get('messageStubType') == proto.WebMessageInfo.CIPHERTEXT and msg.get('category') != 'peer':
                    stub_params = msg.get('messageStubParameters') or []
                    if stub_params and stub_params[0] == MISSING_KEYS_ERROR_TEXT:
                        flags['acked'] = True
                        return await send_message_ack(node, NACK_REASONS['ParsingError'])
                    if stub_params and stub_params[0] == NO_MESSAGE_FOUND_ERROR_TEXT:
                        # Message arrived without encryption (e.g. CTWA ads messages).
                        # Check if this is eligible for placeholder resend (matching WA Web filters).
                        unavailable_node = get_binary_node_child(node, 'unavailable')
                        unavailable_type = ((unavailable_node or {}).get('attrs') or {}).get('type')
                        if unavailable_type in ('bot_unavailable_fanout', 'hosted_unavailable_fanout',
                                                'view_once_unavailable_fanout'):
                            logger.debug({'msgId': msg['key'].get('id'), 'unavailableType': unavailable_type},
                                         'skipping placeholder resend for excluded unavailable type')
                            flags['acked'] = True
                            return await send_message_ack(node)
                        message_age = unix_timestamp_seconds() - to_number(msg.get('messageTimestamp'))
                        if message_age > PLACEHOLDER_MAX_AGE_SECONDS:
                            logger.debug({'msgId': msg['key'].get('id'), 'messageAge': message_age},
                                         'skipping placeholder resend for old message')
                            flags['acked'] = True
                            return await send_message_ack(node)
                        # Request the real content from the phone via placeholder resend PDO. Upsert the CIPHERTEXT
                        # stub as a placeholder, and store the requestId in stubParameters[1].
                        clean_key = {'remoteJid': msg['key'].get('remoteJid'), 'fromMe': msg['key'].get('fromMe'),
                                     'id': msg['key'].get('id'), 'participant': msg['key'].get('participant')}
                        # Cache the original message metadata so the PDO response handler can preserve key fields
                        msg_data = {'key': msg['key'], 'messageTimestamp': msg.get('messageTimestamp'),
                                    'pushName': msg.get('pushName'), 'participant': msg.get('participant'),
                                    'verifiedBizName': msg.get('verifiedBizName')}

                        async def request():
                            try:
                                request_id = await request_placeholder_resend(clean_key, msg_data)
                                if request_id and request_id != 'RESOLVED':
                                    logger.debug({'msgId': msg['key'].get('id'), 'requestId': request_id},
                                                 'requested placeholder resend for unavailable message')
                                    ev.emit('messages.update', [{'key': msg['key'], 'update': {
                                        'messageStubParameters': [NO_MESSAGE_FOUND_ERROR_TEXT, request_id]}}])
                            except Exception as err:
                                logger.warn({'err': err, 'msgId': msg['key'].get('id')},
                                            'failed to request placeholder resend for unavailable message')
                        asyncio.ensure_future(request())
                        flags['acked'] = True
                        await send_message_ack(node)
                        # Don't return — fall through to upsertMessage so the stub is emitted
                    else:
                        # Skip retry for expired status messages (>24h old)
                        if is_jid_status_broadcast(msg['key'].get('remoteJid')):
                            message_age = unix_timestamp_seconds() - to_number(msg.get('messageTimestamp'))
                            if message_age > STATUS_EXPIRY_SECONDS:
                                logger.debug({'msgId': msg['key'].get('id'), 'messageAge': message_age,
                                              'remoteJid': msg['key'].get('remoteJid')},
                                             'skipping retry for expired status message')
                                flags['acked'] = True
                                return await send_message_ack(node)
                        logger.debug('[handleMessage] Attempting retry request for failed decryption')

                        # WAWeb only retry-receipts here; server emits PreKeyLow if prekeys run low.
                        async def retry():
                            try:
                                if not ws.is_open:
                                    logger.debug({'node': node}, 'Connection closed, skipping retry')
                                    return
                                enc = get_binary_node_child(node, 'enc')
                                await send_retry_request(node, not enc)
                                if retry_request_delay_ms:
                                    await delay(retry_request_delay_ms)
                            except Exception as err:
                                logger.error({'err': err}, 'Failed to send retry')
                            flags['acked'] = True
                            await send_message_ack(node, NACK_REASONS['UnhandledError'])
                        await retry_mutex.mutex(retry)
                else:
                    if message_retry_manager and msg['key'].get('id'):
                        message_retry_manager.cancel_pending_phone_request(msg['key']['id'])
                    is_newsletter = is_jid_newsletter(msg['key'].get('remoteJid'))
                    if not is_newsletter:
                        # no type in the receipt => message delivered
                        type_ = None
                        participant = msg['key'].get('participant')
                        if category == 'peer':
                            type_ = 'peer_msg'  # special peer message
                        elif msg['key'].get('fromMe'):
                            # message was sent by us from a different device
                            type_ = 'sender'
                            # need to specially handle this case
                            if is_lid_user(msg['key'].get('remoteJid')) or is_lid_user(msg['key'].get('remoteJidAlt')):
                                participant = author  # TODO: investigate sending receipts to LIDs and not PNs
                        elif not st['sendActiveReceipts']:
                            type_ = 'inactive'
                        flags['acked'] = True
                        await send_receipt(msg['key']['remoteJid'], participant, [msg['key']['id']], type_)
                        # send ack for history message
                        is_any_history_msg = get_history_msg(msg.get('message')) if msg.get('message') else None
                        if is_any_history_msg:
                            jid = jid_normalized_user(msg['key']['remoteJid'])
                            await send_receipt(jid, None, [msg['key']['id']], 'hist_sync')  # TODO: investigate
                    else:
                        flags['acked'] = True
                        await send_message_ack(node)
                        logger.debug({'key': msg['key']}, 'processed newsletter message without receipts')

                clean_message(msg, auth_state['creds']['me']['id'], auth_state['creds']['me'].get('lid'))
                await upsert_message(msg, 'append' if node['attrs'].get('offline') else 'notify')
            await message_mutex.mutex(under_mutex)
        except Exception as error:
            logger.error({'error': error, 'node': binary_node_to_string(node)}, 'error in handling message')
            if not flags['acked']:
                try:
                    await send_message_ack(node, NACK_REASONS['UnhandledError'])
                except Exception as ack_err:
                    logger.error({'ackErr': ack_err}, 'failed to ack message after error')

    async def handle_call(node: dict):
        try:
            attrs = node['attrs']
            children = get_all_binary_node_children(node)
            if not children:
                raise Boom('Missing call info in call node')
            info_child = children[0]
            status = get_call_status_from_node(info_child)
            call_id = info_child['attrs'].get('call-id')
            from_ = info_child['attrs'].get('from') or info_child['attrs'].get('call-creator')
            call = {'chatId': attrs.get('from'), 'from': from_, 'callerPn': info_child['attrs'].get('caller_pn'),
                    'id': call_id, 'date': datetime.fromtimestamp(int(attrs.get('t') or 0)),
                    'offline': bool(attrs.get('offline')), 'status': status}
            if status == 'relaylatency':
                latency_value = info_child['attrs'].get('latency') or info_child['attrs'].get('latency_ms') or \
                    info_child['attrs'].get('latency-ms')
                try:
                    call['latencyMs'] = float(latency_value)
                except (TypeError, ValueError):
                    pass
            if status == 'offer':
                call['isVideo'] = bool(get_binary_node_child(info_child, 'video'))
                call['isGroup'] = info_child['attrs'].get('type') == 'group' or bool(info_child['attrs'].get('group-jid'))
                call['groupJid'] = info_child['attrs'].get('group-jid')
                await _maybe(call_offer_cache.set(call['id'], call))
            existing_call = await _maybe(call_offer_cache.get(call['id']))
            # use existing call info to populate this event
            if existing_call:
                call['isVideo'] = existing_call.get('isVideo')
                call['isGroup'] = existing_call.get('isGroup')
                call['callerPn'] = call.get('callerPn') or existing_call.get('callerPn')
            # delete data once call has ended
            if status in ('reject', 'accept', 'timeout', 'terminate'):
                await _maybe(call_offer_cache.delete(call['id']))
            ev.emit('call', [call])
        except Exception as error:
            logger.error({'error': error, 'node': binary_node_to_string(node)}, 'error in handling call')
        finally:
            try:
                await send_message_ack(node)
            except Exception as ack_err:
                logger.error({'ackErr': ack_err}, 'failed to ack call')

    # dedupe in-flight 463 recovery token issuance by target JID
    in_flight_463_recoveries: set = set()

    async def handle_bad_ack(node: dict):
        attrs = node['attrs']
        key = {'remoteJid': attrs.get('from'), 'fromMe': True, 'id': attrs.get('id')}
        # WARNING: REFRAIN FROM ENABLING THE phash RESEND FOR NOW. IT WILL CAUSE A LOOP
        # error in acknowledgement, device could not display the message
        if attrs.get('error'):
            is_reachout_timelocked = attrs['error'] == str(NACK_REASONS['SenderReachoutTimelocked'])
            if attrs['error'] == SERVER_ERROR_CODES['MessageAccountRestriction']:
                # 463 = 1:1 message missing privacy token (tctoken). Usually means the account is restricted.
                # No retry — retrying counts as another "reach out" and worsens the restriction.
                logger.warn({'msgId': attrs.get('id'), 'from': attrs.get('from')},
                            'error 463: account restricted or missing tctoken for contact')
                ack_from = attrs.get('from')
                if ack_from and ack_from not in in_flight_463_recoveries:
                    in_flight_463_recoveries.add(ack_from)

                    async def recover():
                        try:
                            tc_storage_jid = await resolve_tc_token_jid(ack_from, get_lid_for_pn)
                            issue_jid = await resolve_issuance_jid(
                                ack_from, sock.server_props['lidTrustedTokenIssueToLid'], get_lid_for_pn,
                                signal_repository.lid_mapping.get_pn_for_lid)
                            result = await issue_privacy_tokens([issue_jid], unix_timestamp_seconds())
                            await store_tc_tokens_from_iq_result(result, tc_storage_jid, auth_state['keys'],
                                                                 get_lid_for_pn, track_tc_token_jid)
                            logger.debug({'from': ack_from}, 'completed 463 token recovery issuance')
                        except Exception as err:
                            logger.debug({'from': ack_from, 'err': str(err)}, 'failed 463 token recovery issuance')
                        finally:
                            in_flight_463_recoveries.discard(ack_from)
                    asyncio.ensure_future(recover())
            elif attrs['error'] == SERVER_ERROR_CODES['SmaxInvalid']:
                logger.warn({'msgId': attrs.get('id'), 'from': attrs.get('from')},
                            'smax-invalid (479): stanza rejected by server — likely stale device session or '
                            'malformed addressing')
            elif is_reachout_timelocked:
                # user is temporarily restricted, fetch current restriction details
                try:
                    await fetch_account_reachout_timelock()
                except Exception as err:
                    logger.warn({'err': err}, 'failed to fetch reachout timelock')
                logger.warn({'attrs': attrs}, 'received error in ack')
            else:
                logger.warn({'attrs': attrs}, 'received error in ack')
            from ..Types import WAMessageStatus
            ev.emit('messages.update', [{'key': key, 'update': {
                'status': WAMessageStatus.ERROR,
                'messageStubParameters': [attrs['error'], ACCOUNT_RESTRICTED_TEXT] if is_reachout_timelocked
                else [attrs['error']]}}])

    async def process_node_with_buffer(node: dict, identifier: str, exec_):
        """processes a node with the given function and adds the task to the existing buffer if we're buffering"""
        ev.buffer()
        try:
            await exec_(node)
        except Exception as err:
            on_unexpected_error(err, identifier)
        ev.flush()

    async def _yield_to_event_loop():
        await asyncio.sleep(0)

    offline_node_processor = make_offline_node_processor(
        {'message': handle_message, 'call': handle_call, 'receipt': handle_receipt,
         'notification': handle_notification},
        {'isWsOpen': lambda: ws.is_open, 'onUnexpectedError': on_unexpected_error,
         'yieldToEventLoop': _yield_to_event_loop})

    async def process_node(type_: str, node: dict, identifier: str, exec_):
        # Fast path: ack and drop ignored JIDs before entering the buffer/queue
        from_ = node['attrs'].get('from')
        ignore_jid = from_
        if type_ == 'receipt' and from_:
            attrs = node['attrs']
            is_lid = 'lid' in attrs['from']
            me = auth_state['creds'].get('me') or {}
            is_node_from_me = are_jids_same_user(attrs.get('participant') or attrs.get('from'),
                                                 me.get('lid') if is_lid else me.get('id'))
            ignore_jid = attrs['from'] if (not is_node_from_me or is_jid_group(attrs.get('from'))) \
                else attrs.get('recipient')
        if ignore_jid and ignore_jid != S_WHATSAPP_NET and should_ignore_jid(ignore_jid):
            await send_message_ack(node, NACK_REASONS['UnhandledError'] if type_ == 'message' else None)
            return
        is_offline = bool(node['attrs'].get('offline'))
        if is_offline:
            offline_node_processor['enqueue'](type_, node)
        else:
            await process_node_with_buffer(node, identifier, exec_)

    # recv a message
    ws.on('CB:message', lambda node: process_node('message', node, 'processing message', handle_message))
    ws.on('CB:call', lambda node: process_node('call', node, 'handling call', handle_call))
    ws.on('CB:receipt', lambda node: process_node('receipt', node, 'handling receipt', handle_receipt))
    ws.on('CB:notification', lambda node: process_node('notification', node, 'handling notification',
                                                       handle_notification))

    def on_bad_ack(node: dict):
        async def go():
            try:
                await handle_bad_ack(node)
            except Exception as error:
                on_unexpected_error(error, 'handling bad ack')
        asyncio.ensure_future(go())
    ws.on('CB:ack,class:message', on_bad_ack)

    async def on_call_event(calls: list):
        if not calls:
            return
        call = calls[0]
        # missed call + group call notification message generation
        if call['status'] == 'timeout' or (call['status'] == 'offer' and call.get('isGroup')):
            msg = {'key': {'remoteJid': call['chatId'], 'id': call['id'], 'fromMe': False},
                   'messageTimestamp': unix_timestamp_seconds(call['date'])}
            if call['status'] == 'timeout':
                if call.get('isGroup'):
                    msg['messageStubType'] = WAMessageStubType.CALL_MISSED_GROUP_VIDEO if call.get('isVideo') \
                        else WAMessageStubType.CALL_MISSED_GROUP_VOICE
                else:
                    msg['messageStubType'] = WAMessageStubType.CALL_MISSED_VIDEO if call.get('isVideo') \
                        else WAMessageStubType.CALL_MISSED_VOICE
            else:
                msg['message'] = {'call': {'callKey': call['id'].encode()}}
            proto_msg = normalize(proto.WebMessageInfo, msg)
            await upsert_message(proto_msg, 'append' if call.get('offline') else 'notify')
    ev.on('call', on_call_event)

    # timestamp of last tctoken prune run — throttles to once per 24h
    prune_state = {'lastTcTokenPruneTs': 0}

    async def prune_expired_tc_tokens():
        try:
            await tc_token_index_loaded
            # Union with the persisted index picks up JIDs added by other layers (history sync)
            persisted = await read_tc_token_index(auth_state['keys'])
            all_jids = dict.fromkeys([*tc_token_known_jids, *persisted])
            if not all_jids:
                return
            jids = list(all_jids)
            all_tokens = await auth_state['keys'].get('tctoken', jids)
            writes: dict = {}
            survivors: dict = {}
            mutated = 0
            for jid in jids:
                entry = all_tokens.get(jid)
                if not entry:
                    mutated += 1  # Tracked but nothing in store — drop from index.
                    continue
                has_peer_token = bool(entry.get('token'))
                peer_token_expired = has_peer_token and is_tc_token_expired(entry.get('timestamp'))
                has_sender_ts = entry.get('senderTimestamp') is not None
                sender_ts_expired = has_sender_ts and is_tc_token_expired(entry.get('senderTimestamp'))
                keep_peer_token = has_peer_token and not peer_token_expired
                keep_sender_ts = has_sender_ts and not sender_ts_expired
                if not keep_peer_token and not keep_sender_ts:
                    writes[jid] = None
                    mutated += 1
                elif peer_token_expired and keep_sender_ts:
                    writes[jid] = {'token': b'', 'senderTimestamp': entry['senderTimestamp']}
                    survivors[jid] = None
                    mutated += 1
                else:
                    survivors[jid] = None
            if mutated == 0:
                return
            await auth_state['keys'].set({'tctoken': {
                **writes, TC_TOKEN_INDEX_KEY: {'token': json.dumps(list(survivors)).encode()}}})
            tc_token_known_jids.clear()
            tc_token_known_jids.update(survivors.keys())
            logger.debug({'mutated': mutated, 'remaining': len(survivors)}, 'pruned expired tctokens')
        except Exception as err:
            logger.warn({'err': str(err)}, 'failed to prune expired tctokens')

    def on_connection_update(update: dict):
        is_online, connection = update.get('isOnline'), update.get('connection')
        if is_online is not None:
            st['sendActiveReceipts'] = is_online
            logger.trace(f'sendActiveReceipts set to "{st["sendActiveReceipts"]}"')
        # Flush pending tctoken index save on disconnect to avoid writing after close
        if connection == 'close' and tc_state['indexTimer']:
            tc_state['indexTimer'].cancel()
            tc_state['indexTimer'] = None

            async def best_effort():
                try:
                    await flush_tc_token_index()
                except Exception:
                    pass
            asyncio.ensure_future(best_effort())
        # Prune expired tctokens when coming online, at most once per 24 hours (matches WA Web's CLEAN_TC_TOKENS)
        if is_online:
            now = time.time() * 1000
            DAY_MS = 24 * 60 * 60 * 1000
            if now - prune_state['lastTcTokenPruneTs'] >= DAY_MS:
                prune_state['lastTcTokenPruneTs'] = now
                asyncio.ensure_future(prune_expired_tc_tokens())
    ev.on('connection.update', on_connection_update)

    def on_socket_end(_error=None):
        if not config.get('msgRetryCounterCache') and hasattr(msg_retry_cache, 'close'):
            msg_retry_cache.close()
        if not config.get('callOfferCache') and hasattr(call_offer_cache, 'close'):
            call_offer_cache.close()
        identity_assert_debounce.close()
        st['sendActiveReceipts'] = False
    register_socket_end_handler(on_socket_end)

    return sock.extend(send_message_ack=send_message_ack, send_retry_request=send_retry_request,
                       reject_call=reject_call, fetch_message_history=fetch_message_history,
                       request_placeholder_resend=request_placeholder_resend,
                       message_retry_manager=message_retry_manager)
