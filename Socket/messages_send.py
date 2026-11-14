from __future__ import annotations
import asyncio
import base64
import json
import time
from datetime import datetime
from typing import Optional

from ..Defaults import DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL
from ..Types import WAMessageStatus
from ..Utils.auth_utils import assert_me_id
from ..Utils.cache import TTLCache
from ..Utils.generics import (bind_wait_for_event, encode_newsletter_message, encode_wa_message,
                              generate_message_id_v2, generate_participant_hash_v2, unix_timestamp_seconds)
from ..Utils.link_preview import get_url_info
from ..Utils.make_mutex import make_keyed_mutex, make_mutex
from ..Utils.message_retry_manager import MessageRetryManager
from ..Utils.messages import (aggregate_message_keys_not_from_me, assert_media_content, generate_wa_message,
                              normalize_message_content)
from ..Utils.messages_media import (DEF_MEDIA_HOST, decrypt_media_retry_data, encrypt_media_retry_request,
                                    get_status_code_for_media_retry, get_url_from_direct_path,
                                    get_wa_upload_to_server)
from ..Utils.proto_utils import from_object, normalize
from ..Utils.reporting_utils import get_message_reporting_token, should_include_reporting_token
from ..Utils.signal import extract_device_jids, parse_and_inject_e2e_sessions
from ..Utils.tc_token_utils import (build_merged_tc_token_index_write, is_tc_token_expired, resolve_issuance_jid,
                                    resolve_tc_token_jid, should_send_new_tc_token, store_tc_tokens_from_iq_result)
from ..Utils.validate_connection import encode_signed_device_identity
from ..WABinary import (Boom, are_jids_same_user, get_binary_node_child, get_binary_node_children, is_hosted_lid_user,
                        is_hosted_pn_user, is_jid_bot, is_jid_group, is_jid_meta_ai, is_lid_user, is_pn_user,
                        jid_decode, jid_encode, jid_normalized_user, PSA_WID, S_WHATSAPP_NET)
from ..WABinary.jid_utils import FullJid
from ..WAProto import WAProto_pb2 as proto
from ..WAUSync import USyncQuery, USyncUser
from .newsletter import make_newsletter_socket


def make_messages_socket(config: dict):
    logger = config['logger']
    link_preview_image_thumbnail_width = config['linkPreviewImageThumbnailWidth']
    generate_high_quality_link_preview = config['generateHighQualityLinkPreview']
    http_request_options = config.get('options')
    patch_message_before_sending = config['patchMessageBeforeSending']
    cached_group_metadata = config.get('cachedGroupMetadata')
    enable_recent_message_cache = config['enableRecentMessageCache']
    max_msg_retry_count = config['maxMsgRetryCount']

    sock = make_newsletter_socket(config)
    ev, auth_state, message_mutex = sock.ev, sock.auth_state, sock.message_mutex
    signal_repository, upsert_message, query = sock.signal_repository, sock.upsert_message, sock.query
    fetch_privacy_settings, send_node = sock.fetch_privacy_settings, sock.send_node
    group_metadata, group_toggle_ephemeral = sock.group_metadata, sock.group_toggle_ephemeral
    register_socket_end_handler = sock.register_socket_end_handler

    get_lid_for_pn = signal_repository.lid_mapping.get_lid_for_pn

    # Set of tctoken storage JIDs with a fire-and-forget issuePrivacyTokens IQ in flight.
    in_flight_tc_token_issuance: set = set()

    user_devices_cache = config.get('userDevicesCache') or TTLCache(ttl=DEFAULT_CACHE_TTLS['USER_DEVICES'])
    # Serializes writes to userDevicesCache across USync refresh and device-notification handling.
    devices_mutex = make_mutex()

    # Initialize message retry manager if enabled
    message_retry_manager = MessageRetryManager(logger, max_msg_retry_count) if enable_recent_message_cache else None

    # Prevent race conditions in Signal session encryption by user
    encryption_mutex = make_keyed_mutex()

    state = {'mediaConn': None, 'mediaHost': DEF_MEDIA_HOST}

    async def _fetch_media_conn():
        result = await query({'tag': 'iq', 'attrs': {'type': 'set', 'xmlns': 'w:m', 'to': S_WHATSAPP_NET},
                              'content': [{'tag': 'media_conn', 'attrs': {}}]})
        media_conn_node = get_binary_node_child(result, 'media_conn')
        # TODO: explore full length of data that whatsapp provides
        node = {'hosts': [{'hostname': h['attrs'].get('hostname'),
                           'maxContentLengthBytes': int(h['attrs'].get('maxContentLengthBytes') or 0)}
                          for h in get_binary_node_children(media_conn_node, 'host')],
                'auth': media_conn_node['attrs'].get('auth'), 'ttl': int(media_conn_node['attrs'].get('ttl') or 0),
                'fetchDate': datetime.now()}
        logger.debug('fetched media conn')
        if node['hosts']:
            state['mediaHost'] = node['hosts'][0]['hostname']
        return node

    async def refresh_media_conn(force_get: bool = False) -> dict:
        media = await state['mediaConn'] if state['mediaConn'] else None
        if not media or force_get or (datetime.now() - media['fetchDate']).total_seconds() > media['ttl']:
            state['mediaConn'] = asyncio.ensure_future(_fetch_media_conn())
        return await state['mediaConn']

    async def send_receipt(jid: str, participant: Optional[str], message_ids: list, type_: Optional[str]):
        """generic send receipt function; used for receipts of phone call, read, delivery etc."""
        if not message_ids:
            raise Boom('missing ids in receipt')
        node = {'tag': 'receipt', 'attrs': {'id': message_ids[0]}}
        is_read_receipt = type_ in ('read', 'read-self')
        if is_read_receipt:
            node['attrs']['t'] = str(unix_timestamp_seconds())
        if type_ == 'sender' and (is_pn_user(jid) or is_lid_user(jid)):
            node['attrs']['recipient'] = jid
            node['attrs']['to'] = participant
        else:
            node['attrs']['to'] = jid
            if participant:
                node['attrs']['participant'] = participant
        if type_:
            node['attrs']['type'] = type_
        remaining = message_ids[1:]
        if remaining:
            node['content'] = [{'tag': 'list', 'attrs': {},
                                'content': [{'tag': 'item', 'attrs': {'id': i}} for i in remaining]}]
        logger.debug({'attrs': node['attrs'], 'messageIds': message_ids}, 'sending receipt for messages')
        await send_node(node)

    async def send_receipts(keys: list, type_: Optional[str]):
        """Correctly bulk send receipts to multiple chats, participants"""
        for r in aggregate_message_keys_not_from_me(keys):
            await send_receipt(r['jid'], r['participant'], r['messageIds'], type_)

    async def read_messages(keys: list):
        """Bulk read messages. Keys can be from different chats & participants"""
        privacy_settings = await fetch_privacy_settings()
        # based on privacy settings, we have to change the read type
        read_type = 'read' if privacy_settings.get('readreceipts') == 'all' else 'read-self'
        await send_receipts(keys, read_type)

    async def get_usync_devices(jids: list, use_cache: bool, ignore_zero_devices: bool) -> list:
        """Fetch all the devices we've to send a message to. Returns FullJid-like entries with a `jid` field."""
        device_results: list = []
        if not use_cache:
            logger.debug('not using cache for devices')
        to_fetch: list = []
        jids_with_user = []
        for jid in jids:
            decoded = jid_decode(jid)
            user = decoded.user if decoded else None
            device = decoded.device if decoded else None
            is_explicit_device = isinstance(device, int) and device >= 0
            if is_explicit_device and user:
                device_results.append({'user': user, 'device': device, 'jid': jid})
                continue
            jid = jid_normalized_user(jid)
            jids_with_user.append({'jid': jid, 'user': user})

        for item in jids_with_user:
            jid, user = item['jid'], item['user']
            if use_cache:
                devices = user_devices_cache.get(user)
                if hasattr(devices, '__await__'):
                    devices = await devices
                if devices:
                    for d in devices:
                        device_results.append({**d.to_dict(), 'jid': jid_encode(d.user, d.server, d.device)}
                                              if isinstance(d, FullJid) else {**d, 'jid': jid_encode(
                                                  d['user'], d['server'], d.get('device'))})
                    logger.trace({'user': user}, 'using cache for devices')
                else:
                    to_fetch.append(jid)
            else:
                to_fetch.append(jid)

        if not to_fetch:
            return device_results

        requested_lid_users = set()
        for jid in to_fetch:
            if is_lid_user(jid) or is_hosted_lid_user(jid):
                u = jid_decode(jid).user if jid_decode(jid) else None
                if u:
                    requested_lid_users.add(u)

        usync_query = USyncQuery().with_context('message').with_device_protocol().with_lid_protocol()
        for jid in to_fetch:
            # todo: investigate - the idea here is that <user> should have an inline lid field with the lid being
            # the pn equivalent
            usync_query.with_user(USyncUser().with_id(jid))

        result = await sock.execute_usync_query(usync_query)
        if result:
            # TODO: LID MAP this stuff (lid protocol will now return lid with devices)
            lid_results = [a for a in result['list'] if a.get('lid')]
            if lid_results:
                logger.trace('Storing LID maps from device call')
                await signal_repository.lid_mapping.store_lid_pn_mappings(
                    [{'lid': a['lid'], 'pn': a['id']} for a in lid_results])
                # Force-refresh sessions for newly mapped LIDs to align identity addressing
                try:
                    lids = [a['lid'] for a in lid_results]
                    if lids:
                        await assert_sessions(lids, True)
                except Exception as e:
                    logger.warn({'e': e, 'count': len(lid_results)}, 'failed to assert sessions for newly mapped LIDs')

            me = auth_state['creds']['me']
            extracted = extract_device_jids(result['list'], me['id'], me.get('lid'), ignore_zero_devices)
            device_map: dict = {}
            for item in extracted:
                device_map.setdefault(item.user, []).append(item)

            # Process each user's devices as a group for bulk LID migration
            for user, user_devices in device_map.items():
                is_lid = user in requested_lid_users
                # Process all devices for this user
                for item in user_devices:
                    final_jid = jid_encode(user, item.server, item.device) if is_lid else \
                        jid_encode(item.user, item.server, item.device)
                    device_results.append({**item.to_dict(), 'jid': final_jid})
                    logger.debug({'user': item.user, 'device': item.device, 'finalJid': final_jid,
                                  'usedLid': is_lid}, 'Processed device with LID priority')

            async def write_cache():
                for user, user_devices in device_map.items():
                    r = user_devices_cache.set(user, user_devices)
                    if hasattr(r, '__await__'):
                        await r
            await devices_mutex.mutex(write_cache)

        user_device_updates = {}
        for user_id, devs in device_map.items():
            if devs:
                user_device_updates[user_id] = [str(d.device) if d.device else '0' for d in devs]
        if user_device_updates:
            try:
                await auth_state['keys'].set({'device-list': user_device_updates})
                logger.debug({'userCount': len(user_device_updates)}, 'stored user device lists for bulk migration')
            except Exception as error:
                logger.warn({'error': error}, 'failed to store user device lists')
        return device_results

    def update_member_label(jid: str, member_label: str):
        """Update Member Label"""
        return relay_message(jid, {'protocolMessage': {
            'type': proto.Message.ProtocolMessage.GROUP_MEMBER_LABEL_CHANGE,
            'memberLabel': {'label': (member_label or '')[:30], 'labelTimestamp': unix_timestamp_seconds()}}},
            {'additionalNodes': [{'tag': 'meta', 'attrs': {'tag_reason': 'user_update', 'appdata': 'member_tag'},
                                  'content': None}]})

    async def assert_sessions(jids: list, force: Optional[bool] = None) -> bool:
        did_fetch_new_session = False
        unique_jids = list(dict.fromkeys(jids))
        jids_requiring_fetch = []
        logger.debug({'jids': jids}, 'assertSessions call with jids')
        for jid in unique_jids:
            if not force:
                session_validation = await signal_repository.validate_session(jid)
                if session_validation.get('exists'):
                    continue
            jids_requiring_fetch.append(jid)
        if jids_requiring_fetch:
            # LID if mapped, otherwise original
            mapped = await signal_repository.lid_mapping.get_lids_for_pns(
                [j for j in jids_requiring_fetch if is_pn_user(j) or is_hosted_pn_user(j)])
            wire_jids = [*[j for j in jids_requiring_fetch if is_lid_user(j) or is_hosted_lid_user(j)],
                         *[a['lid'] for a in (mapped or [])]]
            logger.debug({'jidsRequiringFetch': jids_requiring_fetch, 'wireJids': wire_jids}, 'fetching sessions')
            users = []
            for jid in wire_jids:
                attrs = {'jid': jid}
                if force:
                    attrs['reason'] = 'identity'
                users.append({'tag': 'user', 'attrs': attrs})
            result = await query({'tag': 'iq', 'attrs': {'xmlns': 'encrypt', 'type': 'get', 'to': S_WHATSAPP_NET},
                                  'content': [{'tag': 'key', 'attrs': {}, 'content': users}]})
            await parse_and_inject_e2e_sessions(result, signal_repository)
            did_fetch_new_session = True
        return did_fetch_new_session

    async def send_peer_data_operation_message(pdo_message: dict) -> str:
        # TODO: abstract the logic to send a Peer Message instead of just PDO
        if not (auth_state['creds'].get('me') or {}).get('id'):
            raise Boom('Not authenticated')
        protocol_message = {'protocolMessage': {
            'peerDataOperationRequestMessage': pdo_message,
            'type': proto.Message.ProtocolMessage.PEER_DATA_OPERATION_REQUEST_MESSAGE}}
        me_jid = jid_normalized_user(auth_state['creds']['me']['id'])
        return await relay_message(me_jid, protocol_message, {
            'additionalAttributes': {'category': 'peer', 'push_priority': 'high_force'},
            'additionalNodes': [{'tag': 'meta', 'attrs': {'appdata': 'default'}}]})

    async def create_participant_nodes(recipient_jids: list, message: dict, extra_attrs: Optional[dict] = None,
                                       dsm_message: Optional[dict] = None) -> dict:
        if not recipient_jids:
            return {'nodes': [], 'shouldIncludeDeviceIdentity': False}
        patched = patch_message_before_sending(message, recipient_jids)
        if asyncio.iscoroutine(patched):
            patched = await patched
        patched_messages = patched if isinstance(patched, list) else \
            [{'recipientJid': jid, 'message': patched} for jid in recipient_jids]
        flags = {'shouldIncludeDeviceIdentity': False}
        me_id = auth_state['creds']['me']['id']
        me_lid = (auth_state['creds'].get('me') or {}).get('lid')
        me_lid_user = jid_decode(me_lid).user if me_lid and jid_decode(me_lid) else None

        async def encrypt_for(item):
            jid = item.get('recipientJid')
            patched_message = item.get('message')
            try:
                if not jid:
                    return None
                msg_to_encrypt = patched_message
                if dsm_message:
                    target_user = jid_decode(jid).user
                    own_pn_user = jid_decode(me_id).user
                    is_own_user = target_user == own_pn_user or (me_lid_user and target_user == me_lid_user)
                    is_exact_sender_device = jid == me_id or (me_lid and jid == me_lid)
                    if is_own_user and not is_exact_sender_device:
                        msg_to_encrypt = dsm_message
                        logger.debug({'jid': jid, 'targetUser': target_user}, 'Using DSM for own device')
                data = encode_wa_message(msg_to_encrypt)

                async def do_encrypt():
                    r = await signal_repository.encrypt_message(jid=jid, data=data)
                    if r['type'] == 'pkmsg':
                        flags['shouldIncludeDeviceIdentity'] = True
                    return {'tag': 'to', 'attrs': {'jid': jid},
                            'content': [{'tag': 'enc', 'attrs': {'v': '2', 'type': r['type'], **(extra_attrs or {})},
                                         'content': r['ciphertext']}]}
                return await encryption_mutex.mutex(jid, do_encrypt)
            except Exception as err:
                logger.error({'jid': jid, 'err': err}, 'Failed to encrypt for recipient')
                return None

        results = await asyncio.gather(*[encrypt_for(i) for i in patched_messages])
        nodes = [n for n in results if n is not None]
        if len(recipient_jids) > 0 and len(nodes) == 0:
            raise Boom('All encryptions failed', status_code=500)
        return {'nodes': nodes, 'shouldIncludeDeviceIdentity': flags['shouldIncludeDeviceIdentity']}

    def get_media_type(message: dict) -> str:
        if message.get('imageMessage'):
            return 'image'
        if message.get('videoMessage'):
            return 'gif' if message['videoMessage'].get('gifPlayback') else 'video'
        if message.get('audioMessage'):
            return 'ptt' if message['audioMessage'].get('ptt') else 'audio'
        if message.get('contactMessage'):
            return 'vcard'
        if message.get('documentMessage'):
            return 'document'
        if message.get('contactsArrayMessage'):
            return 'contact_array'
        if message.get('liveLocationMessage'):
            return 'livelocation'
        if message.get('stickerMessage'):
            return 'sticker'
        if message.get('listMessage'):
            return 'list'
        if message.get('listResponseMessage'):
            return 'list_response'
        if message.get('buttonsResponseMessage'):
            return 'buttons_response'
        if message.get('orderMessage'):
            return 'order'
        if message.get('productMessage'):
            return 'product'
        if message.get('interactiveResponseMessage'):
            return 'native_flow_response'
        if message.get('groupInviteMessage'):
            return 'url'
        return ''

    def get_message_type(message: dict) -> str:
        normalized = normalize_message_content(message)
        if not normalized:
            return 'text'
        if normalized.get('reactionMessage') or normalized.get('encReactionMessage'):
            return 'reaction'
        if (normalized.get('pollCreationMessage') or normalized.get('pollCreationMessageV2') or
                normalized.get('pollCreationMessageV3') or normalized.get('pollUpdateMessage')):
            return 'poll'
        if normalized.get('eventMessage'):
            return 'event'
        if get_media_type(normalized) != '':
            return 'media'
        return 'text'

    async def issue_privacy_tokens(jids: list, timestamp: Optional[int] = None):
        t = str(timestamp if timestamp is not None else unix_timestamp_seconds())
        return await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'privacy'},
                            'content': [{'tag': 'tokens', 'attrs': {}, 'content': [
                                {'tag': 'token', 'attrs': {'jid': jid_normalized_user(j), 't': t,
                                                           'type': 'trusted_contact'}} for j in jids]}]})

    async def relay_message(jid: str, message: dict, options: Optional[dict] = None) -> str:
        options = options or {}
        msg_id = options.get('messageId')
        participant = options.get('participant')
        additional_attributes = options.get('additionalAttributes')
        additional_nodes = options.get('additionalNodes')
        use_user_devices_cache = options.get('useUserDevicesCache')
        use_cached_group_metadata = options.get('useCachedGroupMetadata')
        status_jid_list = options.get('statusJidList')

        me_id = assert_me_id(auth_state['creds'])
        me_lid = (auth_state['creds'].get('me') or {}).get('lid')
        is_retry_resend = bool(participant and participant.get('jid'))
        flags = {'shouldIncludeDeviceIdentity': is_retry_resend}
        status_jid = 'status@broadcast'

        decoded = jid_decode(jid)
        user, server = decoded.user, decoded.server
        is_group = server == 'g.us'
        is_status = jid == status_jid
        is_lid = server == 'lid'
        is_newsletter = server == 'newsletter'
        is_group_or_status = is_group or is_status
        final_jid = jid

        msg_id = msg_id or generate_message_id_v2(me_id)
        use_user_devices_cache = use_user_devices_cache is not False
        use_cached_group_metadata = use_cached_group_metadata is not False and not is_status

        participants: list = []
        destination_jid = final_jid if not is_status else status_jid
        binary_node_content: list = []
        devices: list = []
        holder = {'reportingMessage': None}

        me_msg = {'deviceSentMessage': {'destinationJid': destination_jid, 'message': message},
                  'messageContextInfo': message.get('messageContextInfo')}
        extra_attrs: dict = {}

        if participant:
            if not is_group and not is_status:
                additional_attributes = {**(additional_attributes or {}), 'device_fanout': 'false'}
            pd = jid_decode(participant['jid'])
            devices.append({'user': pd.user, 'device': pd.device, 'jid': participant['jid']})

        async def tx():
            nonlocal additional_attributes
            media_type = get_media_type(message)
            if media_type:
                extra_attrs['mediatype'] = media_type

            if is_newsletter:
                patched = patch_message_before_sending(message, []) if patch_message_before_sending else message
                if asyncio.iscoroutine(patched):
                    patched = await patched
                data = encode_newsletter_message(patched)
                binary_node_content.append({'tag': 'plaintext', 'attrs': {}, 'content': data})
                stanza = {'tag': 'message', 'attrs': {'to': jid, 'id': msg_id, 'type': get_message_type(message),
                                                      **(additional_attributes or {})},
                          'content': binary_node_content}
                logger.debug({'msgId': msg_id}, f'sending newsletter message to {jid}')
                await send_node(stanza)
                return

            nm = normalize_message_content(message) or {}
            if nm.get('pinInChatMessage') or nm.get('reactionMessage'):
                extra_attrs['decrypt-fail'] = 'hide'  # todo: expand for reactions and other types

            if is_group_or_status and not is_retry_resend:
                async def get_group_data():
                    group_data = await cached_group_metadata(jid) if (use_cached_group_metadata and cached_group_metadata) \
                        else None
                    if group_data and isinstance(group_data.get('participants'), list):
                        logger.trace({'jid': jid, 'participants': len(group_data['participants'])},
                                     'using cached group metadata')
                    elif not is_status:
                        group_data = await group_metadata(jid)
                    return group_data

                async def get_sender_key_map():
                    if not participant and not is_status:
                        result = await auth_state['keys'].get('sender-key-memory', [jid])
                        return result.get(jid) or {}
                    return {}
                group_data, sender_key_map = await asyncio.gather(get_group_data(), get_sender_key_map())

                participants_list = [p['id'] for p in group_data['participants']] if group_data else []
                if group_data and (group_data.get('ephemeralDuration') or 0) > 0:
                    additional_attributes = {**(additional_attributes or {}),
                                             'expiration': str(group_data['ephemeralDuration'])}
                if is_status and status_jid_list:
                    participants_list.extend(status_jid_list)
                additional_devices = await get_usync_devices(participants_list, bool(use_user_devices_cache), False)
                devices.extend(additional_devices)

                if is_group:
                    additional_attributes = {**(additional_attributes or {}),
                                             'addressing_mode': (group_data or {}).get('addressingMode') or 'lid'}

                patched = patch_message_before_sending(message)
                if asyncio.iscoroutine(patched):
                    patched = await patched
                if isinstance(patched, list):
                    raise Boom('Per-jid patching is not supported in groups')
                data = encode_wa_message(patched)
                holder['reportingMessage'] = patched
                group_addressing_mode = (additional_attributes or {}).get('addressing_mode') or \
                    (group_data or {}).get('addressingMode') or 'lid'
                group_sender_identity = me_lid if (group_addressing_mode == 'lid' and me_lid) else me_id

                r = await signal_repository.encrypt_group_message(group=destination_jid, data=data,
                                                                   me_id=group_sender_identity)
                ciphertext, sender_key_distribution_message = r['ciphertext'], r['senderKeyDistributionMessage']

                sender_key_recipients = []
                for device in devices:
                    device_jid = device['jid']
                    has_key = bool(sender_key_map.get(device_jid))
                    if ((not has_key or bool(participant)) and not is_hosted_lid_user(device_jid)
                            and not is_hosted_pn_user(device_jid) and device.get('device') != 99):
                        # todo: revamp all this logic
                        sender_key_recipients.append(device_jid)
                        sender_key_map[device_jid] = True

                if sender_key_recipients:
                    logger.debug({'senderKeyJids': sender_key_recipients}, 'sending new sender key')
                    sender_key_msg = {'senderKeyDistributionMessage': {
                        'axolotlSenderKeyDistributionMessage': sender_key_distribution_message,
                        'groupId': destination_jid}}
                    await assert_sessions(sender_key_recipients)
                    res = await create_participant_nodes(sender_key_recipients, sender_key_msg, extra_attrs)
                    flags['shouldIncludeDeviceIdentity'] = flags['shouldIncludeDeviceIdentity'] or \
                        res['shouldIncludeDeviceIdentity']
                    participants.extend(res['nodes'])

                binary_node_content.append({'tag': 'enc', 'attrs': {'v': '2', 'type': 'skmsg', **extra_attrs},
                                            'content': ciphertext})
                await auth_state['keys'].set({'sender-key-memory': {jid: sender_key_map}})
            else:
                # ADDRESSING CONSISTENCY: Match own identity to conversation context
                own_id = me_id
                if is_lid and me_lid:
                    own_id = me_lid
                    logger.debug({'to': jid, 'ownId': own_id}, 'Using LID identity for @lid conversation')
                else:
                    logger.debug({'to': jid, 'ownId': own_id}, 'Using PN identity for @s.whatsapp.net conversation')
                own_user = jid_decode(own_id).user
                if not participant:
                    patched_for_reporting = patch_message_before_sending(message, [jid])
                    if asyncio.iscoroutine(patched_for_reporting):
                        patched_for_reporting = await patched_for_reporting
                    if isinstance(patched_for_reporting, list):
                        holder['reportingMessage'] = next((i for i in patched_for_reporting
                                                           if i.get('recipientJid') == jid), patched_for_reporting[0])
                    else:
                        holder['reportingMessage'] = patched_for_reporting

                if not is_retry_resend:
                    target_user_server = 'lid' if is_lid else 's.whatsapp.net'
                    devices.append({'user': user, 'device': 0, 'jid': jid_encode(user, target_user_server, 0)})
                    if user != own_user:
                        own_user_server = 'lid' if is_lid else 's.whatsapp.net'
                        own_user_for_addressing = jid_decode(me_lid).user if (is_lid and me_lid) \
                            else jid_decode(me_id).user
                        devices.append({'user': own_user_for_addressing, 'device': 0,
                                        'jid': jid_encode(own_user_for_addressing, own_user_server, 0)})
                    if (additional_attributes or {}).get('category') != 'peer':
                        # Clear placeholders and enumerate actual devices
                        devices.clear()
                        # Use conversation-appropriate sender identity
                        sender_identity = jid_encode(jid_decode(me_lid).user, 'lid', None) if (is_lid and me_lid) \
                            else jid_encode(jid_decode(me_id).user, 's.whatsapp.net', None)
                        # Enumerate devices for sender and target with consistent addressing
                        session_devices = await get_usync_devices([sender_identity, jid], True, False)
                        devices.extend(session_devices)
                        logger.debug({'deviceCount': len(devices)}, 'Device enumeration complete with unified addressing')

                all_recipients, me_recipients, other_recipients = [], [], []
                me_pn_user = jid_decode(me_id).user
                me_lid_user = jid_decode(me_lid).user if me_lid else None
                for d in devices:
                    d_user, d_jid = d['user'], d['jid']
                    is_exact_sender_device = d_jid == me_id or (me_lid and d_jid == me_lid)
                    if is_exact_sender_device:
                        logger.debug({'jid': d_jid, 'meId': me_id, 'meLid': me_lid},
                                     'Skipping exact sender device (whatsmeow pattern)')
                        continue
                    # Check if this is our device (could match either PN or LID user)
                    is_me = d_user == me_pn_user or d_user == me_lid_user
                    (me_recipients if is_me else other_recipients).append(d_jid)
                    all_recipients.append(d_jid)

                await assert_sessions(all_recipients)
                me_res, other_res = await asyncio.gather(
                    # For own devices: use DSM if available (1:1 chats only)
                    create_participant_nodes(me_recipients, me_msg or message, extra_attrs),
                    create_participant_nodes(other_recipients, message, extra_attrs, me_msg))
                participants.extend(me_res['nodes'])
                participants.extend(other_res['nodes'])
                if me_recipients or other_recipients:
                    extra_attrs['phash'] = generate_participant_hash_v2([*me_recipients, *other_recipients])
                flags['shouldIncludeDeviceIdentity'] = (flags['shouldIncludeDeviceIdentity'] or
                                                        me_res['shouldIncludeDeviceIdentity'] or
                                                        other_res['shouldIncludeDeviceIdentity'])

            if is_retry_resend:
                is_participant_lid = is_lid_user(participant['jid'])
                is_me = are_jids_same_user(participant['jid'], me_lid if is_participant_lid else me_id)
                message_to_send = message
                if is_group_or_status:
                    group_sender_identity = None
                    if me_lid and await signal_repository.has_sender_key(group=destination_jid, me_id=me_lid):
                        group_sender_identity = me_lid
                    elif await signal_repository.has_sender_key(group=destination_jid, me_id=me_id):
                        group_sender_identity = me_id
                    if group_sender_identity:
                        try:
                            skdm = await signal_repository.get_sender_key_distribution_message(
                                group=destination_jid, me_id=group_sender_identity)
                            message_to_send = {**message, 'senderKeyDistributionMessage': {
                                'groupId': destination_jid, 'axolotlSenderKeyDistributionMessage': skdm}}
                        except Exception as err:
                            logger.warn({'err': err, 'jid': destination_jid},
                                        'failed to build SKDM for retry, sending without it')
                encoded = encode_wa_message({'deviceSentMessage': {'destinationJid': destination_jid,
                                                                    'message': message_to_send}}) if is_me \
                    else encode_wa_message(message_to_send)
                r = await signal_repository.encrypt_message(data=encoded, jid=participant['jid'])
                binary_node_content.append({'tag': 'enc', 'attrs': {'v': '2', 'type': r['type'],
                                                                    'count': str(participant['count'])},
                                            'content': r['ciphertext']})

            if participants:
                if (additional_attributes or {}).get('category') == 'peer':
                    first = participants[0].get('content')
                    peer_node = first[0] if first else None
                    if peer_node:
                        binary_node_content.append(peer_node)  # push only enc
                else:
                    binary_node_content.append({'tag': 'participants', 'attrs': {}, 'content': participants})

            stanza = {'tag': 'message', 'attrs': {'id': msg_id, 'to': destination_jid, 'type': get_message_type(message),
                                                  **(additional_attributes or {})},
                      'content': binary_node_content}

            # if the participant to send to is explicitly specified (generally retry recp) ensure the message is only
            # sent to that person; if a retry receipt is sent to everyone -- it'll fail decryption for everyone else
            if participant:
                if is_jid_group(destination_jid):
                    stanza['attrs']['to'] = destination_jid
                    stanza['attrs']['participant'] = participant['jid']
                elif are_jids_same_user(participant['jid'], me_id):
                    stanza['attrs']['to'] = participant['jid']
                    stanza['attrs']['recipient'] = destination_jid
                else:
                    stanza['attrs']['to'] = participant['jid']
            else:
                stanza['attrs']['to'] = destination_jid

            if flags['shouldIncludeDeviceIdentity']:
                stanza['content'].append({'tag': 'device-identity', 'attrs': {},
                                          'content': encode_signed_device_identity(auth_state['creds']['account'], True)})
                logger.debug({'jid': jid}, 'adding device identity')

            reporting_message = holder['reportingMessage']
            if (not is_newsletter and not is_retry_resend and reporting_message and
                    (reporting_message.get('messageContextInfo') or {}).get('messageSecret') and
                    should_include_reporting_token(reporting_message)):
                try:
                    encoded = encode_wa_message(reporting_message)
                    reporting_key = {'id': msg_id, 'fromMe': True, 'remoteJid': destination_jid,
                                     'participant': participant.get('jid') if participant else None}
                    reporting_node = await get_message_reporting_token(encoded, reporting_message, reporting_key)
                    if reporting_node:
                        stanza['content'].append(reporting_node)
                        logger.trace({'jid': jid}, 'added reporting token to message')
                except Exception as error:
                    logger.warn({'jid': jid, 'trace': str(error)}, 'failed to attach reporting token')

            # WA Web never attaches tctoken to peer (AppStateSync) messages — server rejects with 479
            is_peer_message = (additional_attributes or {}).get('category') == 'peer'
            is_1on1_send = not is_group and not is_retry_resend and not is_status and not is_newsletter \
                and not is_peer_message

            # Resolve destination to LID for tctoken storage — matches Signal session key pattern
            tc_token_jid = await resolve_tc_token_jid(destination_jid, get_lid_for_pn) if is_1on1_send \
                else destination_jid
            contact_tc_token_data = await auth_state['keys'].get('tctoken', [tc_token_jid]) if is_1on1_send else {}
            existing_token_entry = contact_tc_token_data.get(tc_token_jid)
            tc_token_buffer = existing_token_entry.get('token') if existing_token_entry else None

            # Treat expired tokens the same as missing — clear from cache
            if tc_token_buffer and is_tc_token_expired(existing_token_entry.get('timestamp')):
                logger.debug({'jid': destination_jid, 'timestamp': existing_token_entry.get('timestamp')},
                             'tctoken expired, clearing')
                tc_token_buffer = None
                # Preserve senderTimestamp so the fire-and-forget issuance dedupe survives cleanup.
                cleared = {'token': b'', 'senderTimestamp': existing_token_entry['senderTimestamp']} \
                    if existing_token_entry.get('senderTimestamp') is not None else None
                try:
                    await auth_state['keys'].set({'tctoken': {tc_token_jid: cleared}})
                except Exception as err:
                    logger.debug({'jid': destination_jid, 'err': str(err)}, 'failed to persist tctoken expiry cleanup')

            if tc_token_buffer and sock.server_props['privacyTokenOn1to1']:
                stanza['content'].append({'tag': 'tctoken', 'attrs': {}, 'content': tc_token_buffer})

            if additional_nodes:
                stanza['content'].extend(additional_nodes)

            logger.debug({'msgId': msg_id}, f'sending message to {len(participants)} devices')
            await send_node(stanza)

            # Fire-and-forget: issue our token to the contact AFTER message send.
            # WA Web skips protocol messages and PSA/bot contacts (TcTokenChatAction: isRegularUser)
            is_protocol_msg = bool((normalize_message_content(message) or {}).get('protocolMessage'))
            is_bot_or_psa = destination_jid == PSA_WID or is_jid_bot(destination_jid) or is_jid_meta_ai(destination_jid)
            if (is_1on1_send and not is_protocol_msg and not is_bot_or_psa and
                    should_send_new_tc_token((existing_token_entry or {}).get('senderTimestamp')) and
                    tc_token_jid not in in_flight_tc_token_issuance):
                in_flight_tc_token_issuance.add(tc_token_jid)
                issue_timestamp = unix_timestamp_seconds()
                get_pn_for_lid = signal_repository.lid_mapping.get_pn_for_lid

                async def issue():
                    try:
                        issue_jid = await resolve_issuance_jid(destination_jid,
                                                               sock.server_props['lidTrustedTokenIssueToLid'],
                                                               get_lid_for_pn, get_pn_for_lid)
                        result = await issue_privacy_tokens([issue_jid], issue_timestamp)
                        await store_tc_tokens_from_iq_result(result, tc_token_jid, auth_state['keys'], get_lid_for_pn)
                        current_data = await auth_state['keys'].get('tctoken', [tc_token_jid])
                        current_entry = current_data.get(tc_token_jid)
                        index_write = await build_merged_tc_token_index_write(auth_state['keys'], [tc_token_jid])
                        await auth_state['keys'].set({'tctoken': {
                            tc_token_jid: {'token': b'', **(current_entry or {}), 'senderTimestamp': issue_timestamp},
                            **index_write}})
                    except Exception as err:
                        logger.debug({'jid': destination_jid, 'err': str(err)},
                                     'fire-and-forget tctoken issuance failed')
                    finally:
                        in_flight_tc_token_issuance.discard(tc_token_jid)
                asyncio.ensure_future(issue())

            # Add message to retry cache if enabled
            if message_retry_manager and not participant:
                message_retry_manager.add_recent_message(destination_jid, msg_id, message)

        await auth_state['keys'].transaction(tx, me_id)
        return msg_id

    wa_upload_to_server = get_wa_upload_to_server(config, refresh_media_conn)
    wait_for_msg_media_update = bind_wait_for_event(ev, 'messages.media-update')

    def on_socket_end(_error=None):
        if not config.get('userDevicesCache') and hasattr(user_devices_cache, 'close'):
            user_devices_cache.close()
        state['mediaConn'] = None
        if message_retry_manager:
            message_retry_manager.clear()
    register_socket_end_handler(on_socket_end)

    async def update_media_message(message: dict) -> dict:
        content = assert_media_content(message.get('message'))
        media_key = content['mediaKey']
        me_id = auth_state['creds']['me']['id']
        node = encrypt_media_retry_request(message['key'], media_key, me_id)
        holder = {'error': None}

        async def check(update: list):
            result = next((c for c in update if c['key'].get('id') == message['key'].get('id')), None)
            if result:
                if result.get('error'):
                    holder['error'] = result['error']
                else:
                    try:
                        media = decrypt_media_retry_data(result['media'], media_key, result['key']['id'])
                        if media.result != proto.MediaRetryNotification.SUCCESS:
                            result_str = proto.MediaRetryNotification.ResultType.Name(media.result)
                            raise Boom(f'Media re-upload failed by device ({result_str})',
                                       data=str(media),
                                       status_code=get_status_code_for_media_retry(media.result) or 404)
                        content['directPath'] = media.directPath
                        content['url'] = get_url_from_direct_path(content['directPath'], state['mediaHost'])
                        logger.debug({'directPath': media.directPath, 'key': result['key']}, 'media update successful')
                    except Exception as err:
                        holder['error'] = err
                return True
            return None
        await asyncio.gather(send_node(node), wait_for_msg_media_update(check))
        if holder['error']:
            raise holder['error']
        ev.emit('messages.update', [{'key': message['key'], 'update': {'message': message['message']}}])
        return message

    async def send_message(jid: str, content: dict, options: Optional[dict] = None):
        options = options or {}
        user_jid = auth_state['creds']['me']['id']
        if isinstance(content, dict) and content.get('disappearingMessagesInChat') is not None \
                and 'disappearingMessagesInChat' in content and is_jid_group(jid):
            v = content['disappearingMessagesInChat']
            value = (WA_DEFAULT_EPHEMERAL if v else 0) if isinstance(v, bool) else v
            await group_toggle_ephemeral(jid, value)
            return None

        async def get_url_info_fn(text):
            return await get_url_info(text, {
                'thumbnailWidth': link_preview_image_thumbnail_width,
                'fetchOpts': {'timeout': 3_000, **(http_request_options or {})}, 'logger': logger,
                'uploadImage': wa_upload_to_server if generate_high_quality_link_preview else None})

        full_msg = await generate_wa_message(jid, content, {
            'logger': logger, 'userJid': user_jid, 'getUrlInfo': get_url_info_fn,
            # TODO: CACHE
            'getProfilePicUrl': sock.profile_picture_url, 'getCallLink': sock.create_call_link,
            'upload': wa_upload_to_server, 'mediaCache': config.get('mediaCache'), 'options': config.get('options'),
            'messageId': generate_message_id_v2((sock.user or {}).get('id')), **options})
        is_event_msg = bool(content.get('event'))
        is_delete_msg = bool(content.get('delete'))
        is_edit_msg = bool(content.get('edit'))
        is_pin_msg = bool(content.get('pin'))
        is_poll_message = bool(content.get('poll'))
        additional_attributes: dict = {}
        additional_nodes: list = []
        # required for delete
        if is_delete_msg:
            # if the chat is a group, and I am not the author, then delete the message as an admin
            if is_jid_group((content['delete'] or {}).get('remoteJid')) and not content['delete'].get('fromMe'):
                additional_attributes['edit'] = '8'
            else:
                additional_attributes['edit'] = '7'
        elif is_edit_msg:
            additional_attributes['edit'] = '1'
        elif is_pin_msg:
            additional_attributes['edit'] = '2'
        elif is_poll_message:
            additional_nodes.append({'tag': 'meta', 'attrs': {'polltype': 'creation'}})
        elif is_event_msg:
            additional_nodes.append({'tag': 'meta', 'attrs': {'event_type': 'creation'}})

        await relay_message(jid, full_msg['message'], {
            'messageId': full_msg['key']['id'], 'useCachedGroupMetadata': options.get('useCachedGroupMetadata'),
            'additionalAttributes': additional_attributes, 'statusJidList': options.get('statusJidList'),
            'additionalNodes': additional_nodes})
        if config.get('emitOwnEvents'):
            async def own():
                await message_mutex.mutex(lambda: upsert_message(full_msg, 'append'))
            asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(own()))
        return full_msg

    return sock.extend(
        user_devices_cache=user_devices_cache, devices_mutex=devices_mutex, issue_privacy_tokens=issue_privacy_tokens,
        assert_sessions=assert_sessions, relay_message=relay_message, send_receipt=send_receipt,
        send_receipts=send_receipts, read_messages=read_messages, refresh_media_conn=refresh_media_conn,
        # Function (not getter) so the live closure binding is preserved across layers.
        get_media_host=lambda: state['mediaHost'], wa_upload_to_server=wa_upload_to_server,
        fetch_privacy_settings=fetch_privacy_settings, send_peer_data_operation_message=send_peer_data_operation_message,
        create_participant_nodes=create_participant_nodes, get_usync_devices=get_usync_devices,
        message_retry_manager=message_retry_manager, update_member_label=update_member_label,
        update_media_message=update_media_message, send_message=send_message)
