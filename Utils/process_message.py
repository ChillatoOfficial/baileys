from __future__ import annotations
import json
from typing import Optional

from ..Types import WAMessageStubType
from ..WABinary import (Boom, are_jids_same_user, is_hosted_lid_user, is_hosted_pn_user, is_jid_broadcast,
                        is_jid_status_broadcast, is_lid_user, jid_decode, jid_encode, jid_normalized_user)
from ..WAProto import WAProto_pb2 as proto
from .crypto import aes_decrypt_gcm, hmac_sign
from .generics import get_key_author, to_number
from .history import download_and_process_history_sync_notification
from .messages import get_content_type, normalize_message_content
from .proto_utils import to_dict
from .tc_token_utils import build_merged_tc_token_index_write, resolve_tc_token_jid

_ST = WAMessageStubType
REAL_MSG_STUB_TYPES = {_ST.CALL_MISSED_GROUP_VIDEO, _ST.CALL_MISSED_GROUP_VOICE, _ST.CALL_MISSED_VIDEO,
                       _ST.CALL_MISSED_VOICE}
REAL_MSG_REQ_ME_STUB_TYPES = {_ST.GROUP_PARTICIPANT_ADD}
PM = proto.Message.ProtocolMessage


async def _store_tc_tokens_from_history_sync(chats: list, signal_repository, key_store, logger=None):
    get_lid_for_pn = signal_repository.lid_mapping.get_lid_for_pn
    candidates = []
    for chat in chats:
        ts = to_number(chat.get('tcTokenTimestamp')) if chat.get('tcTokenTimestamp') else 0
        if chat.get('tcToken') and ts > 0:
            jid = jid_normalized_user(chat['id'])
            storage_jid = await resolve_tc_token_jid(jid, get_lid_for_pn)
            candidates.append({'storageJid': storage_jid, 'token': bytes(chat['tcToken']), 'ts': ts,
                               'senderTs': to_number(chat['tcTokenSenderTimestamp'])
                               if chat.get('tcTokenSenderTimestamp') else None})
    if not candidates:
        return
    jids = [c['storageJid'] for c in candidates]
    existing = await key_store.get('tctoken', jids)
    entries = {}
    for c in candidates:
        existing_entry = existing.get(c['storageJid'])
        existing_ts = int(existing_entry['timestamp']) if existing_entry and existing_entry.get('timestamp') else 0
        if existing_ts > 0 and existing_ts >= c['ts']:
            continue
        entries[c['storageJid']] = {**(existing_entry or {}), 'token': c['token'], 'timestamp': str(c['ts']),
                                    **({'senderTimestamp': c['senderTs']} if c['senderTs'] is not None else {})}
    if entries:
        if logger:
            logger.debug({'count': len(entries)}, 'storing tctokens from history sync')
        try:
            # Include updated __index so cross-session pruning picks these JIDs up.
            index_write = await build_merged_tc_token_index_write(key_store, list(entries.keys()))
            await key_store.set({'tctoken': {**entries, **index_write}})
        except Exception as err:
            if logger:
                logger.warn({'err': err}, 'failed to store tctokens from history sync')


def clean_message(message: dict, me_id: str, me_lid: str):
    """Cleans a received message to further processing"""
    key = message['key']

    def clean(jid):
        if jid and (is_hosted_pn_user(jid) or is_hosted_lid_user(jid)):
            return jid_encode(jid_decode(jid).user, 's.whatsapp.net' if is_hosted_pn_user(jid) else 'lid')
        return jid_normalized_user(jid) if jid else jid_normalized_user(jid)

    # ensure remoteJid and participant doesn't have device or agent in it
    key['remoteJid'] = clean(key.get('remoteJid'))
    if key.get('participant') is not None or True:
        p = key.get('participant')
        key['participant'] = clean(p) if p else (jid_normalized_user(p) if p is not None else '')

    content = normalize_message_content(message.get('message'))

    def normalise_key(msg_key: dict):
        # if the reaction is from another user we've to correctly map the key to this user's perspective
        if not key.get('fromMe'):
            if not msg_key.get('fromMe'):
                who = msg_key.get('participant') or msg_key.get('remoteJid')
                msg_key['fromMe'] = are_jids_same_user(who, me_id) or are_jids_same_user(who, me_lid)
            else:
                msg_key['fromMe'] = False
            msg_key['remoteJid'] = key['remoteJid']
            msg_key['participant'] = msg_key.get('participant') or key.get('participant')

    if content and content.get('reactionMessage'):
        normalise_key(content['reactionMessage']['key'])
    if content and content.get('pollUpdateMessage'):
        normalise_key(content['pollUpdateMessage']['pollCreationMessageKey'])


def is_real_message(message: dict) -> bool:
    normalized = normalize_message_content(message.get('message'))
    has_some_content = bool(get_content_type(normalized))
    return (bool(normalized) or message.get('messageStubType') in REAL_MSG_STUB_TYPES
            or message.get('messageStubType') in REAL_MSG_REQ_ME_STUB_TYPES) and has_some_content \
        and not (normalized or {}).get('protocolMessage') and not (normalized or {}).get('reactionMessage') \
        and not (normalized or {}).get('pollUpdateMessage')


def should_increment_chat_unread(message: dict) -> bool:
    return not message['key'].get('fromMe') and not message.get('messageStubType')


def get_chat_id(key: dict) -> str:
    """Get the ID of the chat from the given key. For broadcasts it'll be the participant"""
    remote_jid, participant, from_me = key.get('remoteJid'), key.get('participant'), key.get('fromMe')
    if not remote_jid:
        raise Boom('Cannot derive chat id: message key is missing remoteJid',
                   data={'remoteJid': remote_jid, 'participant': participant, 'fromMe': from_me})
    if is_jid_broadcast(remote_jid) and not is_jid_status_broadcast(remote_jid) and not from_me:
        if not participant:
            raise Boom('Cannot derive chat id: broadcast message key is missing participant',
                       data={'remoteJid': remote_jid, 'fromMe': from_me})
        return participant
    return remote_jid


def _decrypt_enc_value(enc: dict, msg_id: str, creator: str, actor: str, enc_key: bytes, label: str) -> bytes:
    sign = msg_id.encode() + creator.encode() + actor.encode() + label.encode() + bytes([1])
    key0 = hmac_sign(enc_key, bytes(32), 'sha256')
    dec_key = hmac_sign(sign, key0, 'sha256')
    aad = f'{msg_id}\u0000{actor}'.encode()
    return aes_decrypt_gcm(enc['encPayload'], dec_key, enc['encIv'], aad)


def decrypt_poll_vote(vote: dict, poll_creator_jid: str, poll_msg_id: str, poll_enc_key: bytes, voter_jid: str):
    """Decrypt a poll vote; returns the PollVoteMessage as dict (selectedOptions are SHA256 hashes)"""
    decrypted = _decrypt_enc_value(vote, poll_msg_id, poll_creator_jid, voter_jid, poll_enc_key, 'Poll Vote')
    m = proto.Message.PollVoteMessage()
    m.ParseFromString(decrypted)
    return to_dict(m)


def decrypt_event_response(response: dict, event_creator_jid: str, event_msg_id: str, event_enc_key: bytes,
                           responder_jid: str):
    decrypted = _decrypt_enc_value(response, event_msg_id, event_creator_jid, responder_jid, event_enc_key,
                                   'Event Response')
    m = proto.Message.EventResponseMessage()
    m.ParseFromString(decrypted)
    return to_dict(m)


async def process_message(message: dict, should_process_history_msg: bool, creds: dict, key_store, ev,
                          signal_repository, get_message, options: Optional[dict] = None, logger=None,
                          placeholder_resend_cache=None):
    me_id = creds['me']['id']
    account_settings = creds.get('accountSettings')

    chat = {'id': jid_normalized_user(get_chat_id(message['key']))}
    is_real_msg = is_real_message(message)
    if is_real_msg:
        chat['messages'] = [{'message': message}]
        chat['conversationTimestamp'] = to_number(message.get('messageTimestamp'))
        # only increment unread count if not CIPHERTEXT and from another person
        if should_increment_chat_unread(message):
            chat['unreadCount'] = (chat.get('unreadCount') or 0) + 1

    content = normalize_message_content(message.get('message'))

    # unarchive chat if it's a real message, or someone reacted to our message and we've the unarchive setting on
    if (is_real_msg or ((content or {}).get('reactionMessage') or {}).get('key', {}).get('fromMe')) \
            and (account_settings or {}).get('unarchiveChats'):
        chat['archived'] = False
        chat['readOnly'] = False

    protocol_msg = (content or {}).get('protocolMessage')
    if protocol_msg:
        # Self-only types (drop if !fromMe): history sync, app state key share, LID migration, PDO response
        SELF_ONLY_TYPES = {PM.HISTORY_SYNC_NOTIFICATION, PM.APP_STATE_SYNC_KEY_SHARE, PM.LID_MIGRATION_MAPPING_SYNC,
                           PM.PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE}
        ptype = protocol_msg.get('type')
        if ptype is not None and ptype in SELF_ONLY_TYPES and not message['key'].get('fromMe'):
            if logger:
                logger.warn({'msgId': message['key'].get('id'), 'type': ptype,
                             'from': message['key'].get('participant') or message['key'].get('remoteJid')},
                            'dropping spoofed self-only protocolMessage from non-self origin')
            return

        if ptype == PM.HISTORY_SYNC_NOTIFICATION:
            hist = protocol_msg['historySyncNotification']
            process = should_process_history_msg
            is_latest = not creds.get('processedHistoryMessages')
            if logger:
                logger.info({'histNotification': hist, 'process': process, 'id': message['key'].get('id'),
                             'isLatest': is_latest}, 'got history notification')
            if process:
                if hist.get('syncType') != proto.HistorySync.ON_DEMAND:
                    ev.emit('creds.update', {'processedHistoryMessages': [
                        *(creds.get('processedHistoryMessages') or []),
                        {'key': message['key'], 'messageTimestamp': message.get('messageTimestamp')}]})
                data = await download_and_process_history_sync_notification(hist, options, logger)
                if data.get('lidPnMappings'):
                    if logger:
                        logger.debug({'count': len(data['lidPnMappings'])}, 'processing LID-PN mappings from history sync')
                    try:
                        await signal_repository.lid_mapping.store_lid_pn_mappings(data['lidPnMappings'])
                    except Exception as err:
                        if logger:
                            logger.warn({'err': err}, 'failed to store LID-PN mappings from history sync')
                await _store_tc_tokens_from_history_sync(data['chats'], signal_repository, key_store, logger)
                ev.emit('messaging-history.set', {
                    **data,
                    'isLatest': is_latest if hist.get('syncType') != proto.HistorySync.ON_DEMAND else None,
                    'chunkOrder': hist.get('chunkOrder'),
                    'peerDataRequestSessionId': hist.get('peerDataRequestSessionId')})
        elif ptype == PM.APP_STATE_SYNC_KEY_SHARE:
            keys = (protocol_msg.get('appStateSyncKeyShare') or {}).get('keys')
            if keys:
                import base64
                state = {'newAppStateSyncKeyId': ''}

                async def work():
                    new_keys = []
                    for k in keys:
                        str_key_id = base64.b64encode(bytes(k['keyId']['keyId'])).decode()
                        new_keys.append(str_key_id)
                        await key_store.set({'app-state-sync-key': {str_key_id: k['keyData']}})
                        state['newAppStateSyncKeyId'] = str_key_id
                    if logger:
                        logger.info({'newAppStateSyncKeyId': state['newAppStateSyncKeyId'], 'newKeys': new_keys},
                                    'injecting new app state sync keys')
                await key_store.transaction(work, me_id)
                ev.emit('creds.update', {'myAppStateKeyId': state['newAppStateSyncKeyId']})
            elif logger:
                logger.info({'protocolMsg': protocol_msg}, 'recv app state sync with 0 keys')
        elif ptype == PM.REVOKE:
            ev.emit('messages.update', [{
                'key': {**message['key'], 'id': protocol_msg['key']['id']},
                'update': {'message': None, 'messageStubType': WAMessageStubType.REVOKE, 'key': message['key']}}])
        elif ptype == PM.EPHEMERAL_SETTING:
            chat['ephemeralSettingTimestamp'] = to_number(message.get('messageTimestamp'))
            chat['ephemeralExpiration'] = protocol_msg.get('ephemeralExpiration') or None
        elif ptype == PM.PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE:
            response = protocol_msg.get('peerDataOperationRequestResponseMessage')
            if response:
                for result in response.get('peerDataOperationResult') or []:
                    retry_response = (result or {}).get('placeholderMessageResendResponse')
                    if not retry_response or not retry_response.get('webMessageInfoBytes'):
                        continue
                    try:
                        wmi = proto.WebMessageInfo()
                        wmi.ParseFromString(bytes(retry_response['webMessageInfoBytes']))
                        web_message_info = to_dict(wmi)
                        msg_id = (web_message_info.get('key') or {}).get('id')
                        # Retrieve cached original message data (preserves LID details, timestamps, etc.)
                        cached = None
                        if msg_id and placeholder_resend_cache:
                            cached = placeholder_resend_cache.get(msg_id)
                            if hasattr(cached, '__await__'):
                                cached = await cached
                        if msg_id and placeholder_resend_cache:
                            r = placeholder_resend_cache.delete(msg_id) if hasattr(placeholder_resend_cache, 'delete') \
                                else placeholder_resend_cache.del_(msg_id)
                            if hasattr(r, '__await__'):
                                await r
                        if cached and isinstance(cached, dict):
                            # Apply decoded message content onto cached metadata (preserves LID etc.)
                            cached['message'] = web_message_info.get('message')
                            if web_message_info.get('messageTimestamp'):
                                cached['messageTimestamp'] = web_message_info['messageTimestamp']
                            final_msg = cached
                        else:
                            final_msg = web_message_info
                        if logger:
                            logger.debug({'msgId': msg_id, 'requestId': response.get('stanzaId')},
                                         'received placeholder resend')
                        ev.emit('messages.upsert', {'messages': [final_msg], 'type': 'notify',
                                                    'requestId': response.get('stanzaId')})
                    except Exception as err:
                        if logger:
                            logger.warn({'err': err, 'stanzaId': response.get('stanzaId')},
                                        'failed to decode placeholder resend response')
        elif ptype == PM.MESSAGE_EDIT:
            ev.emit('messages.update', [{
                # flip the sender / fromMe properties because they're in the perspective of the sender
                'key': {**message['key'], 'id': (protocol_msg.get('key') or {}).get('id')},
                'update': {'message': {'editedMessage': {'message': protocol_msg.get('editedMessage')}},
                           'messageTimestamp': (to_number(protocol_msg['timestampMs']) // 1000)
                           if protocol_msg.get('timestampMs') else message.get('messageTimestamp')}}])
        elif ptype == PM.GROUP_MEMBER_LABEL_CHANGE:
            label_msg = protocol_msg.get('memberLabel')
            if label_msg and label_msg.get('label'):
                ev.emit('group.member-tag.update', {
                    'groupId': chat['id'], 'label': label_msg['label'],
                    'participant': message['key'].get('participant'),
                    'participantAlt': message['key'].get('participantAlt'),
                    'messageTimestamp': int(message.get('messageTimestamp') or 0)})
        elif ptype == PM.LID_MIGRATION_MAPPING_SYNC:
            encoded = (protocol_msg.get('lidMigrationMappingSyncMessage') or {}).get('encodedMappingPayload')
            payload = proto.LIDMigrationMappingSyncPayload()
            payload.ParseFromString(bytes(encoded))
            if logger:
                logger.debug({'pnToLidMappings': len(payload.pnToLidMappings),
                              'chatDbMigrationTimestamp': payload.chatDbMigrationTimestamp},
                             'got lid mappings and chat db migration timestamp')
            pairs = []
            for m in payload.pnToLidMappings:
                lid = m.latestLid or m.assignedLid
                pairs.append({'lid': f'{lid}@lid', 'pn': f'{m.pn}@s.whatsapp.net'})
            await signal_repository.lid_mapping.store_lid_pn_mappings(pairs)
            for p in pairs:
                await signal_repository.migrate_session(p['pn'], p['lid'])
    elif content and content.get('reactionMessage'):
        reaction = {**content['reactionMessage'], 'key': message['key']}
        ev.emit('messages.reaction', [{'reaction': reaction, 'key': content['reactionMessage'].get('key')}])
    elif content and content.get('encEventResponseMessage'):
        enc_event_response = content['encEventResponseMessage']
        creation_msg_key = enc_event_response['eventCreationMessageKey']
        # we need to fetch the event creation message to get the event enc key
        event_msg = await get_message(creation_msg_key)
        if event_msg:
            try:
                me_id_normalised = jid_normalized_user(me_id)
                # all jids need to be PN
                event_creator_key = creation_msg_key.get('participant') or creation_msg_key.get('remoteJid')
                event_creator_pn = await signal_repository.lid_mapping.get_pn_for_lid(event_creator_key) \
                    if is_lid_user(event_creator_key) else event_creator_key
                event_creator_jid = get_key_author({'remoteJid': jid_normalized_user(event_creator_pn),
                                                    'fromMe': me_id_normalised == event_creator_pn}, me_id_normalised)
                responder_jid = get_key_author(message['key'], me_id_normalised)
                event_enc_key = (event_msg.get('messageContextInfo') or {}).get('messageSecret')
                if not event_enc_key:
                    if logger:
                        logger.warn({'creationMsgKey': creation_msg_key},
                                    'event response: missing messageSecret for decryption')
                else:
                    response_msg = decrypt_event_response(enc_event_response, event_creator_jid,
                                                          creation_msg_key['id'], event_enc_key, responder_jid)
                    event_response = {'eventResponseMessageKey': message['key'],
                                      'senderTimestampMs': response_msg.get('timestampMs'),
                                      'response': response_msg}
                    ev.emit('messages.update', [{'key': creation_msg_key,
                                                 'update': {'eventResponses': [event_response]}}])
            except Exception as err:
                if logger:
                    logger.warn({'err': err, 'creationMsgKey': creation_msg_key}, 'failed to decrypt event response')
        elif logger:
            logger.warn({'creationMsgKey': creation_msg_key}, 'event creation message not found, cannot decrypt response')
    elif message.get('messageStubType'):
        jid = message['key'].get('remoteJid')
        stub_params = message.get('messageStubParameters') or []
        state = {'participants': []}

        def emit_participants_update(action):
            ev.emit('group-participants.update', {
                'id': jid, 'author': message['key'].get('participant'),
                'authorPn': message['key'].get('participantAlt'),
                'authorUsername': message['key'].get('participantUsername'),
                'participants': state['participants'], 'action': action})

        def emit_group_update(update: dict):
            ev.emit('groups.update', [{'id': jid, **update, 'author': message['key'].get('participant'),
                                       'authorPn': message['key'].get('participantAlt'),
                                       'authorUsername': message['key'].get('participantUsername')}])

        def emit_group_request_join(participant: dict, action, method):
            ev.emit('group.join-request', {
                'id': jid, 'author': message['key'].get('participant'),
                'authorPn': message['key'].get('participantAlt'),
                'authorUsername': message['key'].get('participantUsername'),
                'participant': participant.get('lid'), 'participantPn': participant.get('pn'),
                'action': action, 'method': method})

        def participants_includes_me():
            return any(are_jids_same_user(me_id, p.get('phoneNumber')) for p in state['participants'])

        def parse_participants():
            return [json.loads(a) if isinstance(a, str) else a for a in stub_params]

        st = message['messageStubType']
        if st == _ST.GROUP_PARTICIPANT_CHANGE_NUMBER:
            state['participants'] = parse_participants()
            emit_participants_update('modify')
        elif st in (_ST.GROUP_PARTICIPANT_LEAVE, _ST.GROUP_PARTICIPANT_REMOVE):
            state['participants'] = parse_participants()
            emit_participants_update('remove')
            if participants_includes_me():  # mark the chat read only if you left the group
                chat['readOnly'] = True
        elif st in (_ST.GROUP_PARTICIPANT_ADD, _ST.GROUP_PARTICIPANT_INVITE, _ST.GROUP_PARTICIPANT_ADD_REQUEST_JOIN):
            state['participants'] = parse_participants()
            if participants_includes_me():
                chat['readOnly'] = False
            emit_participants_update('add')
        elif st == _ST.GROUP_PARTICIPANT_DEMOTE:
            state['participants'] = parse_participants()
            emit_participants_update('demote')
        elif st == _ST.GROUP_PARTICIPANT_PROMOTE:
            state['participants'] = parse_participants()
            emit_participants_update('promote')
        elif st == _ST.GROUP_CHANGE_ANNOUNCE:
            v = stub_params[0] if stub_params else None
            emit_group_update({'announce': v in ('true', 'on')})
        elif st == _ST.GROUP_CHANGE_RESTRICT:
            v = stub_params[0] if stub_params else None
            emit_group_update({'restrict': v in ('true', 'on')})
        elif st == _ST.GROUP_CHANGE_SUBJECT:
            name = stub_params[0] if stub_params else None
            chat['name'] = name
            emit_group_update({'subject': name})
        elif st == _ST.GROUP_CHANGE_DESCRIPTION:
            description = stub_params[0] if stub_params else None
            chat['description'] = description
            emit_group_update({'desc': description})
        elif st == _ST.GROUP_CHANGE_INVITE_LINK:
            emit_group_update({'inviteCode': stub_params[0] if stub_params else None})
        elif st == _ST.GROUP_MEMBER_ADD_MODE:
            emit_group_update({'memberAddMode': (stub_params[0] if stub_params else None) == 'all_member_add'})
        elif st == _ST.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE:
            emit_group_update({'joinApprovalMode': (stub_params[0] if stub_params else None) == 'on'})
        elif st == _ST.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD:
            participant = json.loads(stub_params[0])
            emit_group_request_join(participant, stub_params[1] if len(stub_params) > 1 else None,
                                    stub_params[2] if len(stub_params) > 2 else None)

    if len(chat) > 1:
        ev.emit('chats.update', [chat])
