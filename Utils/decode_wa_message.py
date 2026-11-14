from __future__ import annotations
from typing import Optional
from ..WABinary import (Boom, are_jids_same_user, is_hosted_lid_user, is_hosted_pn_user, is_jid_broadcast,
                        is_jid_group, is_jid_meta_ai, is_jid_newsletter, is_jid_status_broadcast, is_lid_user,
                        is_pn_user)
from ..WAProto import WAProto_pb2 as proto
from .generics import unpad_random_max16
from .proto_utils import to_dict


async def get_decryption_jid(sender: str, repository) -> str:
    if is_lid_user(sender) or is_hosted_lid_user(sender):
        return sender
    mapped = await repository.lid_mapping.get_lid_for_pn(sender)
    return mapped or sender


async def _store_mapping_from_envelope(stanza: dict, sender: str, repository, decryption_jid: str, logger):
    # TODO: Handle hosted IDs
    sender_alt = extract_addressing_context(stanza)['senderAlt']
    if sender_alt and is_lid_user(sender_alt) and is_pn_user(sender) and decryption_jid == sender:
        try:
            await repository.lid_mapping.store_lid_pn_mappings([{'lid': sender_alt, 'pn': sender}])
            await repository.migrate_session(sender, sender_alt)
            logger.debug({'sender': sender, 'senderAlt': sender_alt}, 'Stored LID mapping from envelope')
        except Exception as error:
            logger.warn({'sender': sender, 'senderAlt': sender_alt, 'error': error}, 'Failed to store LID mapping')


NO_MESSAGE_FOUND_ERROR_TEXT = 'Message absent from node'
MISSING_KEYS_ERROR_TEXT = 'Key used already or never filled'
ACCOUNT_RESTRICTED_TEXT = 'Your account has been restricted'

# Retry configuration for failed decryption
DECRYPTION_RETRY_CONFIG = {'maxRetries': 3, 'baseDelayMs': 100,
                           'sessionRecordErrors': ['No session record', 'SessionError: No session record']}

# NACK reason codes we send to the server (client -> server)
NACK_REASONS = {
    'SenderReachoutTimelocked': 463, 'ParsingError': 487, 'UnrecognizedStanza': 488, 'UnrecognizedStanzaClass': 489,
    'UnrecognizedStanzaType': 490, 'InvalidProtobuf': 491, 'InvalidHostedCompanionStanza': 493,
    'MissingMessageSecret': 495, 'SignalErrorOldCounter': 496, 'MessageDeletedOnPeer': 499, 'UnhandledError': 500,
    'UnsupportedAdminRevoke': 550, 'UnsupportedLIDGroup': 551, 'DBOperationFailed': 552,
}

# Server-side error codes returned in ack stanzas (server -> client) that we have dedicated handlers for.
SERVER_ERROR_CODES = {'MessageAccountRestriction': '463', 'SmaxInvalid': '479'}


def extract_addressing_context(stanza: dict) -> dict:
    attrs = stanza['attrs']
    sender = attrs.get('participant') or attrs.get('from')
    addressing_mode = attrs.get('addressing_mode') or ('lid' if (sender or '').endswith('lid') else 'pn')
    if addressing_mode == 'lid':
        # Message is LID-addressed: sender is LID, extract corresponding PN without device data
        sender_alt = attrs.get('participant_pn') or attrs.get('sender_pn') or attrs.get('peer_recipient_pn')
        recipient_alt = attrs.get('recipient_pn')
    else:
        # Message is PN-addressed: sender is PN, extract corresponding LID without device data
        sender_alt = attrs.get('participant_lid') or attrs.get('sender_lid') or attrs.get('peer_recipient_lid')
        recipient_alt = attrs.get('recipient_lid')
    return {'addressingMode': addressing_mode, 'senderAlt': sender_alt, 'recipientAlt': recipient_alt}


def decode_message_node(stanza: dict, me_id: str, me_lid: str) -> dict:
    """Decode the received node as a message. Note: this only parses the message, it does not decrypt it."""
    attrs = stanza['attrs']
    from_me = False
    msg_id, from_ = attrs.get('id'), attrs.get('from')
    participant, recipient = attrs.get('participant'), attrs.get('recipient')
    if not msg_id:
        raise Boom('Invalid message stanza: missing id attribute', data=stanza)
    if not from_:
        raise Boom('Invalid message stanza: missing from attribute', data=stanza)
    addressing_context = extract_addressing_context(stanza)
    is_me = lambda jid: are_jids_same_user(jid, me_id)
    is_me_lid = lambda jid: are_jids_same_user(jid, me_lid)

    if is_pn_user(from_) or is_lid_user(from_) or is_hosted_lid_user(from_) or is_hosted_pn_user(from_):
        if recipient and not is_jid_meta_ai(recipient):
            if not is_me(from_) and not is_me_lid(from_):
                raise Boom('receipient present, but msg not from me', data=stanza)
            if is_me(from_) or is_me_lid(from_):
                from_me = True
            chat_id = recipient
        else:
            # Peer-routed self stanzas (history sync, app-state sync, etc.) arrive with `from` set to our own
            # device but no `recipient` attribute — still mark as fromMe so self-only handlers run.
            if is_me(from_) or is_me_lid(from_):
                from_me = True
            chat_id = from_
        msg_type = 'chat'
        author = from_
    elif is_jid_group(from_):
        if not participant:
            raise Boom('No participant in group message')
        if is_me(participant) or is_me_lid(participant):
            from_me = True
        msg_type = 'group'
        author = participant
        chat_id = from_
    elif is_jid_broadcast(from_):
        if not participant:
            raise Boom('No participant in group message')
        is_participant_me = is_me(participant)
        if is_jid_status_broadcast(from_):
            msg_type = 'direct_peer_status' if is_participant_me else 'other_status'
        else:
            msg_type = 'peer_broadcast' if is_participant_me else 'other_broadcast'
        from_me = is_participant_me
        chat_id = from_
        author = participant
    elif is_jid_newsletter(from_):
        msg_type = 'newsletter'
        chat_id = from_
        author = from_
        if is_me(from_) or is_me_lid(from_):
            from_me = True
    else:
        raise Boom('Unknown message type', data=stanza)

    pushname = attrs.get('notify')
    key = {
        'remoteJid': chat_id,
        'remoteJidAlt': addressing_context['senderAlt'] if not is_jid_group(chat_id) else None,
        'remoteJidUsername': (attrs.get('peer_recipient_username') or attrs.get('recipient_username'))
        if not is_jid_group(chat_id) else None,
        'fromMe': from_me,
        'id': msg_id,
        'participant': participant,
        'participantAlt': addressing_context['senderAlt'] if is_jid_group(chat_id) else None,
        'participantUsername': attrs.get('participant_username') if attrs.get('participant') else None,
        'addressingMode': addressing_context['addressingMode'],
    }
    if msg_type == 'newsletter' and attrs.get('server_id'):
        key['server_id'] = attrs['server_id']
    key = {k: v for k, v in key.items() if v is not None}

    full_message = {'key': key, 'category': attrs.get('category'), 'messageTimestamp': int(attrs.get('t') or 0),
                    'pushName': pushname, 'broadcast': is_jid_broadcast(from_)}
    full_message = {k: v for k, v in full_message.items() if v is not None}
    if key.get('fromMe'):
        full_message['status'] = proto.WebMessageInfo.SERVER_ACK
    return {'fullMessage': full_message, 'author': author, 'sender': author if msg_type == 'chat' else chat_id}


def decrypt_message_node(stanza: dict, me_id: str, me_lid: str, repository, logger) -> dict:
    d = decode_message_node(stanza, me_id, me_lid)
    full_message, author, sender = d['fullMessage'], d['author'], d['sender']

    async def decrypt():
        decryptables = 0
        if isinstance(stanza.get('content'), list):
            for child in stanza['content']:
                tag, attrs, content = child['tag'], child['attrs'], child.get('content')
                if tag == 'verified_name' and isinstance(content, (bytes, bytearray)):
                    cert = proto.VerifiedNameCertificate()
                    cert.ParseFromString(bytes(content))
                    details = proto.VerifiedNameCertificate.Details()
                    details.ParseFromString(cert.details)
                    full_message['verifiedBizName'] = details.verifiedName
                if tag == 'unavailable' and attrs.get('type') == 'view_once':
                    full_message['key']['isViewOnce'] = True  # TODO: remove from here and add a STUB TYPE
                if attrs.get('count') and tag == 'enc':
                    full_message['retryCount'] = int(attrs['count'])
                if tag != 'enc' and tag != 'plaintext':
                    continue
                if not isinstance(content, (bytes, bytearray)):
                    continue
                decryptables += 1
                decryption_jid = await get_decryption_jid(author, repository)
                if tag != 'plaintext':
                    # TODO: Handle hosted devices
                    await _store_mapping_from_envelope(stanza, author, repository, decryption_jid, logger)
                try:
                    e2e_type = 'plaintext' if tag == 'plaintext' else attrs.get('type')
                    if e2e_type == 'skmsg':
                        msg_buffer = await repository.decrypt_group_message(group=sender, author_jid=author,
                                                                            msg=content)
                    elif e2e_type in ('pkmsg', 'msg'):
                        msg_buffer = await repository.decrypt_message(jid=decryption_jid, type=e2e_type,
                                                                      ciphertext=content)
                    elif e2e_type == 'plaintext':
                        msg_buffer = content
                    else:
                        raise ValueError(f'Unknown e2e type: {e2e_type}')
                    m = proto.Message()
                    m.ParseFromString(bytes(unpad_random_max16(msg_buffer) if e2e_type != 'plaintext' else msg_buffer))
                    msg = to_dict(m)
                    msg = (msg.get('deviceSentMessage') or {}).get('message') or msg
                    if msg.get('senderKeyDistributionMessage'):
                        try:
                            await repository.process_sender_key_distribution_message(
                                author_jid=author, item=msg['senderKeyDistributionMessage'])
                        except Exception as err:
                            logger.error({'key': full_message['key'], 'err': err},
                                         'failed to process sender key distribution message')
                    if full_message.get('message'):
                        full_message['message'].update(msg)
                    else:
                        full_message['message'] = msg
                except Exception as err:
                    logger.error({'key': full_message['key'], 'err': err,
                                  'messageType': 'plaintext' if tag == 'plaintext' else attrs.get('type'),
                                  'sender': sender, 'author': author,
                                  'isSessionRecordError': _is_session_record_error(err)}, 'failed to decrypt message')
                    full_message['messageStubType'] = proto.WebMessageInfo.CIPHERTEXT
                    full_message['messageStubParameters'] = [str(err)]
        # if nothing was found to decrypt
        if not decryptables and not (full_message.get('key') or {}).get('isViewOnce'):
            full_message['messageStubType'] = proto.WebMessageInfo.CIPHERTEXT
            full_message['messageStubParameters'] = [NO_MESSAGE_FOUND_ERROR_TEXT]

    return {'fullMessage': full_message, 'category': stanza['attrs'].get('category'), 'author': author,
            'decrypt': decrypt}


def _is_session_record_error(error) -> bool:
    msg = str(error) or ''
    return any(p in msg for p in DECRYPTION_RETRY_CONFIG['sessionRecordErrors'])
