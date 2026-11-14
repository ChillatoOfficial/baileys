from __future__ import annotations
import zlib
from typing import Optional
from ..Types import WAMessageStubType
from ..WABinary import is_hosted_lid_user, is_hosted_pn_user, is_lid_user, is_pn_user
from ..WAProto import WAProto_pb2 as proto
from .generics import to_number
from .messages import normalize_message_content
from .messages_media import download_content_from_message, to_buffer
from .proto_utils import to_dict


def _extract_pn_from_messages(messages: list) -> Optional[str]:
    for msg_item in messages:
        message = msg_item.get('message')
        # Only extract from outgoing messages (fromMe: true) in 1:1 chats
        if not message or not (message.get('key') or {}).get('fromMe') or not message.get('userReceipt'):
            continue
        user_jid = message['userReceipt'][0].get('userJid')
        if user_jid and (is_pn_user(user_jid) or is_hosted_pn_user(user_jid)):
            return user_jid
    return None


async def download_history(msg: dict, options: Optional[dict] = None):
    stream = await download_content_from_message(msg, 'md-msg-hist', {'options': options or {}})
    inflater = zlib.decompressobj()
    chunks = []
    async for chunk in stream:
        chunks.append(inflater.decompress(chunk))
    chunks.append(inflater.flush())
    sync = proto.HistorySync()
    sync.ParseFromString(b''.join(chunks))
    return to_dict(sync)


def process_history_message(item: dict, logger=None) -> dict:
    messages, contacts, chats, lid_pn_mappings = [], [], [], []
    if logger:
        logger.trace({'progress': item.get('progress')}, 'processing history of type ' + str(item.get('syncType')))
    # Extract LID-PN mappings for all sync types
    for m in item.get('phoneNumberToLidMappings') or []:
        if m.get('lidJid') and m.get('pnJid'):
            lid_pn_mappings.append({'lid': m['lidJid'], 'pn': m['pnJid']})

    HST = proto.HistorySync
    sync_type = item.get('syncType')
    if sync_type in (HST.INITIAL_BOOTSTRAP, HST.RECENT, HST.FULL, HST.ON_DEMAND):
        for chat in item.get('conversations') or []:
            contacts.append({'id': chat['id'],
                             'name': chat.get('displayName') or chat.get('name') or chat.get('username') or None,
                             'username': chat.get('username') or None,
                             'lid': chat.get('lidJid') or chat.get('accountLid') or None,
                             'phoneNumber': chat.get('pnJid') or None})
            chat_id = chat['id']
            is_lid = is_lid_user(chat_id) or is_hosted_lid_user(chat_id)
            is_pn = is_pn_user(chat_id) or is_hosted_pn_user(chat_id)
            if is_lid and chat.get('pnJid'):
                lid_pn_mappings.append({'lid': chat_id, 'pn': chat['pnJid']})
            elif is_pn and chat.get('lidJid'):
                lid_pn_mappings.append({'lid': chat['lidJid'], 'pn': chat_id})
            elif is_lid and not chat.get('pnJid'):
                # Fallback: extract PN from userReceipt in messages when pnJid is missing
                pn_from_receipt = _extract_pn_from_messages(chat.get('messages') or [])
                if pn_from_receipt:
                    lid_pn_mappings.append({'lid': chat_id, 'pn': pn_from_receipt})

            msgs = chat.pop('messages', None) or []
            for it in msgs:
                message = it['message']
                messages.append(message)
                if not chat.get('messages'):
                    chat['messages'] = [{'message': message}]  # keep only the most recent message
                if not message['key'].get('fromMe') and not chat.get('lastMessageRecvTimestamp'):
                    chat['lastMessageRecvTimestamp'] = to_number(message.get('messageTimestamp'))
                if message.get('messageStubType') in (WAMessageStubType.BIZ_PRIVACY_MODE_TO_BSP,
                                                      WAMessageStubType.BIZ_PRIVACY_MODE_TO_FB) \
                        and (message.get('messageStubParameters') or [None])[0]:
                    contacts.append({'id': message['key'].get('participant') or message['key'].get('remoteJid'),
                                     'verifiedName': message['messageStubParameters'][0]})
            chats.append(chat)
    elif sync_type == HST.PUSH_NAME:
        for c in item.get('pushnames') or []:
            contacts.append({'id': c['id'], 'notify': c.get('pushname')})

    return {'chats': chats, 'contacts': contacts, 'messages': messages, 'lidPnMappings': lid_pn_mappings,
            'pastParticipants': item.get('pastParticipants'), 'syncType': sync_type,
            'progress': item.get('progress')}


async def download_and_process_history_sync_notification(msg: dict, options: Optional[dict] = None, logger=None):
    if msg.get('initialHistBootstrapInlinePayload'):
        h = proto.HistorySync()
        h.ParseFromString(zlib.decompress(bytes(msg['initialHistBootstrapInlinePayload'])))
        history_msg = to_dict(h)
    else:
        history_msg = await download_history(msg, options)
    return process_history_message(history_msg, logger)


def get_history_msg(message: dict) -> Optional[dict]:
    normalized = normalize_message_content(message) if message else None
    return ((normalized or {}).get('protocolMessage') or {}).get('historySyncNotification')
