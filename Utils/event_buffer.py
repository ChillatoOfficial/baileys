from __future__ import annotations
import asyncio
import json
from typing import Callable, Optional

from ..Types import WAMessageStatus
from .event_emitter import EventEmitter
from .generics import trim_undefined
from .messages import update_message_with_reaction, update_message_with_receipt
from .process_message import is_real_message, should_increment_chat_unread

BUFFERABLE_EVENT = ['messaging-history.set', 'chats.upsert', 'chats.update', 'chats.delete', 'contacts.upsert',
                    'contacts.update', 'messages.upsert', 'messages.update', 'messages.delete', 'messages.reaction',
                    'message-receipt.update', 'groups.update']
BUFFERABLE_EVENT_SET = set(BUFFERABLE_EVENT)


def _make_buffer_data() -> dict:
    return {
        'historySets': {'chats': {}, 'messages': {}, 'contacts': {}, 'isLatest': False, 'empty': True},
        'chatUpserts': {}, 'chatUpdates': {}, 'chatDeletes': set(), 'contactUpserts': {}, 'contactUpdates': {},
        'messageUpserts': {}, 'messageUpdates': {}, 'messageReactions': {}, 'messageDeletes': {},
        'messageReceipts': {}, 'groupUpdates': {},
    }


def _stringify_message_key(key: dict) -> str:
    return f"{key.get('remoteJid')},{key.get('id')},{'1' if key.get('fromMe') else '0'}"


def _concat_chats(a: dict, b: dict) -> dict:
    if 'unreadCount' in b and b['unreadCount'] is None and (a.get('unreadCount') or 0) < 0:
        # neutralize unread counter
        a['unreadCount'] = None
        b['unreadCount'] = None
    if isinstance(a.get('unreadCount'), (int, float)) and isinstance(b.get('unreadCount'), (int, float)):
        b = dict(b)
        if b['unreadCount'] >= 0:
            b['unreadCount'] = max(b['unreadCount'], 0) + max(a['unreadCount'], 0)
    a.update(b)
    return a


class BufferedEventEmitter(EventEmitter):
    """BaileysBufferableEventEmitter: consolidates events while buffering."""

    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        self._ev = EventEmitter()
        self.history_cache: set = set()
        self.data = _make_buffer_data()
        self._is_buffering = False
        self._buffer_timeout: Optional[asyncio.TimerHandle] = None
        self._flush_pending_timeout: Optional[asyncio.TimerHandle] = None
        self.buffer_count = 0
        self.MAX_HISTORY_CACHE_SIZE = 10000
        self.BUFFER_TIMEOUT_MS = 30000

        # take the generic event and fire it as a baileys event
        def on_event(m: dict):
            for event, d in m.items():
                EventEmitter.emit(self, event, d)
        self._ev.on('event', on_event)

    # --- EventEmitter api (delegating the user-visible listeners to the base class) -------------------------
    def process(self, handler: Callable) -> Callable:
        """Use to process events in a batch; returns an unsubscribe function"""
        async def listener(m):
            r = handler(m)
            if asyncio.iscoroutine(r):
                await r
        self._ev.on('event', listener)
        return lambda: self._ev.off('event', listener)

    def buffer(self):
        """starts buffering events, call flush() to release them"""
        if not self._is_buffering:
            self.logger.debug('Event buffer activated')
            self._is_buffering = True
            self.buffer_count = 0
            if self._buffer_timeout:
                self._buffer_timeout.cancel()

            def on_timeout():
                if self._is_buffering:
                    self.logger.warn('Buffer timeout reached, auto-flushing')
                    self.flush()
            self._buffer_timeout = asyncio.get_running_loop().call_later(self.BUFFER_TIMEOUT_MS / 1000, on_timeout)
        # Always increment count when requested
        self.buffer_count += 1

    def is_buffering(self) -> bool:
        return self._is_buffering

    def flush(self) -> bool:
        """flushes all buffered events; returns True if the flush actually happened"""
        if not self._is_buffering:
            return False
        self.logger.debug({'bufferCount': self.buffer_count}, 'Flushing event buffer')
        self._is_buffering = False
        self.buffer_count = 0
        if self._buffer_timeout:
            self._buffer_timeout.cancel()
            self._buffer_timeout = None
        if self._flush_pending_timeout:
            self._flush_pending_timeout.cancel()
            self._flush_pending_timeout = None
        if len(self.history_cache) > self.MAX_HISTORY_CACHE_SIZE:
            self.logger.debug({'cacheSize': len(self.history_cache)}, 'Clearing history cache')
            self.history_cache.clear()

        new_data = _make_buffer_data()
        conditional_left = 0
        for update in list(self.data['chatUpdates'].values()):
            if update.get('conditional'):
                conditional_left += 1
                new_data['chatUpdates'][update['id']] = update
                del self.data['chatUpdates'][update['id']]
        consolidated = _consolidate_events(self.data)
        if consolidated:
            self._ev.emit('event', consolidated)
        self.data = new_data
        self.logger.trace({'conditionalChatUpdatesLeft': conditional_left}, 'released buffered events')
        return True

    def create_buffered_function(self, work: Callable):
        """buffers all events till the awaited work completes"""
        async def wrapped(*args, **kwargs):
            self.buffer()
            try:
                result = await work(*args, **kwargs)
                # If this is the only buffer, flush after a small delay
                if self.buffer_count == 1:
                    def maybe_flush():
                        if self._is_buffering and self.buffer_count == 1:
                            self.flush()
                    asyncio.get_running_loop().call_later(0.1, maybe_flush)
                return result
            finally:
                self.buffer_count = max(0, self.buffer_count - 1)
                if self.buffer_count == 0:
                    # Only schedule ONE timeout, not 10,000
                    if not self._flush_pending_timeout:
                        self._flush_pending_timeout = asyncio.get_running_loop().call_later(0.1, self.flush)
        return wrapped

    def emit(self, event: str, ev_data=None) -> bool:
        # Check if this is a messages.upsert with a different type than what's buffered
        if event == 'messages.upsert':
            type_ = ev_data['type']
            existing = list(self.data['messageUpserts'].values())
            if existing:
                buffered_type = existing[0]['type']
                if buffered_type != type_:
                    self.logger.debug({'bufferedType': buffered_type, 'newType': type_},
                                      'messages.upsert type mismatch, emitting buffered messages')
                    self._ev.emit('event', {'messages.upsert': {'messages': [m['message'] for m in existing],
                                                                'type': buffered_type}})
                    self.data['messageUpserts'] = {}
        if self._is_buffering and event in BUFFERABLE_EVENT_SET:
            _append(self.data, self.history_cache, event, ev_data, self.logger)
            return True
        return self._ev.emit('event', {event: ev_data})

    def destroy(self):
        if self._buffer_timeout:
            self._buffer_timeout.cancel()
            self._buffer_timeout = None
        if self._flush_pending_timeout:
            self._flush_pending_timeout.cancel()
            self._flush_pending_timeout = None
        self.history_cache.clear()
        self.data = _make_buffer_data()
        self._is_buffering = False
        self.buffer_count = 0
        self.remove_all_listeners()
        self._ev.remove_all_listeners()
        self.logger.debug('Event buffer destroyed')


def make_event_buffer(logger) -> BufferedEventEmitter:
    """The event buffer logically consolidates different events into a single event making data processing efficient."""
    return BufferedEventEmitter(logger)


def _append(data: dict, history_cache: set, event: str, event_data, logger):
    hs = data['historySets']

    def absorbing_chat_update(existing: dict):
        chat_id = existing.get('id') or ''
        update = data['chatUpdates'].get(chat_id)
        if update:
            cond = update.get('conditional')
            matches = cond(data) if cond else True
            if matches:
                update.pop('conditional', None)
                logger.debug({'chatId': chat_id}, 'absorbed chat update in existing chat')
                existing.update(_concat_chats(update, existing))
                data['chatUpdates'].pop(chat_id, None)
            elif matches is False:
                logger.debug({'chatId': chat_id}, 'chat update condition fail, removing')
                data['chatUpdates'].pop(chat_id, None)

    def decrement_chat_read_counter_if_msg_did_unread(message: dict):
        # decrement chat unread counter if the message has already been marked read by us
        chat_id = message['key'].get('remoteJid')
        chat = data['chatUpdates'].get(chat_id) or data['chatUpserts'].get(chat_id)
        if (is_real_message(message) and should_increment_chat_unread(message) and chat
                and isinstance(chat.get('unreadCount'), (int, float)) and chat['unreadCount'] > 0):
            logger.debug({'chatId': chat.get('id')}, 'decrementing chat counter')
            chat['unreadCount'] -= 1
            if chat['unreadCount'] == 0:
                del chat['unreadCount']

    if event == 'messaging-history.set':
        for chat in event_data['chats']:
            id_ = chat.get('id') or ''
            existing_chat = hs['chats'].get(id_)
            if existing_chat:
                existing_chat['endOfHistoryTransferType'] = chat.get('endOfHistoryTransferType')
            if not existing_chat and id_ not in history_cache:
                hs['chats'][id_] = chat
                history_cache.add(id_)
                absorbing_chat_update(chat)
        for contact in event_data['contacts']:
            existing_contact = hs['contacts'].get(contact['id'])
            if existing_contact:
                existing_contact.update(trim_undefined(dict(contact)))
            else:
                history_contact_id = f"c:{contact['id']}"
                has_any_name = contact.get('notify') or contact.get('name') or contact.get('verifiedName')
                if history_contact_id not in history_cache or has_any_name:
                    hs['contacts'][contact['id']] = contact
                    history_cache.add(history_contact_id)
        for message in event_data['messages']:
            key = _stringify_message_key(message['key'])
            if key not in hs['messages'] and key not in history_cache:
                hs['messages'][key] = message
                history_cache.add(key)
        hs['empty'] = False
        hs['syncType'] = event_data.get('syncType')
        if event_data.get('pastParticipants'):
            merged: dict = {}

            def sig_of(p):
                return f"{p.get('userJid') or ''}:{p.get('leaveTs') or ''}:{p.get('leaveReason') or ''}"

            def ingest(entry):
                k = entry.get('groupJid') if entry.get('groupJid') is not None else json.dumps(entry, default=str)
                existing = merged.get(k)
                if not existing:
                    merged[k] = {**entry, 'pastParticipants': list(entry.get('pastParticipants') or [])}
                    return
                seen = {sig_of(p) for p in existing.get('pastParticipants') or []}
                for p in entry.get('pastParticipants') or []:
                    s = sig_of(p)
                    if s not in seen:
                        existing['pastParticipants'].append(p)
                        seen.add(s)
            for entry in hs.get('pastParticipants') or []:
                ingest(entry)
            for entry in event_data['pastParticipants']:
                ingest(entry)
            hs['pastParticipants'] = list(merged.values())
        hs['progress'] = event_data.get('progress')
        hs['chunkOrder'] = event_data.get('chunkOrder')
        hs['peerDataRequestSessionId'] = event_data.get('peerDataRequestSessionId')
        hs['isLatest'] = event_data.get('isLatest') or hs['isLatest']

    elif event == 'chats.upsert':
        for chat in event_data:
            id_ = chat.get('id') or ''
            upsert = data['chatUpserts'].get(id_)
            if id_ and not upsert:
                upsert = hs['chats'].get(id_)
                if upsert:
                    logger.debug({'chatId': id_}, 'absorbed chat upsert in chat set')
            if upsert:
                upsert = _concat_chats(upsert, chat)
            else:
                upsert = chat
                data['chatUpserts'][id_] = upsert
            absorbing_chat_update(upsert)
            data['chatDeletes'].discard(id_)

    elif event == 'chats.update':
        for update in event_data:
            chat_id = update['id']
            cond = update.get('conditional')
            condition_matches = cond(data) if cond else True
            if condition_matches:
                update.pop('conditional', None)
                # if there is an existing upsert, merge the update into it
                upsert = hs['chats'].get(chat_id) or data['chatUpserts'].get(chat_id)
                if upsert:
                    _concat_chats(upsert, update)
                else:
                    chat_update = data['chatUpdates'].get(chat_id) or {}
                    data['chatUpdates'][chat_id] = _concat_chats(chat_update, update)
            elif condition_matches is None:
                # condition yet to be fulfilled
                data['chatUpdates'][chat_id] = update
            # otherwise -- condition not met, update is invalid
            data['chatDeletes'].discard(chat_id)

    elif event == 'chats.delete':
        for chat_id in event_data:
            data['chatDeletes'].add(chat_id)
            # remove any prior updates & upserts
            data['chatUpdates'].pop(chat_id, None)
            data['chatUpserts'].pop(chat_id, None)
            hs['chats'].pop(chat_id, None)

    elif event == 'contacts.upsert':
        for contact in event_data:
            upsert = data['contactUpserts'].get(contact['id'])
            if not upsert:
                upsert = hs['contacts'].get(contact['id'])
                if upsert:
                    logger.debug({'contactId': contact['id']}, 'absorbed contact upsert in contact set')
            if upsert:
                upsert.update(trim_undefined(dict(contact)))
            else:
                upsert = contact
                data['contactUpserts'][contact['id']] = upsert
            if data['contactUpdates'].get(contact['id']):
                upsert = data['contactUpdates'][contact['id']]
                upsert.update(trim_undefined(dict(contact)))
                del data['contactUpdates'][contact['id']]

    elif event == 'contacts.update':
        for update in event_data:
            id_ = update['id']
            # merge into prior upsert
            upsert = hs['contacts'].get(id_) or data['contactUpserts'].get(id_)
            if upsert:
                upsert.update(update)
            else:
                contact_update = data['contactUpdates'].get(id_) or {}
                contact_update.update(update)
                data['contactUpdates'][id_] = contact_update

    elif event == 'messages.upsert':
        messages, type_ = event_data['messages'], event_data['type']
        for message in messages:
            key = _stringify_message_key(message['key'])
            existing = (data['messageUpserts'].get(key) or {}).get('message')
            if not existing:
                existing = hs['messages'].get(key)
                if existing:
                    logger.debug({'messageId': key}, 'absorbed message upsert in message set')
            if existing:
                message['messageTimestamp'] = existing.get('messageTimestamp')
            if data['messageUpdates'].get(key):
                logger.debug('absorbed prior message update in message upsert')
                message.update(data['messageUpdates'][key]['update'])
                del data['messageUpdates'][key]
            if hs['messages'].get(key):
                hs['messages'][key] = message
            else:
                prev_type = (data['messageUpserts'].get(key) or {}).get('type')
                data['messageUpserts'][key] = {
                    'message': message, 'type': 'notify' if (type_ == 'notify' or prev_type == 'notify') else type_}

    elif event == 'messages.update':
        for item in event_data:
            key, update = item['key'], item['update']
            key_str = _stringify_message_key(key)
            existing = hs['messages'].get(key_str) or (data['messageUpserts'].get(key_str) or {}).get('message')
            if existing:
                existing.update(update)
                # if the message was received & read by us the chat counter must have been incremented
                if update.get('status') == WAMessageStatus.READ and not key.get('fromMe'):
                    decrement_chat_read_counter_if_msg_did_unread(existing)
            else:
                msg_update = data['messageUpdates'].get(key_str) or {'key': key, 'update': {}}
                msg_update['update'].update(update)
                data['messageUpdates'][key_str] = msg_update

    elif event == 'messages.delete':
        if 'keys' in event_data:
            for key in event_data['keys']:
                key_str = _stringify_message_key(key)
                if not data['messageDeletes'].get(key_str):
                    data['messageDeletes'][key_str] = key
                data['messageUpserts'].pop(key_str, None)
                data['messageUpdates'].pop(key_str, None)
        # else: TODO: add support

    elif event == 'messages.reaction':
        for item in event_data:
            key, reaction = item['key'], item['reaction']
            key_str = _stringify_message_key(key)
            existing = data['messageUpserts'].get(key_str)
            if existing:
                update_message_with_reaction(existing['message'], reaction)
            else:
                data['messageReactions'].setdefault(key_str, {'key': key, 'reactions': []})
                update_message_with_reaction(data['messageReactions'][key_str], reaction)

    elif event == 'message-receipt.update':
        for item in event_data:
            key, receipt = item['key'], item['receipt']
            key_str = _stringify_message_key(key)
            existing = data['messageUpserts'].get(key_str)
            if existing:
                update_message_with_receipt(existing['message'], receipt)
            else:
                data['messageReceipts'].setdefault(key_str, {'key': key, 'userReceipt': []})
                update_message_with_receipt(data['messageReceipts'][key_str], receipt)

    elif event == 'groups.update':
        for update in event_data:
            id_ = update['id']
            if id_ not in data['groupUpdates']:
                data['groupUpdates'][id_] = dict(update)
    else:
        raise ValueError(f'"{event}" cannot be buffered')


def _consolidate_events(data: dict) -> dict:
    m: dict = {}
    hs = data['historySets']
    if not hs['empty']:
        m['messaging-history.set'] = {
            'chats': list(hs['chats'].values()), 'messages': list(hs['messages'].values()),
            'contacts': list(hs['contacts'].values()), 'pastParticipants': hs.get('pastParticipants'),
            'syncType': hs.get('syncType'), 'progress': hs.get('progress'), 'isLatest': hs['isLatest'],
            'chunkOrder': hs.get('chunkOrder'), 'peerDataRequestSessionId': hs.get('peerDataRequestSessionId')}
    if data['chatUpserts']:
        m['chats.upsert'] = list(data['chatUpserts'].values())
    if data['chatUpdates']:
        m['chats.update'] = list(data['chatUpdates'].values())
    if data['chatDeletes']:
        m['chats.delete'] = list(data['chatDeletes'])
    if data['messageUpserts']:
        lst = list(data['messageUpserts'].values())
        m['messages.upsert'] = {'messages': [x['message'] for x in lst], 'type': lst[0]['type']}
    if data['messageUpdates']:
        m['messages.update'] = list(data['messageUpdates'].values())
    if data['messageDeletes']:
        m['messages.delete'] = {'keys': list(data['messageDeletes'].values())}
    reactions = [{'key': x['key'], 'reaction': r} for x in data['messageReactions'].values() for r in x['reactions']]
    if reactions:
        m['messages.reaction'] = reactions
    receipts = [{'key': x['key'], 'receipt': r} for x in data['messageReceipts'].values() for r in x['userReceipt']]
    if receipts:
        m['message-receipt.update'] = receipts
    if data['contactUpserts']:
        m['contacts.upsert'] = list(data['contactUpserts'].values())
    if data['contactUpdates']:
        m['contacts.update'] = list(data['contactUpdates'].values())
    if data['groupUpdates']:
        m['groups.update'] = list(data['groupUpdates'].values())
    return m
