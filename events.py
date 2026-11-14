"""Typed event payloads for the high level `Client`.

Every raw Baileys event (`'messages.update'`, `'group-participants.update'`, ...) is turned into one
`Event` per affected item, with friendly attributes:

    @client.on_reaction
    async def reaction(e):
        print(e.sender, 'reacted', e.emoji, 'to', e.message_id, 'in', e.chat)

`Event` is a dict, so `e['key']` / `e.get('key')` / the original camelCase keys keep working, and `e.raw` is
the untouched payload.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Optional

_SNAKE = re.compile(r'_([a-z0-9])')

MESSAGE_STATUS = {0: 'error', 1: 'pending', 2: 'sent', 3: 'delivered', 4: 'read', 5: 'played'}


def _camel(name: str) -> str:
    return _SNAKE.sub(lambda m: m.group(1).upper(), name)


class Event(dict):
    """dict with attribute access (snake_case or camelCase); nested dicts are returned as Event too."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith('__'):
            raise AttributeError(name)
        for k in (name, _camel(name), name.rstrip('_')):  # `from_` -> `from`
            if k in self:
                v = dict.__getitem__(self, k)
                return Event(v) if type(v) is dict else v
        return None  # missing fields are None, like optional chaining in JS

    client: Any = None  # the Client that dispatched this event (set by the decorator layer)

    @property
    def raw(self) -> Any:
        return dict.get(self, '_raw', self)

    async def send(self, content, **options):
        """Send to the chat this event is about (`e.chat`) -- e.g. welcome new group members."""
        chat = self.chat
        if self.client is None or not chat:
            raise ValueError('this event has no chat to send to')
        return await self.client.send(chat, content, **options)

    def __repr__(self):
        return f'<Event {dict.__repr__({k: v for k, v in self.items() if k != "_raw"})}>'


# ---------------------------------------------------------------------------------------------------------
# typed subclasses: annotations only (so editors autocomplete `e.`); values still come from Event.__getattr__
# ---------------------------------------------------------------------------------------------------------
class _KeyEvent(Event):
    key: dict
    id: Optional[str]
    chat: Optional[str]
    from_me: bool
    sender: Optional[str]


class ConnectionEvent(Event):
    connection: Optional[str]          # 'connecting' | 'open' | 'close'
    qr: Optional[str]
    last_disconnect: Optional[dict]
    is_new_login: Optional[bool]
    received_pending_notifications: Optional[bool]
    is_online: Optional[bool]


class HistoryEvent(Event):
    chats: list
    contacts: list
    messages: list
    is_latest: Optional[bool]
    progress: Optional[int]
    sync_type: Optional[int]


class MessageUpdateEvent(_KeyEvent):
    update: dict
    status: Optional[str]              # 'error' | 'pending' | 'sent' | 'delivered' | 'read' | 'played'
    message: Optional[dict]            # new content when the message was edited
    poll_updates: Optional[list]
    edited: bool


class MessageDeleteEvent(_KeyEvent):
    all: bool                          # True: the whole chat was cleared (only `chat` is set)


class ReactionEvent(_KeyEvent):
    message_id: Optional[str]
    emoji: Optional[str]
    removed: bool
    reactor: Optional[str]
    reaction: dict


class ReceiptEvent(_KeyEvent):
    user: Optional[str]
    status: str                        # 'delivered' | 'read' | 'played'
    receipt: dict


class PresenceEvent(Event):
    chat: Optional[str]
    jid: str
    presence: Optional[str]            # 'available' | 'unavailable' | 'composing' | 'recording' | 'paused'
    last_seen: Optional[int]


class ParticipantsEvent(Event):
    id: str                            # group jid
    chat: str
    author: Optional[str]              # who performed the action
    author_pn: Optional[str]
    participants: list                 # jids, or dicts {id, phoneNumber, lid, admin} in LID groups -- prefer .jids
    jids: list                         # participant ids as plain strings (use for mentions)
    phones: list                       # phone-number jids when known, else the id
    action: str                        # 'add' | 'remove' | 'promote' | 'demote' | 'modify'


class ChatEvent(Event):
    id: str
    chat: str
    name: Optional[str]
    unread_count: Optional[int]
    archived: Optional[bool]
    pinned: Optional[int]
    mute_end_time: Optional[int]
    conversation_timestamp: Optional[int]
    ephemeral_expiration: Optional[int]


class ChatDeleteEvent(Event):
    id: str


class ContactEvent(Event):
    id: str
    name: Optional[str]
    notify: Optional[str]
    verified_name: Optional[str]
    lid: Optional[str]
    phone_number: Optional[str]
    img_url: Optional[str]
    status: Optional[str]


class GroupEvent(Event):
    id: str
    chat: str
    subject: Optional[str]
    desc: Optional[str]
    owner: Optional[str]
    announce: Optional[bool]
    restrict: Optional[bool]
    participants: Optional[list]
    author: Optional[str]
    size: Optional[int]


class JoinRequestEvent(Event):
    id: str                            # group jid
    chat: str
    action: Optional[str]
    method: Optional[str]
    participant: Optional[str]
    author: Optional[str]


class CallEvent(Event):
    id: str
    chat: str
    chat_id: Optional[str]
    from_: str                         # caller jid (`from` in the raw payload)
    status: str                        # 'offer' | 'accept' | 'reject' | 'timeout' | 'terminate' ...
    date: Any
    is_video: Optional[bool]
    is_group: Optional[bool]
    offline: Optional[bool]


class BlocklistEvent(Event):
    jid: str
    type: Optional[str]                # 'add' | 'remove'


class LabelEvent(Event):
    id: str
    name: Optional[str]
    color: Optional[int]
    deleted: Optional[bool]


class LabelAssociationEvent(Event):
    type: Optional[str]                # 'add' | 'remove'
    association: Optional[dict]


class SettingsEvent(Event):
    value: Any


class LidMappingEvent(Event):
    lid: Optional[str]
    pn: Optional[str]


class NewsletterEvent(Event):
    id: Optional[str]
    server_id: Optional[str]
    code: Optional[str]
    count: Optional[int]
    removed: Optional[bool]
    view_count: Optional[int]


def _ev(raw: Any, _cls: type = Event, **fields) -> Event:
    e = _cls({k: v for k, v in fields.items()})
    e['_raw'] = raw
    return e


def _key_fields(key: Optional[dict]) -> dict:
    key = key or {}
    return {'key': key, 'id': key.get('id'), 'chat': key.get('remoteJid'), 'from_me': bool(key.get('fromMe')),
            'sender': key.get('participant') or (None if key.get('fromMe') else key.get('remoteJid'))}


# ---------------------------------------------------------------------------------------------------------
# converters: raw payload -> iterable of Event
# ---------------------------------------------------------------------------------------------------------
def _items(data) -> Iterable[Any]:
    return data if isinstance(data, (list, tuple)) else [data]


_CHAT_ID: tuple = ()  # filled below, after the classes exist


def item(data, _cls: type = Event) -> Iterable[Event]:
    """Generic list-of-dicts events (chats, contacts, groups, calls, ...)"""
    for d in _items(data):
        if isinstance(d, dict):
            extra = {}
            if _cls is CallEvent:
                extra['chat'] = d.get('chatId') or d.get('from')
            elif _cls in _CHAT_ID and 'id' in d:
                extra['chat'] = d['id']
            yield _ev(d, _cls, **{**d, **extra})
        else:
            yield _ev(d, _cls, value=d)


def message_update(data) -> Iterable[Event]:
    for d in _items(data):
        upd = d.get('update') or {}
        st = upd.get('status')
        yield _ev(d, MessageUpdateEvent, **_key_fields(d.get('key')), update=upd, status=MESSAGE_STATUS.get(st, st), message=upd.get('message'),
                  poll_updates=upd.get('pollUpdates'), edited=bool(upd.get('message')))


def message_delete(data) -> Iterable[Event]:
    if data.get('all'):
        yield _ev(data, MessageDeleteEvent, chat=data.get('jid'), all=True, key=None, id=None)
    for k in data.get('keys') or []:
        yield _ev(k, MessageDeleteEvent, **_key_fields(k), all=False)


def reaction(data) -> Iterable[Event]:
    for d in _items(data):
        r = d.get('reaction') or {}
        rk = r.get('key') or {}
        sender = rk.get('participant') or (None if rk.get('fromMe') else rk.get('remoteJid'))
        yield _ev(d, ReactionEvent, **_key_fields(d.get('key')), message_id=(d.get('key') or {}).get('id'), emoji=r.get('text') or None,
                  removed=not r.get('text'), reactor=sender, reaction=r)


def receipt(data) -> Iterable[Event]:
    for d in _items(data):
        r = d.get('receipt') or {}
        status = 'played' if r.get('playedTimestamp') else 'read' if r.get('readTimestamp') else 'delivered'
        yield _ev(d, ReceiptEvent, **_key_fields(d.get('key')), user=r.get('userJid'), status=status, receipt=r)


def presence(data) -> Iterable[Event]:
    for jid, p in (data.get('presences') or {}).items():
        yield _ev(data, PresenceEvent, chat=data.get('id'), jid=jid, presence=p.get('lastKnownPresence'), last_seen=p.get('lastSeen'))


def participants(data) -> Iterable[Event]:
    jids = [x.get('id') if isinstance(x, dict) else x for x in data.get('participants') or []]
    phones = [(x.get('phoneNumber') or x.get('id')) if isinstance(x, dict) else x for x in data.get('participants') or []]
    yield _ev(data, ParticipantsEvent, **data, chat=data.get('id'), jids=jids, phones=phones)


def blocklist(data) -> Iterable[Event]:
    for jid in data.get('blocklist') or []:
        yield _ev(data, BlocklistEvent, jid=jid, type=data.get('type'))


def history(data) -> Iterable[Event]:
    yield _ev(data, HistoryEvent, **data)


# ---------------------------------------------------------------------------------------------------------
# decorator name -> (raw event, converter, optional filter on the raw payload)
# ---------------------------------------------------------------------------------------------------------
Spec = tuple  # (event, converter, predicate | None)

_CHAT_ID = (ChatEvent, ContactEvent, GroupEvent, JoinRequestEvent)


def _of(cls: type):
    return lambda d: item(d, cls)


def _single(cls: type):
    return lambda d: [_ev(d, cls, **d)]


TYPED: dict[str, Spec] = {
    'on_connection':        ('connection.update', _single(ConnectionEvent), None),
    'on_history':           ('messaging-history.set', history, None),
    'on_message_update':    ('messages.update', message_update, None),
    'on_message_delete':    ('messages.delete', message_delete, None),
    'on_reaction':          ('messages.reaction', reaction, None),
    'on_receipt':           ('message-receipt.update', receipt, None),
    'on_presence':          ('presence.update', presence, None),
    'on_chat_upsert':       ('chats.upsert', _of(ChatEvent), None),
    'on_chat_update':       ('chats.update', _of(ChatEvent), None),
    'on_chat_delete':       ('chats.delete', lambda ids: [_ev(i, ChatDeleteEvent, id=i) for i in ids], None),
    'on_contact_upsert':    ('contacts.upsert', _of(ContactEvent), None),
    'on_contact_update':    ('contacts.update', _of(ContactEvent), None),
    'on_group_upsert':      ('groups.upsert', _of(GroupEvent), None),
    'on_group_update':      ('groups.update', _of(GroupEvent), None),
    'on_participants':      ('group-participants.update', participants, None),
    'on_group_join':        ('group-participants.update', participants, lambda d: d.get('action') == 'add'),
    'on_group_leave':       ('group-participants.update', participants, lambda d: d.get('action') == 'remove'),
    'on_group_promote':     ('group-participants.update', participants, lambda d: d.get('action') == 'promote'),
    'on_group_demote':      ('group-participants.update', participants, lambda d: d.get('action') == 'demote'),
    'on_join_request':      ('group.join-request', _of(JoinRequestEvent), None),
    'on_call':              ('call', _of(CallEvent), None),
    'on_blocklist':         ('blocklist.update', blocklist, None),
    'on_label':             ('labels.edit', _of(LabelEvent), None),
    'on_label_association': ('labels.association', _of(LabelAssociationEvent), None),
    'on_settings_update':   ('settings.update', _of(SettingsEvent), None),
    'on_lid_mapping':       ('lid-mapping.update', _single(LidMappingEvent), None),
    'on_newsletter_reaction': ('newsletter.reaction', _of(NewsletterEvent), None),
    'on_newsletter_view':   ('newsletter.view', _of(NewsletterEvent), None),
}
