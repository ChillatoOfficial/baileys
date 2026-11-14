# baileys — reference

Complete reference for the Python port of Baileys: every type, every class, every option.
For a guided tour see [README.md](README.md) and [docs/](docs/index.md). All types below exist in code in
[baileys/schemas.py](baileys/schemas.py) (`from baileys.schemas import WAMessage, GroupMetadata, ...`).

**Contents**
1. [Conventions](#1-conventions)
2. [Quick start](#2-quick-start)
3. [Basic types](#3-basic-types): JID, keys, messages, enums, errors
4. [Message content you can send](#4-message-content-you-can-send)
5. [`Client`](#5-client)
6. [`Message`](#6-message)
7. [Events](#7-events)
8. [Filters](#8-filters)
9. [Commands, permissions, middleware](#9-commands-permissions-middleware)
10. [`Group`, `Contact`, LID helpers](#10-group-contact-lid-helpers)
11. [Send queue](#11-send-queue)
12. [Storage and authentication types](#12-storage-and-authentication-types)
13. [Plugins and `ClientPool`](#13-plugins-and-clientpool)
14. [Socket configuration](#14-socket-configuration)
15. [Low-level socket API](#15-low-level-socket-api)
16. [Utilities](#16-utilities)

---

## 1. Conventions

| JavaScript (Baileys) | Python (baileys) |
|---|---|
| `makeWASocket(config)` | `make_wa_socket(config)` (alias `makeWASocket`) — call inside a running event loop |
| `sock.sendMessage(...)` | `sock.send_message(...)` (the camelCase name also works) |
| `Buffer` | `bytes` |
| object `{ a: 1 }` | `dict` with the **same camelCase keys** (`{'remoteJid': ...}`) |
| `Promise` | `async def` / `await` |
| `sock.ev.on('x', fn)` | same; listeners may be sync or `async` |
| `sock.ev.process(fn)` | same; returns an unsubscribe function |

Protobuf is only used at the edges (wire format); everything you see is plain `dict` / `bytes` / `int` / `str`.
Handler arguments are positional. Attributes that do not exist on `Event` return `None` instead of raising.

## 2. Quick start

```python
from baileys import Client, filters

client = Client('auth')                     # folder | 'bot.db' | (state, save) | async factory

@client.command('ping')
async def ping(m):
    await m.reply('pong')

@client.message(filters.text & ~filters.from_me)
async def log(m):
    print(m.sender, m.text)

client.run()                                # QR in the terminal; Ctrl+C to stop
```

Without the high level layer:

```python
from baileys import make_wa_socket, use_multi_file_auth_state

state, save_creds = await use_multi_file_auth_state('auth')
sock = make_wa_socket({'auth': state})
sock.ev.on('creds.update', lambda _: asyncio.ensure_future(save_creds()))
```

## 3. Basic types

### 3.1 JID

`str`. Forms: `'<number>@s.whatsapp.net'` (phone number), `'<id>@lid'` (privacy ID), `'<id>@g.us'` (group),
`'<id>@newsletter'` (channel), `'status@broadcast'`, `'<id>@broadcast'` (list). A device suffix looks like
`'3933312345:7@s.whatsapp.net'`. Helpers (all in `baileys`): `jid_decode`, `jid_encode`, `jid_normalized_user`,
`are_jids_same_user`, `is_pn_user`, `is_lid_user`, `is_jid_group`, `is_jid_broadcast`, `is_jid_status_broadcast`,
`is_jid_newsletter`, `S_WHATSAPP_NET`.

### 3.2 `MessageKey`

```python
class MessageKey(TypedDict, total=False):
    remoteJid: JID        # the chat
    id: str
    fromMe: bool
    participant: JID      # sender inside a group
    participantAlt: JID   # same sender in the other addressing mode (LID <-> phone number)
    remoteJidAlt: JID
```

### 3.3 `WAMessage`

```python
class WAMessage(TypedDict, total=False):
    key: MessageKey
    message: dict         # {'conversation': 'hi'} | {'imageMessage': {...}} | {'extendedTextMessage': {...}} ...
    messageTimestamp: int
    pushName: str
    status: int           # 0 error, 1 pending, 2 sent, 3 delivered, 4 read, 5 played
    broadcast: bool
    participant: JID
    messageStubType: int
    messageStubParameters: list[str]
```

`m.raw` of a `Message` is a `WAMessage`. Common `message` keys: `conversation`, `extendedTextMessage` (`text`,
`contextInfo`), `imageMessage` / `videoMessage` / `audioMessage` / `documentMessage` / `stickerMessage` (`caption`,
`mimetype`, `url`, `directPath`, `mediaKey`, ...), `reactionMessage`, `protocolMessage`, `pollCreationMessageV3`,
`locationMessage`, `contactMessage`, `buttonsResponseMessage`, `listResponseMessage`.

### 3.4 `ContextInfo`

`stanzaId`, `participant`, `quotedMessage`, `mentionedJid`, `isForwarded`, `forwardingScore`, `expiration`.

### 3.5 Enums (`from baileys import ...`)

| enum | values |
|---|---|
| `DisconnectReason` | `connectionClosed=428`, `connectionLost=408`, `connectionReplaced=440`, `timedOut=408`, `loggedOut=401`, `badSession=500`, `restartRequired=515`, `multideviceMismatch=411`, `forbidden=403`, `unavailableService=503` |
| `WAMessageAddressingMode` | `PN='pn'`, `LID='lid'` |
| `SyncState` | `Connecting=0`, `AwaitingInitialSync=1`, `Syncing=2`, `Online=3` |
| `LabelColor` | `Color1=0` … `Color20=19` |
| `LabelAssociationType` | `Chat='label_jid'`, `Message='label_message'` |
| `NewChatMessageCappingStatusType` | `NONE`, `FIRST_WARNING`, `SECOND_WARNING`, `CAPPED` |
| `NewChatMessageCappingMVStatusType` | `NOT_ELIGIBLE`, `NOT_ACTIVE`, `ACTIVE`, `ACTIVE_UPGRADE_AVAILABLE` |
| `NewChatMessageCappingOTEStatusType` | `NOT_ELIGIBLE`, `ELIGIBLE`, `ACTIVE_IN_CURRENT_CYCLE`, `EXHAUSTED` |
| `ReachoutTimelockEnforcementType` | `BIZ_QUALITY`, `DEFAULT`, `WEB_COMPANION_ONLY`, `BIZ_COMMERCE_VIOLATION_*` |
| `WAMessageStatus` | message delivery states (`ERROR`, `PENDING`, `SERVER_ACK`, `DELIVERY_ACK`, `READ`, `PLAYED`) |

### 3.6 Errors

`Boom` (from `baileys`): `.message`, `.status_code`, `.data`, `.output['statusCode']`.

```python
@client.on_close
async def closed(reason):                    # a DisconnectReason, or the raw status code
    if reason == DisconnectReason.loggedOut: ...
```

Retried automatically by the send queue: `ConnectionError`, `TimeoutError`, and status codes
`408, 428, 440, 503, 515`.

## 4. Message content you can send

`await client.send(jid, content, **options)`, `await m.reply(content, **options)`, `sock.send_message(jid, content,
options)`. A plain `str` becomes `{'text': ...}`. Every content may also carry `mentions`, `mentionAll`, `contextInfo`,
`viewOnce`, `edit`.

| type | shape | notes |
|---|---|---|
| `TextContent` | `{'text': str, 'linkPreview'?: dict \| None, 'backgroundColor'?, 'font'?}` | link preview generated automatically |
| `ImageContent` | `{'image': Source, 'caption'?, 'jpegThumbnail'?, 'viewOnce'?}` | |
| `VideoContent` | `{'video': Source, 'caption'?, 'gifPlayback'?, 'ptv'?}` | `ptv` = round video note; needs ffmpeg for thumbnails |
| `AudioContent` | `{'audio': Source, 'ptt'?: bool, 'mimetype'?, 'seconds'?}` | voice note: `ptt=True`, `audio/ogg; codecs=opus` |
| `DocumentContent` | `{'document': Source, 'fileName', 'mimetype', 'caption'?}` | |
| `StickerContent` | `{'sticker': Source}` | `image/webp` |
| `LocationContent` | `{'location': {'degreesLatitude', 'degreesLongitude', 'name'?, 'address'?}}` | |
| `ContactsContent` | `{'contacts': {'displayName', 'contacts': [{'vcard': str}]}}` | one contact → contact card, several → array |
| `ReactContent` | `{'react': {'text': '👍', 'key': MessageKey}}` | `''` removes the reaction |
| `DeleteContent` | `{'delete': MessageKey}` | revoke for everyone |
| `ForwardContent` | `{'forward': WAMessage, 'force'?: bool}` | |
| `PollContent` | `{'poll': {'name', 'values': [str], 'selectableCount'?, 'toAnnouncementGroup'?}}` | `selectableCount` 0..len(values) |
| `EventContent` | `{'event': {'name', 'startDate', 'endDate'?, 'description'?, 'location'?, 'call'?}}` | dates are `datetime` |
| `PinContent` | `{'pin': MessageKey, 'type': 1 \| 2, 'time'?: seconds}` | 1 pin, 2 unpin |
| `GroupInviteContent` | `{'groupInvite': {'inviteCode', 'inviteExpiration', 'text', 'jid', 'subject'}}` | |
| `ButtonReplyContent` | `{'buttonReply': {'displayText', 'id', 'index'}, 'type': 'plain' \| 'template'}` | |
| `ListReplyContent` | `{'listReply': {...}}` | |
| `ProductContent` | `{'product': {...}, 'businessOwnerJid', 'body'?, 'footer'?}` | |
| `DisappearingContent` | `{'disappearingMessagesInChat': True \| seconds \| False}` | in groups toggles the setting (returns `None`) |
| album / others | `{'album': {...}, 'albumParentKey'}`, `{'sharePhoneNumber': True}`, `{'requestPhoneNumber': True}`, `{'limitSharing': bool}` | |

`Source` = `bytes` · `{'url': '<path or http(s) URL or data: URI>'}` · `{'stream': async iterable of bytes}`.
In the high level helpers you can pass a `str` / `Path` directly.

### `SendOptions` (keyword arguments)

| key | meaning |
|---|---|
| `quoted` | `WAMessage` to reply to |
| `messageId` | custom message id |
| `timestamp` | `datetime` |
| `ephemeralExpiration` | seconds (disappearing chats) |
| `statusJidList` | recipients when sending to `status@broadcast` |
| `useCachedGroupMetadata` / `useUserDevicesCache` | skip lookups when you maintain caches |
| `participant` | send to one participant only |
| `additionalAttributes` / `additionalNodes` | extra stanza attributes / child nodes |
| `backgroundColor`, `font` | text status styling |
| `mediaUploadTimeoutMs` | upload timeout |

## 5. `Client`

```python
Client(auth='baileys_auth_info', *, prefix='/', print_qr=True, pairing_phone=None, auto_reconnect=True,
       fetch_version=False, ignore_from_me=True, only_notify=True, log_level='warn', owners=(),
       help_command=True, denied_messages=None, rate_limit=None, auto_read=False, group_cache_ttl=300,
       name=None, **socket_config)
```

| parameter | meaning |
|---|---|
| `auth` | folder (multi-file) · path ending `.db` / `.sqlite` / `.sqlite3` (SQLite) · `(state, save_creds)` · `async def` returning that tuple |
| `prefix` | `str` or tuple of command prefixes |
| `print_qr` | render the QR in the terminal (needs `qrcode`, else prints the string); `@client.on_qr` replaces it |
| `pairing_phone` | digits only; log in with a pairing code instead of the QR |
| `auto_reconnect` | reconnect on every close except `loggedOut` |
| `fetch_version` | fetch the latest WhatsApp Web version before connecting |
| `ignore_from_me` | default for handlers: skip messages sent by this account |
| `only_notify` | only `messages.upsert` of type `notify` reach message handlers / commands |
| `owners` | phone numbers (digits) allowed to run `owner_only` commands |
| `help_command` | register the built-in `/help` |
| `denied_messages` | `dict` overriding refusal texts, `False` = silent (keys in [9.3](#93-refusals)) |
| `rate_limit` | `None` defaults · `False` off · `dict` / `SendQueue` (see [11](#11-send-queue)) |
| `auto_read` | mark incoming messages as read |
| `group_cache_ttl` | seconds group metadata stays cached |
| `log_level` | `'trace' 'debug' 'info' 'warn' 'error' 'silent'` |
| `name` | label (shown in `ClientPool`) |
| `**socket_config` | any [socket option](#14-socket-configuration) |

### Attributes

| attribute | type |
|---|---|
| `client.sock` | the low-level socket (after `start()`) |
| `client.me` | `{'id', 'lid', 'name'}` of your account |
| `client.contacts` | `dict[JID, ContactInfo]` kept up to date from sync events |
| `client.chats` | `dict[JID, ChatInfo]` |
| `client.queue` | the `SendQueue` (or `None`) |
| `client.owners`, `client.prefixes`, `client.logger`, `client.name`, `client.plugins` | |

### Lifecycle

| method | |
|---|---|
| `client.run()` | blocking; handles Ctrl+C |
| `await client.run_forever()` | connect and keep reconnecting |
| `await client.start()` / `await client.stop()` | connect (non-blocking) / close |
| `async with Client(...) as c:` | start / stop |
| `await client.wait_ready(timeout=None)` | wait for the connection to open |
| `await client.logout()` | unlink the device |

### Decorators

| decorator | handler signature |
|---|---|
| `@client.command(name, *aliases, **options)` | `(m)` or `(m, args)` |
| `@client.message(*filters, from_me=None)` | `(m)` |
| `@client.use` | `(m, call_next)` |
| `@client.on(raw_event)` | `(payload)` |
| `@client.on_ready` | `()` |
| `@client.on_qr` | `(qr: str)` |
| `@client.on_pairing_code` | `(code: str)`, e.g. `'ABCD-1234'`; only with `pairing_phone=`; replaces the default terminal message |
| `@client.on_close` | `(reason)` |
| `@client.on_error` | `(exception, handler)` |
| `@client.on_denied` | `(m, reason, command)` |
| `@client.on_<typed event>` | `(e: Event)` — see [7.2](#72-typed-events) |

### Sending and waiting

| method | returns |
|---|---|
| `await client.send(jid, content, **options)` | sent `Message` (through the queue) |
| `await client.send_now(jid, content, **options)` | like `send`, no spacing |
| `await client.broadcast(jids, content, **options)` | `list[Message]` |
| `await client.relay(jid, message, *, message_id=None, participant=None, additional_attributes=None, additional_nodes=None, use_cached_group_metadata=None, status_jid_list=None)` | relayMessage: ships an already-built proto `Message` dict as is; returns the message id |
| `await client.wait_for(chat=None, sender=None, filter=None, timeout=None, consume=True)` | next matching `Message`; raises `asyncio.TimeoutError` |

### Groups, contacts, identities

| method | returns |
|---|---|
| `await client.group(jid, refresh=False)` | `Group` (cached) |
| `await client.groups()` | `list[Group]` |
| `client.contact(jid)` / `client.name_of(jid)` | `Contact` / name or `None` |
| `await client.phone_of(jid)` / `await client.lid_of(jid)` | normalized JID or `None` |
| `await client.same_user(a, b)` | `bool` |
| `await client.is_owner(m)` | `bool` |

### Plugins

`client.load(path_or_module)`, `client.load_plugin(plugin)`, `client.unload(name)`, `client.reload(name)`,
`client.plugins`.

Anything else is forwarded to the socket: `await client.group_metadata(jid)`, `await client.reject_call(id, from_)`,
`await client.update_profile_status('hi')` ...

## 6. `Message`

`Message(client, raw: WAMessage)`. Never raises for missing optional data.

### Properties

| property | type | |
|---|---|---|
| `raw`, `key`, `content` | `WAMessage`, `MessageKey`, `dict` | `content` has ephemeral / view-once / edit wrappers removed |
| `id`, `chat`, `sender` | `str` | `sender` may be a LID |
| `from_me`, `is_group` | `bool` | |
| `push_name`, `timestamp` | `str \| None`, `int \| None` | |
| `text` | `str \| None` | conversation, extended text, caption, button / list reply id |
| `media_type` | `'image' \| 'video' \| 'audio' \| 'document' \| 'sticker' \| None` | |
| `quoted` | `Message \| None` | the replied-to message |
| `mentions` | `list[JID]` | |
| `command`, `args`, `arg_text` | `str \| None`, `list[str]`, `str` | set for command handlers |
| `state` | `dict` | scratch space for middleware |
| `client` | `Client` | |

### Methods (all `async`)

| method | |
|---|---|
| `reply(content, **options)` / `send(content, **options)` | quoted / not quoted → `Message` |
| `reply_image(src, caption=None)`, `reply_video(src, caption=None, gif=False)`, `reply_audio(src, voice_note=False)` | |
| `reply_document(src, filename, mimetype, caption=None)`, `reply_sticker(src, convert=True, pack=None, author=None)` | images/GIFs/videos are converted to 512x512 WebP (`baileys.stickers.to_sticker`); `convert=False` sends as is |
| `await m.relay_to(jid)` | relays this message's content to another chat unchanged |
| `reply_location(lat, lon, name=None, address=None)`, `reply_contact(name, number)` | |
| `reply_poll(question, choices, multiple=False)`, `reply_mentioning(text, jids)`, `mention_all(text='')` | |
| `react(emoji)` | `''` removes |
| `edit(text)` / `delete()` | on messages you sent (delete: also others' if you are group admin) |
| `forward(to)` | |
| `read()`, `typing(seconds=0)` | |
| `download(type='buffer')`, `save(path)` | decrypt media → `bytes` / file |
| `group(refresh=False)`, `sender_is_admin()` | |
| `sender_phone()`, `sender_number()` | phone JID / digits even when addressed by LID |
| `ask(prompt=None, timeout=60, filter=None, same_sender=True, quote=True)` | → `Message`; raises `asyncio.TimeoutError` |
| `confirm(prompt, timeout=30)` | `True` / `False` / `None` |
| `choose(prompt, options, timeout=30)` | chosen option or `None` |

## 7. Events

### 7.1 Raw events (`@client.on('name')` / `sock.ev.on('name', fn)`)

| event | payload (type in `baileys.schemas`) |
|---|---|
| `connection.update` | `ConnectionUpdate` |
| `creds.update` | partial `AuthenticationCreds` (save them!) |
| `messaging-history.set` | `MessagingHistorySet` |
| `messaging-history.status` | `{'syncType', 'status', 'explicit'}` |
| `messages.upsert` | `MessagesUpsert` |
| `messages.update` | `list[MessageUpdate]` |
| `messages.delete` | `MessagesDelete` |
| `messages.reaction` | `list[Reaction]` |
| `messages.media-update` | `list[{'key', 'media'?, 'error'?}]` |
| `message-receipt.update` | `list[MessageReceipt]` |
| `presence.update` | `PresenceUpdate` |
| `chats.upsert` / `chats.update` | `list[ChatInfo]` / `list[partial ChatInfo]` |
| `chats.delete` | `list[JID]` |
| `chats.lock` | `{'id', 'locked'}` |
| `contacts.upsert` / `contacts.update` | `list[ContactInfo]` / `list[partial ContactInfo]` |
| `groups.upsert` / `groups.update` | `list[GroupMetadata]` / `list[partial GroupMetadata]` |
| `group-participants.update` | `GroupParticipantsUpdate` |
| `group.join-request` | `{'id', 'author', 'participant', 'action', 'method'}` |
| `group.member-tag.update` | `{'groupId', 'participant', 'label', 'messageTimestamp'}` |
| `blocklist.update` | `BlocklistUpdate` (`fetch_blocklist()` returns the full list) |
| `call` | `list[CallEvent]` |
| `labels.edit` / `labels.association` | `Label` / `{'association', 'type'}` |
| `settings.update` | `{'setting': 'unarchiveChats' \| 'locale' \| 'disableLinkPreviews' \| ..., 'value'}` |
| `lid-mapping.update` | `LidMappingUpdate` |
| `newsletter.reaction` / `newsletter.view` | `{'id', 'server_id', 'reaction'}` / `{'id', 'server_id', 'count'}` |
| `newsletter-participants.update` / `newsletter-settings.update` | `{'id', 'author', 'user', 'new_role', 'action'}` / `{'id', 'update'}` |
| `message-capping.update` | capping info |

### 7.2 Typed events

`@client.on_xxx` calls your handler **once per item** with an `Event`: a `dict` with attribute access
(snake_case or camelCase), `e.raw` (original payload), `e.client`, and `await e.send(content)` (writes to `e.chat`).

| decorator | raw event | fields |
|---|---|---|
| `on_connection` | `connection.update` | `connection`, `qr`, `last_disconnect`, `is_new_login`, `received_pending_notifications` |
| `on_history` | `messaging-history.set` | `chats`, `contacts`, `messages`, `is_latest`, `progress`, `sync_type` |
| `on_message_update` | `messages.update` | `key`, `id`, `chat`, `from_me`, `sender`, `status` (`'error' 'pending' 'sent' 'delivered' 'read' 'played'`), `edited`, `message`, `poll_updates`, `update` |
| `on_message_delete` | `messages.delete` | `key`, `id`, `chat`, `from_me`, `all` |
| `on_reaction` | `messages.reaction` | `key`, `id`, `chat`, `message_id`, `emoji` (`None` if removed), `removed`, `reactor`, `reaction` |
| `on_receipt` | `message-receipt.update` | `key`, `id`, `chat`, `user`, `status` (`'delivered' 'read' 'played'`), `receipt` |
| `on_presence` | `presence.update` | `chat`, `jid`, `presence`, `last_seen` |
| `on_chat_upsert`, `on_chat_update` | `chats.upsert/update` | `ChatInfo` fields |
| `on_chat_delete` | `chats.delete` | `id` |
| `on_contact_upsert`, `on_contact_update` | `contacts.*` | `ContactInfo` fields |
| `on_group_upsert`, `on_group_update` | `groups.*` | `GroupMetadata` fields |
| `on_participants` | `group-participants.update` | `chat`, `id`, `action`, `participants`, `author` |
| `on_group_join` / `on_group_leave` / `on_group_promote` / `on_group_demote` | same | same, filtered by `action` |
| `on_join_request` | `group.join-request` | request fields |
| `on_call` | `call` | `id`, `chat_id`, `from_`, `status`, `is_video`, `is_group`, `date` |
| `on_blocklist` | `blocklist.update` | `jid`, `type` |
| `on_label`, `on_label_association` | `labels.*` | label fields |
| `on_settings_update` | `settings.update` | `setting`, `value` |
| `on_lid_mapping` | `lid-mapping.update` | `lid`, `pn` |
| `on_newsletter_reaction`, `on_newsletter_view` | `newsletter.*` | fields |

Event ordering: the library buffers events during the initial sync (`BufferedEventEmitter`) and flushes them
consolidated; `messages.upsert` of the history arrives with `type == 'append'`.

## 8. Filters

`from baileys import filters`. Combine with `&`, `|`, `~`. A `Filter` is callable: `filters.text(m)`.

| filter | matches |
|---|---|
| `text` | has text |
| `from_me` | sent by this account |
| `group` / `private` | where it was sent |
| `media` | has attachment |
| `media_type('image', 'video')` | attachment of those kinds |
| `quoted` | replies to a message |
| `regex(pattern, flags=re.I)` | text matches |
| `equals(*values, ignore_case=True)` | whole text equals one of them |
| `chat(*jids)` / `sender(*jids)` | specific chat / sender |
| `everything` | always |

Custom: `filters.Filter(lambda m: m.timestamp and m.timestamp > t0, 'recent')`.

## 9. Commands, permissions, middleware

### 9.1 `@client.command(name, *aliases, ...)`

| option | type | default | |
|---|---|---|---|
| `help` | `str` | `None` | text for `/help` |
| `admin_only` | `bool` | `False` | sender is group admin (or owner / own account); implies `group_only` |
| `bot_admin` | `bool` | `False` | bot is group admin; implies `group_only` |
| `group_only` / `private_only` | `bool` | `False` | |
| `owner_only` | `bool` | `False` | own account or `owners` |
| `cooldown` | `float` | `0` | seconds per user and command |
| `hidden` | `bool` | `False` | not listed in `/help` |
| `filter` | `Filter` | `None` | extra condition (ignored silently if false) |
| `from_me` | `bool` | `None` | also handle messages from this account |

The dataclass is `baileys.Command` (`name`, `fn`, `aliases`, `help`, `skip_me`, `admin_only`, `bot_admin`,
`group_only`, `private_only`, `owner_only`, `cooldown`, `hidden`, `filter`, `n_params`).

### 9.2 Middleware

```python
@client.use
async def mw(m: Message, call_next):
    ...                      # before
    await call_next()        # continue (omit to stop the message here)
    ...                      # after
```

### 9.3 Refusals

Reasons: `admin_only`, `bot_admin`, `group_only`, `private_only`, `owner_only`, `cooldown`. Default texts live in
`baileys.commands.DEFAULT_DENIED`; `{seconds}` is available in the `cooldown` text.

## 10. `Group`, `Contact`, LID helpers

### `Group` (`await client.group(jid)` / `await m.group()`)

| member | |
|---|---|
| `id`, `subject` (`name`), `description`, `owner`, `size` | |
| `participants` → `list[Participant]`, `admins` | |
| `only_admins_can_send`, `only_admins_can_edit` | `announce`, `restrict` |
| `find(jid)`, `has(jid)`, `is_admin(jid)`, `is_superadmin(jid)` | match by LID **or** phone JID |
| `me`, `bot_is_admin` | |
| `await add/remove/promote/demote(*jids)` | → `list[ParticipantUpdateResult]` |
| `await rename(subject)`, `set_description(text)`, `lock(only_admins_send=True)` | |
| `await invite_link()`, `revoke_invite()`, `leave()` | |
| `await send(content)`, `mention_all(text='')`, `refresh()` | |
| `meta` | the raw `GroupMetadata` |

### `Contact` (`client.contact(jid)`)

`id`, `lid`, `name`, `push_name`, `number`, `await picture_url(high_res=False)`, `await send(content)`, `data`.

### LID helpers

`phone_of`, `lid_of`, `same_user` on the client; `m.sender_phone()` / `m.sender_number()` on messages.

## 11. Send queue

```python
SendQueue(per_chat=0.6, global_=0.12, jitter=0.25, retries=2, backoff=1.5, logger=None)
await queue.submit(chat, coroutine_function, priority_bypass=False)
```

`per_chat` / `global_`: minimum seconds between sends to one chat / overall; `jitter`: random extra seconds;
`retries`: extra attempts for transient errors; `backoff`: base of the exponential wait. Messages of one chat keep
their order. Pass `Client(rate_limit={...})`, `True` (defaults), `False` (off) or a `SendQueue`.

## 12. Storage and authentication types

```python
class AuthenticationState(TypedDict):
    creds: AuthenticationCreds
    keys: SignalKeyStore          # async get(type, ids) -> {id: value}; async set({type: {id: value | None}})
```

`AuthenticationCreds` keys: `noiseKey`, `pairingEphemeralKeyPair`, `signedIdentityKey`, `signedPreKey`,
`registrationId`, `advSecretKey`, `processedHistoryMessages`, `nextPreKeyId`, `firstUnuploadedPreKeyId`,
`accountSyncCounter`, `accountSettings`, `registered`, `pairingCode`, `lastPropHash`, `routingInfo`,
`additionalData`, and after pairing `me`, `account`, `signalIdentities`, `myAppStateKeyId`, `platform`.
Key store categories (`SignalKeyType`): `pre-key`, `session`, `sender-key`, `sender-key-memory`,
`app-state-sync-key`, `app-state-sync-version`, `lid-mapping`, `device-list`, `tctoken`, `identity-key`.

| helper | |
|---|---|
| `await use_multi_file_auth_state(folder)` | JSON files |
| `await use_sqlite_auth_state(path)` | one SQLite file |
| `await use_redis_auth_state(redis, prefix='baileys')` | `redis.asyncio.Redis` |
| `await use_custom_auth_state(backend)` | your backend |
| `make_cacheable_signal_key_store(store, logger)` | add an in-memory TTL cache (done for you by `Client`) |
| `add_transaction_capability(store, logger, opts)` | transactional key store |
| `init_auth_creds()` | brand new credentials |

All return `(state, save_creds)`. `AuthBackend` protocol:

```python
class AuthBackend(Protocol):
    async def get_many(self, keys: list[str]) -> dict[str, Optional[str]]: ...
    async def set_many(self, items: dict[str, Optional[str]]) -> None: ...   # None = delete
```

## 13. Plugins and `ClientPool`

`Plugin(name)`: same decorators as `Client` (`command`, `message`, `use`, `on`, `on_*`), recorded and applied by
`client.load_plugin(plugin)`. `client.load(target)` accepts a folder, a `.py` file or a dotted module and expects
either `plugin = Plugin('x')` or `def setup(client)`; `reload(name)` re-reads the file (no `.pyc` cache).

`ClientPool(accounts: dict[name, auth], **client_kwargs)`: `pool[name]`, iteration, the same decorators (applied to
every client), `pool.load(...)`, `pool.run()`, `await pool.run_forever()`; `m.client.name` tells accounts apart.

## 14. Socket configuration

`Client(..., **config)` and `make_wa_socket(config)`. Defaults come from `DEFAULT_CONNECTION_CONFIG`.

| key | default | |
|---|---|---|
| `version` | `[2, 3000, …]` | WhatsApp Web version (`fetch_latest_wa_web_version()`) |
| `browser` | `['Mac OS', 'Chrome', '14.4.1']` | name in linked devices; helpers in `Browsers` |
| `waWebSocketUrl` | `wss://web.whatsapp.com/ws/chat` | |
| `connectTimeoutMs` / `keepAliveIntervalMs` / `defaultQueryTimeoutMs` | 20000 / 30000 / 60000 | |
| `auth` | — | `AuthenticationState` (required) |
| `emitOwnEvents` | `True` | emit your own sent messages as `messages.upsert` |
| `markOnlineOnConnect` | `True` | |
| `syncFullHistory` | `True` | request full history on first login |
| `fireInitQueries` | `True` | |
| `generateHighQualityLinkPreview` | `False` | upload link preview thumbnails |
| `linkPreviewImageThumbnailWidth` | `192` | |
| `countryCode` | `'US'` | |
| `maxMsgRetryCount` / `retryRequestDelayMs` | 5 / 250 | |
| `enableAutoSessionRecreation` / `enableRecentMessageCache` | `True` | |
| `transactionOpts` | `{'maxCommitRetries': 10, 'delayBetweenTriesMs': 3000}` | |
| `appStateMacVerification` | `{'patch': False, 'snapshot': False}` | |
| `customUploadHosts`, `options`, `agent` | | media hosts / httpx options / proxy |
| `getMessage` | `None` | `async (key) -> message dict` for retries and poll decryption |
| `cachedGroupMetadata` | `None` | `async (jid) -> GroupMetadata` to avoid fetching group info on every send |
| `patchMessageBeforeSending` | identity | `(message, recipients) -> message` |
| `shouldSyncHistoryMessage` / `shouldIgnoreJid` | built-ins | filters |
| `msgRetryCounterCache`, `userDevicesCache`, `placeholderResendCache`, `callOfferCache`, `mediaCache` | in-memory | plug your own cache |
| `makeSignalRepository` | built-in | custom Signal implementation |
| `logger` | `ILogger('baileys')` | |

## 15. Low-level socket API

`sock = client.sock` or `make_wa_socket(...)`. Every function is `async` unless marked. Plus `sock.ev`
(event emitter), `sock.auth_state`, `sock.signal_repository`, `sock.user`.

**Connection / core** — `end(error)`, `logout(msg=None)`, `request_pairing_code(phone, custom=None)`,
`wait_for_connection_update(check, timeout_ms=None)`, `wait_for_socket_open()`, `query(node, timeout_ms=None)`,
`send_node(node)`, `send_raw_message(data)`, `generate_message_tag()` *(sync)*, `register_socket_end_handler(fn)`
*(sync)*, `upload_pre_keys(count)`, `rotate_signed_pre_key()`, `digest_key_bundle()`, `on_whatsapp(*numbers)`
→ `[{'jid', 'exists'}]` (only numbers that exist).

**Messages** — `send_message(jid, content, options=None)`, `relay_message(jid, message, options=None)` → message id,
`read_messages(keys)`, `send_receipt(jid, participant, message_ids, type)`, `send_receipts(keys, type)`,
`update_media_message(message)`, `fetch_message_history(count, oldest_msg_key, oldest_msg_timestamp)` → request id,
`request_placeholder_resend(message_key, msg_data=None)`, `wait_for_message(msg_id, timeout_ms=60000)`.

**Chats / presence / profile** — `chat_modify(mod, jid)` (`ChatModification`), `star(jid, messages, star_)`,
`send_presence_update(type, to_jid=None)`, `presence_subscribe(jid)`, `profile_picture_url(jid, type='preview')`,
`update_profile_picture(jid, content, dimensions=None)`, `remove_profile_picture(jid)`,
`update_profile_name(name)`, `update_profile_status(status)`, `fetch_status(*jids)`, `fetch_blocklist()`,
`update_block_status(jid, 'block' \| 'unblock')`, `add_or_edit_contact`, `remove_contact`,
`add_label`, `add_chat_label`, `remove_chat_label`, `add_message_label`, `remove_message_label`,
`add_or_edit_quick_reply`, `remove_quick_reply`, `resync_app_state(collections, is_initial_sync)`, `app_patch(patch)`.

**Privacy** — `fetch_privacy_settings(force=False)`, `update_last_seen_privacy`, `update_online_privacy`,
`update_profile_picture_privacy`, `update_status_privacy`, `update_read_receipts_privacy`,
`update_groups_add_privacy`, `update_messages_privacy`, `update_call_privacy`,
`update_disable_link_previews_privacy`, `update_default_disappearing_mode(duration)`,
`fetch_disappearing_duration(*jids)`.

**Groups** — `group_create(subject, participants)`, `group_metadata(jid)`, `group_fetch_all_participating()`,
`group_participants_update(jid, jids, 'add' | 'remove' | 'promote' | 'demote')`, `group_update_subject`,
`group_update_description`, `group_leave`, `group_invite_code`, `group_revoke_invite`, `group_accept_invite(code)`,
`group_get_invite_info(code)`, `group_toggle_ephemeral(jid, seconds)`, `group_setting_update(jid,
'announcement' | 'not_announcement' | 'locked' | 'unlocked')`, `group_member_add_mode(jid, 'admin_add' | 'all_member_add')`,
`group_join_approval_mode(jid, 'on' | 'off')`, `group_request_participants_list`, `group_request_participants_update`,
`update_member_label`.

**Communities** — `community_create`, `community_create_group`, `community_metadata`, `community_leave`,
`community_update_subject`, `community_update_description`, `community_link_group`, `community_unlink_group`,
`community_fetch_linked_groups`, `community_fetch_all_participating`, `community_participants_update`,
`community_invite_code`, `community_revoke_invite`, `community_accept_invite`, `community_get_invite_info`,
`community_setting_update`, `community_member_add_mode`, `community_join_approval_mode`,
`community_toggle_ephemeral`, `community_request_participants_list/update`.

**Newsletters (channels)** — `newsletter_create(name, description=None)`, `newsletter_metadata(type, key)` (`type` = `'invite'` or `'jid'`),
`newsletter_update`, `newsletter_update_name`, `newsletter_update_description`, `newsletter_update_picture`,
`newsletter_remove_picture`, `newsletter_follow`, `newsletter_unfollow`, `newsletter_mute`, `newsletter_unmute`,
`newsletter_delete`, `newsletter_react_message(jid, server_id, reaction=None)`, `newsletter_fetch_messages(jid, count, since=None, after=None)`,
`newsletter_subscribers`, `newsletter_admin_count`, `newsletter_change_owner`, `newsletter_demote`,
`subscribe_newsletter_updates(jid)`.

**Business** — `get_business_profile(jid)`, `update_bussines_profile(args)` *(sic, as upstream)*,
`update_cover_photo`, `remove_cover_photo`, `get_catalog(jid=None, limit=None, cursor=None)`, `get_collections`, `get_order_details`,
`product_create`, `product_update`, `product_delete`.

**Calls / misc** — `reject_call(call_id, call_from)`, `create_call_link(type, event=None, timeout_ms=None)`,
`get_bot_list_v2()`, `fetch_account_reachout_timelock()`, `fetch_new_chat_message_cap()`,
`get_usync_devices`, `execute_usync_query`, `send_peer_data_operation_message`, `send_wam_buffer`.

## 16. Utilities

| name | |
|---|---|
| `generate_message_id`, `generate_message_id_v2`, `generate_wa_message`, `generate_wa_message_content`, `generate_forward_message_content`, `normalize_message_content`, `extract_message_content`, `get_content_type`, `get_key_author`, `update_message_with_reaction`, `update_message_with_receipt`, `download_media_message`, `download_content_from_message`, `get_url_from_direct_path`, `encrypt_stream`-family in `Utils.messages_media` | messages / media |
| `process_message`, `make_event_buffer` | event processing |
| `encode_binary_node`, `decode_binary_node`, `get_binary_node_child`, `get_binary_node_children`, `get_binary_node_child_string` | WABinary |
| `Curve`, `aes_encrypt_gcm`, `aes_decrypt_gcm`, `hkdf`, `sha256`, `hmac_sign` | crypto |
| `BufferJSON`, `fetch_latest_wa_web_version`, `fetch_latest_baileys_version`, `delay`, `unix_timestamp_seconds` | generics |
| `Browsers` (`ubuntu`, `macOS`, `windows`, `baileys`, `appropriate`) | browser strings |
| `from_object`, `to_dict`, `normalize`, `proto` | protobuf ↔ dict |
| `ILogger`, `logger` | pino-style logger (`logger.info(obj, msg)`) |

The exact exported names are whatever `import baileys; dir(baileys)` shows; each module keeps the file name of the
TypeScript original in snake_case (see [PORTING.md](PORTING.md)).


## Typed event classes

Each typed handler receives a subclass of `Event` with annotated fields. Annotate the parameter to get editor
autocomplete (`e.` lists the fields): `async def f(e: ReactionEvent)`. All are importable from `baileys`.

| Decorator | Class | Main fields |
|---|---|---|
| `on_connection` | `ConnectionEvent` | connection, qr, last_disconnect, is_new_login |
| `on_history` | `HistoryEvent` | chats, contacts, messages, is_latest, progress |
| `on_message_update` | `MessageUpdateEvent` | id, chat, sender, status, message, edited |
| `on_message_delete` | `MessageDeleteEvent` | id, chat, sender, all |
| `on_reaction` | `ReactionEvent` | message_id, chat, emoji, removed, reactor |
| `on_receipt` | `ReceiptEvent` | id, chat, user, status |
| `on_presence` | `PresenceEvent` | chat, jid, presence, last_seen |
| `on_participants`, `on_group_join/leave/promote/demote` | `ParticipantsEvent` | chat, author, participants, action |
| `on_chat_upsert/update` | `ChatEvent` | id, name, unread_count, archived, pinned |
| `on_chat_delete` | `ChatDeleteEvent` | id |
| `on_contact_upsert/update` | `ContactEvent` | id, name, notify, lid, phone_number |
| `on_group_upsert/update` | `GroupEvent` | id, subject, desc, announce, restrict, participants |
| `on_join_request` | `JoinRequestEvent` | chat, action, method, participant |
| `on_call` | `CallEvent` | id, chat, from_, status, is_video, is_group |
| `on_blocklist` | `BlocklistEvent` | jid, type |
| `on_label` / `on_label_association` | `LabelEvent` / `LabelAssociationEvent` | id, name, color / type, association |
| `on_settings_update` | `SettingsEvent` | value |
| `on_lid_mapping` | `LidMappingEvent` | lid, pn |
| `on_newsletter_reaction/view` | `NewsletterEvent` | id, server_id, count |
