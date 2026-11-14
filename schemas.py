"""TypedDict descriptions of every dict the library passes around (for IDEs / mypy; nothing is enforced at runtime).

Naming follows the original Baileys types. Keys keep their JavaScript camelCase spelling because that is what the
dicts really contain. `total=False` means every key is optional.
"""
from __future__ import annotations

from typing import Any, Callable, Literal, Optional, TypedDict, Union

JID = str  # '<number>@s.whatsapp.net' | '<id>@lid' | '<id>@g.us' | '<id>@newsletter' | 'status@broadcast' | ...
Bytes = bytes


# =====================================================================================================================
# keys, messages
# =====================================================================================================================
class MessageKey(TypedDict, total=False):
    remoteJid: JID
    id: str
    fromMe: bool
    participant: JID        # sender inside a group
    participantAlt: JID     # the same sender in the other addressing mode (LID <-> phone number)
    remoteJidAlt: JID


class WAMessage(TypedDict, total=False):
    key: MessageKey
    message: dict[str, Any]       # the protobuf Message as a dict: {'conversation': ...} / {'imageMessage': {...}} ...
    messageTimestamp: int
    pushName: str
    status: int                   # 0 error, 1 pending, 2 sent, 3 delivered, 4 read, 5 played
    broadcast: bool
    participant: JID
    messageStubType: int
    messageStubParameters: list[str]


class ContextInfo(TypedDict, total=False):
    stanzaId: str
    participant: JID
    quotedMessage: dict[str, Any]
    mentionedJid: list[JID]
    isForwarded: bool
    forwardingScore: int
    expiration: int


# =====================================================================================================================
# content you pass to send_message / client.send / m.reply  (AnyMessageContent)
# =====================================================================================================================
class _Mentionable(TypedDict, total=False):
    mentions: list[JID]
    mentionAll: bool
    contextInfo: ContextInfo
    edit: MessageKey              # turn the content into an edit of this (your own) message
    viewOnce: bool


class TextContent(_Mentionable):
    text: str
    linkPreview: Optional[dict]   # None disables the automatic preview
    backgroundColor: str
    font: int


class MediaCommon(_Mentionable, total=False):
    caption: str
    mimetype: str
    fileName: str
    jpegThumbnail: bytes
    gifPlayback: bool
    ptt: bool                     # voice note
    seconds: int
    width: int
    height: int


# media sources: bytes | {'url': 'path-or-http(s)-url'} | {'stream': async iterator of bytes}
MediaSource = Union[bytes, dict]


class ImageContent(MediaCommon):
    image: MediaSource


class VideoContent(MediaCommon):
    video: MediaSource
    ptv: bool                     # round "video note"


class AudioContent(MediaCommon):
    audio: MediaSource


class DocumentContent(MediaCommon):
    document: MediaSource


class StickerContent(MediaCommon):
    sticker: MediaSource


class LocationContent(TypedDict):
    location: dict                # {'degreesLatitude', 'degreesLongitude', 'name'?, 'address'?}


class ContactsContent(TypedDict):
    contacts: dict                # {'displayName': str, 'contacts': [{'vcard': 'BEGIN:VCARD...'}]}


class ReactContent(TypedDict):
    react: dict                   # {'text': '👍' ('' removes), 'key': MessageKey}


class DeleteContent(TypedDict):
    delete: MessageKey


class ForwardContent(TypedDict, total=False):
    forward: WAMessage
    force: bool


class PollInfo(TypedDict, total=False):
    name: str
    values: list[str]
    selectableCount: int          # 0 = no limit, 1 = single choice
    toAnnouncementGroup: bool


class PollContent(TypedDict):
    poll: PollInfo


class EventInfo(TypedDict, total=False):
    name: str
    description: str
    startDate: Any                # datetime
    endDate: Any
    location: dict
    call: Literal['audio', 'video']
    isCancelled: bool
    extraGuestsAllowed: bool


class EventContent(TypedDict):
    event: EventInfo


class PinContent(TypedDict, total=False):
    pin: MessageKey
    type: int                     # 1 pin, 2 unpin
    time: int                     # seconds, default 86400


class GroupInviteContent(TypedDict):
    groupInvite: dict             # {'inviteCode', 'inviteExpiration', 'text', 'jid', 'subject'}


class ButtonReplyContent(TypedDict):
    buttonReply: dict             # {'displayText', 'id', 'index'}
    type: Literal['plain', 'template']


class ListReplyContent(TypedDict):
    listReply: dict


class ProductContent(TypedDict, total=False):
    product: dict
    businessOwnerJid: JID
    body: str
    footer: str


class DisappearingContent(TypedDict):
    disappearingMessagesInChat: Union[bool, int]   # True = default (7 days), int = seconds, False/0 = off


AnyMessageContent = Union[
    TextContent, ImageContent, VideoContent, AudioContent, DocumentContent, StickerContent, LocationContent,
    ContactsContent, ReactContent, DeleteContent, ForwardContent, PollContent, EventContent, PinContent,
    GroupInviteContent, ButtonReplyContent, ListReplyContent, ProductContent, DisappearingContent]


class SendOptions(TypedDict, total=False):
    """Options of `send_message` (pass them as keyword arguments to `client.send` / `m.reply`)."""
    quoted: WAMessage
    messageId: str
    timestamp: Any                # datetime
    ephemeralExpiration: int
    statusJidList: list[JID]      # recipients when sending to status@broadcast
    useCachedGroupMetadata: bool
    useUserDevicesCache: bool
    participant: JID
    additionalAttributes: dict[str, str]
    additionalNodes: list[dict]
    backgroundColor: str
    font: int
    mediaUploadTimeoutMs: int


# =====================================================================================================================
# groups, contacts, chats
# =====================================================================================================================
class Participant(TypedDict, total=False):
    id: JID
    lid: Optional[JID]
    phoneNumber: Optional[JID]
    username: Optional[str]
    admin: Optional[Literal['admin', 'superadmin']]


class GroupMetadata(TypedDict, total=False):
    id: JID
    notify: str
    addressingMode: Literal['pn', 'lid']
    subject: str
    subjectOwner: JID
    subjectOwnerPn: JID
    subjectTime: int
    desc: Optional[str]
    descId: str
    descOwner: JID
    descTime: int
    owner: Optional[JID]
    creation: int
    size: int
    announce: bool                # only admins can send
    restrict: bool                # only admins can edit group info
    joinApprovalMode: bool
    memberAddMode: bool
    isCommunity: bool
    isCommunityAnnounce: bool
    linkedParent: JID
    ephemeralDuration: int
    inviteCode: str
    participants: list[Participant]


class ParticipantUpdateResult(TypedDict):
    status: str                   # '200' ok
    jid: JID
    content: dict


class ContactInfo(TypedDict, total=False):
    id: JID
    lid: JID
    phoneNumber: JID
    name: str                     # name in your address book
    notify: str                   # push name chosen by the user
    verifiedName: str
    imgUrl: Optional[str]         # None = removed, 'changed' = fetch again
    status: str


class ChatInfo(TypedDict, total=False):
    id: JID
    name: str
    displayName: str
    unreadCount: int
    archived: bool
    pinned: int
    muteEndTime: int
    conversationTimestamp: int
    ephemeralExpiration: int
    readOnly: bool
    lidJid: JID
    pnJid: JID


class Label(TypedDict, total=False):
    id: str
    name: str
    color: int                    # LabelColor
    deleted: bool
    predefinedId: str


class ChatModification(TypedDict, total=False):
    """`sock.chat_modify(mod, jid)` -- exactly one of these keys per call."""
    archive: bool
    pin: bool
    mute: Optional[int]           # unix ms when the mute ends, None to unmute
    markRead: bool
    clear: bool
    delete: bool
    deleteForMe: dict             # {'deleteMedia': bool, 'key': MessageKey, 'timestamp': int}
    star: dict                    # {'messages': [{'id', 'fromMe'}], 'star': bool}
    contact: Optional[dict]
    disableLinkPreviews: dict
    pushNameSetting: str
    quickReply: dict
    addLabel: Label
    addChatLabel: dict
    removeChatLabel: dict
    addMessageLabel: dict
    removeMessageLabel: dict
    lastMessages: list[WAMessage]  # required for archive / markRead / clear / delete


# =====================================================================================================================
# event payloads (the raw events: @client.on('<name>'))
# =====================================================================================================================
class LastDisconnect(TypedDict):
    error: Optional[BaseException]  # a Boom: err.output['statusCode'] is a DisconnectReason
    date: Any


class ConnectionUpdate(TypedDict, total=False):
    connection: Literal['connecting', 'open', 'close']
    lastDisconnect: LastDisconnect
    isNewLogin: bool
    qr: str
    receivedPendingNotifications: bool
    legacy: dict
    isOnline: bool


class MessagesUpsert(TypedDict):
    messages: list[WAMessage]
    type: Literal['notify', 'append']   # notify = new incoming; append = history / sent from another device
    requestId: str


class MessageUpdate(TypedDict):
    key: MessageKey
    update: dict                  # partial WAMessage: status, message (edited), pollUpdates, ...


class MessagesDelete(TypedDict, total=False):
    keys: list[MessageKey]
    jid: JID
    all: bool


class Reaction(TypedDict):
    key: MessageKey               # the message that was reacted to
    reaction: dict                # {'key': reactor's key, 'text': emoji ('' = removed), 'senderTimestampMs'}


class MessageUserReceipt(TypedDict, total=False):
    userJid: JID
    receiptTimestamp: int
    readTimestamp: int
    playedTimestamp: int


class MessageReceipt(TypedDict):
    key: MessageKey
    receipt: MessageUserReceipt


class PresenceData(TypedDict, total=False):
    lastKnownPresence: Literal['unavailable', 'available', 'composing', 'recording', 'paused']
    lastSeen: int


class PresenceUpdate(TypedDict):
    id: JID
    presences: dict[JID, PresenceData]


class GroupParticipantsUpdate(TypedDict, total=False):
    id: JID
    author: JID
    participants: list
    action: Literal['add', 'remove', 'promote', 'demote', 'modify']


class CallEvent(TypedDict, total=False):
    chatId: JID
    from_: JID                    # 'from' in the raw dict
    id: str
    date: Any
    isVideo: bool
    isGroup: bool
    status: Literal['offer', 'ringing', 'timeout', 'reject', 'accept', 'terminate']
    offline: bool
    latencyMs: int
    groupJid: JID


class BlocklistUpdate(TypedDict):
    blocklist: list[JID]
    type: Literal['add', 'remove']


class MessagingHistorySet(TypedDict, total=False):
    chats: list[ChatInfo]
    contacts: list[ContactInfo]
    messages: list[WAMessage]
    isLatest: bool
    progress: int
    syncType: int
    peerDataRequestSessionId: str


class LidMappingUpdate(TypedDict):
    lid: JID
    pn: JID


# =====================================================================================================================
# authentication
# =====================================================================================================================
class KeyPair(TypedDict):
    public: bytes
    private: bytes


class SignedKeyPair(TypedDict, total=False):
    keyPair: KeyPair
    signature: bytes
    keyId: int
    timestampS: int


class AuthenticationCreds(TypedDict, total=False):
    noiseKey: KeyPair
    pairingEphemeralKeyPair: KeyPair
    signedIdentityKey: KeyPair
    signedPreKey: SignedKeyPair
    registrationId: int
    advSecretKey: str
    processedHistoryMessages: list[dict]
    nextPreKeyId: int
    firstUnuploadedPreKeyId: int
    accountSyncCounter: int
    accountSettings: dict
    registered: bool
    pairingCode: Optional[str]
    lastPropHash: Optional[str]
    routingInfo: Optional[bytes]
    additionalData: Optional[Any]
    me: dict                      # {'id': '<number>:<device>@s.whatsapp.net', 'lid': ..., 'name': ...}
    account: dict
    signalIdentities: list[dict]
    myAppStateKeyId: str
    platform: str


# the Signal key store categories (SignalDataTypeMap)
SignalKeyType = Literal['pre-key', 'session', 'sender-key', 'sender-key-memory', 'app-state-sync-key',
                        'app-state-sync-version', 'lid-mapping', 'device-list', 'tctoken', 'identity-key']


class SignalKeyStore(TypedDict):
    """Protocol, shown as a TypedDict for brevity: `async get(type, ids) -> {id: value}`,
    `async set({type: {id: value | None}})` (None deletes), optional `transaction(work, key)`."""
    get: Callable
    set: Callable


class AuthenticationState(TypedDict):
    creds: AuthenticationCreds
    keys: SignalKeyStore


# =====================================================================================================================
# socket configuration (make_wa_socket({...}) / Client(..., **config))
# =====================================================================================================================
class SocketConfig(TypedDict, total=False):
    version: list[int]                          # WhatsApp Web version [2, 3000, N]
    browser: list[str]                          # [os, browser, version] shown in "linked devices"
    waWebSocketUrl: str
    connectTimeoutMs: int
    keepAliveIntervalMs: int
    defaultQueryTimeoutMs: int
    qrTimeout: int
    logger: Any                                 # ILogger
    auth: AuthenticationState
    emitOwnEvents: bool                         # emit messages.upsert for messages you send
    markOnlineOnConnect: bool
    syncFullHistory: bool
    fireInitQueries: bool                       # profile / blocklist / props queries on connect
    generateHighQualityLinkPreview: bool
    linkPreviewImageThumbnailWidth: int
    countryCode: str
    maxMsgRetryCount: int
    retryRequestDelayMs: int
    enableAutoSessionRecreation: bool
    enableRecentMessageCache: bool
    transactionOpts: dict                       # {'maxCommitRetries': 10, 'delayBetweenTriesMs': 3000}
    appStateMacVerification: dict               # {'patch': bool, 'snapshot': bool}
    customUploadHosts: list[str]
    options: dict                               # extra httpx options for media / link previews
    agent: Any
    # callbacks
    getMessage: Callable                        # async (key) -> proto.Message dict | None   (retries, polls)
    cachedGroupMetadata: Callable               # async (jid) -> GroupMetadata | None
    patchMessageBeforeSending: Callable         # (message, recipientJids) -> message
    shouldSyncHistoryMessage: Callable          # (historySyncMessage) -> bool
    shouldIgnoreJid: Callable                   # (jid) -> bool
    makeSignalRepository: Callable
    msgRetryCounterCache: Any
    userDevicesCache: Any
    placeholderResendCache: Any
    callOfferCache: Any
    mediaCache: Any
