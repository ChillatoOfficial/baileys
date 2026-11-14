"""`Message`: friendly wrapper around a raw WAMessage dict."""
from __future__ import annotations

import asyncio
import os
import re
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union

from .WABinary.jid_utils import is_lid_user, is_pn_user, jid_normalized_user

if TYPE_CHECKING:  # pragma: no cover
    from .client import Client
    from .filters import Filter
    from .models import Group
    from .schemas import MessageKey, WAMessage

_MEDIA_KEYS = {'imageMessage': 'image', 'videoMessage': 'video', 'audioMessage': 'audio',
               'documentMessage': 'document', 'stickerMessage': 'sticker'}
_WRAPPERS = ('ephemeralMessage', 'viewOnceMessage', 'viewOnceMessageV2', 'documentWithCaptionMessage',
             'editedMessage')
_YES = re.compile(r'^\s*(y|yes|si|sì|s|ok|sure|certo|confermo|1)\s*[.!]*\s*$', re.I)
_NO = re.compile(r'^\s*(n|no|nope|annulla|cancel|0)\s*[.!]*\s*$', re.I)

Source = Union[bytes, str, os.PathLike, dict]


def unwrap(content: Optional[dict]) -> dict:
    content = content or {}
    for _ in range(5):
        for w in _WRAPPERS:
            inner = (content.get(w) or {}).get('message')
            if inner:
                content = inner
                break
        else:
            break
    return content


def source(src: Source) -> Any:
    """bytes -> as is; path / URL string -> {'url': ...}; dict (e.g. {'stream': ...}) -> as is."""
    if isinstance(src, (bytes, bytearray, memoryview)):
        return bytes(src)
    if isinstance(src, dict):
        return src
    return {'url': os.fspath(src)}


class Message:
    """`m.raw` is the original dict. Attribute access never raises for absent optional data (returns None)."""

    def __init__(self, client: 'Client', raw: 'WAMessage'):
        self.client, self.raw = client, raw  # raw: WAMessage
        self.key: 'MessageKey' = raw.get('key') or {}
        self.content: dict = unwrap(raw.get('message'))
        self.state: dict = {}            # scratch space for middleware
        self.command: Optional[str] = None  # set when the message was dispatched as a command
        self.args: list[str] = []
        self.arg_text: str = ''

    # --- identity -----------------------------------------------------------------------------------------
    @property
    def id(self) -> str:
        return self.key.get('id')

    @property
    def chat(self) -> str:
        return self.key.get('remoteJid')

    @property
    def sender(self) -> str:
        """Who wrote it (in a group: the participant; in a 1:1 chat: the chat). May be a LID."""
        return self.key.get('participant') or self.key.get('participantAlt') or self.chat

    @property
    def from_me(self) -> bool:
        return bool(self.key.get('fromMe'))

    @property
    def is_group(self) -> bool:
        return bool(self.chat) and self.chat.endswith('@g.us')

    @property
    def push_name(self) -> Optional[str]:
        return self.raw.get('pushName')

    @property
    def timestamp(self) -> Optional[int]:
        t = self.raw.get('messageTimestamp')
        return int(t) if t is not None else None

    async def sender_phone(self) -> Optional[str]:
        """The sender as `<number>@s.whatsapp.net` even when WhatsApp addressed them by LID (None if unknown)."""
        for cand in (self.sender, self.key.get('participantAlt'), self.key.get('remoteJidAlt')):
            if is_pn_user(cand):
                return jid_normalized_user(cand)
        return await self.client.phone_of(self.sender)

    async def sender_number(self) -> Optional[str]:
        """Digits only, e.g. '393331234567'."""
        pn = await self.sender_phone()
        return pn.split('@')[0] if pn else None

    # --- content ------------------------------------------------------------------------------------------
    @property
    def media_type(self) -> Optional[str]:
        for k, v in _MEDIA_KEYS.items():
            if k in self.content:
                return v
        return None

    @property
    def text(self) -> Optional[str]:
        c = self.content
        return (c.get('conversation')
                or (c.get('extendedTextMessage') or {}).get('text')
                or next((c[k].get('caption') for k in _MEDIA_KEYS if k in c and c[k].get('caption')), None)
                or (c.get('buttonsResponseMessage') or {}).get('selectedButtonId')
                or (c.get('listResponseMessage') or {}).get('singleSelectReply', {}).get('selectedRowId'))

    @property
    def quoted(self) -> Optional['Message']:
        """The message this one replies to (if any)."""
        for v in self.content.values():
            ci = v.get('contextInfo') if isinstance(v, dict) else None
            if ci and ci.get('quotedMessage'):
                key = {'remoteJid': self.chat, 'id': ci.get('stanzaId'), 'participant': ci.get('participant'),
                       'fromMe': False}
                return Message(self.client, {'key': key, 'message': ci['quotedMessage']})
        return None

    @property
    def mentions(self) -> list:
        for v in self.content.values():
            ci = v.get('contextInfo') if isinstance(v, dict) else None
            if ci and ci.get('mentionedJid'):
                return list(ci['mentionedJid'])
        return []

    async def group(self, refresh: bool = False) -> Optional['Group']:
        """The `Group` this message was sent in (cached), or None in private chats."""
        return await self.client.group(self.chat, refresh=refresh) if self.is_group else None

    async def sender_is_admin(self) -> bool:
        g = await self.group()
        return bool(g and g.is_admin(self.sender))

    # --- sending ------------------------------------------------------------------------------------------
    async def reply(self, content: Union[str, dict], **options) -> 'Message':
        """`await m.reply('hi')` or `await m.reply({'image': {'url': ...}, 'caption': 'x'})` (quotes this message)"""
        return await self.client.send(self.chat, content, quoted=self.raw, **options)

    async def send(self, content: Union[str, dict], **options) -> 'Message':
        """Same as reply() but without quoting."""
        return await self.client.send(self.chat, content, **options)

    async def reply_image(self, src: Source, caption: Optional[str] = None, **options) -> 'Message':
        return await self.reply({'image': source(src), **({'caption': caption} if caption else {})}, **options)

    async def reply_video(self, src: Source, caption: Optional[str] = None, gif: bool = False, **options) -> 'Message':
        return await self.reply({'video': source(src), 'gifPlayback': gif, **({'caption': caption} if caption else {})},
                                **options)

    async def reply_audio(self, src: Source, voice_note: bool = False, **options) -> 'Message':
        """voice_note=True sends it as a push-to-talk voice message (needs OGG/Opus for best compatibility)."""
        mime = 'audio/ogg; codecs=opus' if voice_note else 'audio/mp4'
        return await self.reply({'audio': source(src), 'ptt': voice_note, 'mimetype': mime}, **options)

    async def reply_document(self, src: Source, filename: str, mimetype: str = 'application/octet-stream',
                             caption: Optional[str] = None, **options) -> 'Message':
        return await self.reply({'document': source(src), 'fileName': filename, 'mimetype': mimetype,
                                 **({'caption': caption} if caption else {})}, **options)

    async def reply_sticker(self, src: Source, convert: bool = True, pack: Optional[str] = None,
                            author: Optional[str] = None, **options) -> 'Message':
        """Sends a sticker. Images/GIFs/videos (bytes or file path) are converted to 512x512 WebP first
        (`convert=False` to send as is) -- WhatsApp never loads a sticker that is not WebP."""
        if convert:
            from .stickers import to_sticker
            data = bytes(src) if isinstance(src, (bytes, bytearray, memoryview)) else None
            if data is None and not isinstance(src, dict) and not str(src).startswith(('http://', 'https://')):
                with open(src, 'rb') as f:
                    data = f.read()
            if data is not None:
                src = await to_sticker(data, pack, author)
        return await self.reply({'sticker': source(src)}, **options)

    async def relay_to(self, jid: str, **options) -> str:
        """Relay this message's content to another chat as is (no 'forwarded' flag). Returns the new message id."""
        return await self.client.relay(jid, self.raw.get('message') or {}, **options)

    async def reply_location(self, latitude: float, longitude: float, name: Optional[str] = None,
                             address: Optional[str] = None, **options) -> 'Message':
        loc = {'degreesLatitude': latitude, 'degreesLongitude': longitude}
        if name:
            loc['name'] = name
        if address:
            loc['address'] = address
        return await self.reply({'location': loc}, **options)

    async def reply_contact(self, name: str, number: str, **options) -> 'Message':
        digits = re.sub(r'\D', '', number)
        vcard = f'BEGIN:VCARD\nVERSION:3.0\nFN:{name}\nTEL;type=CELL;type=VOICE;waid={digits}:+{digits}\nEND:VCARD'
        return await self.reply({'contacts': {'displayName': name, 'contacts': [{'vcard': vcard}]}}, **options)

    async def reply_poll(self, question: str, choices: Sequence[str], multiple: bool = False, **options) -> 'Message':
        return await self.reply({'poll': {'name': question, 'values': list(choices),
                                          'selectableCount': len(choices) if multiple else 1}}, **options)

    async def reply_mentioning(self, text: str, jids: Sequence[str], **options) -> 'Message':
        """text should contain @<number> for every jid."""
        return await self.reply({'text': text, 'mentions': list(jids)}, **options)

    async def mention_all(self, text: str = '', **options) -> 'Message':
        g = await self.group()
        if g is None:
            raise ValueError('mention_all only works in groups')
        return await g.mention_all(text, **options)

    async def react(self, emoji: str) -> 'Message':
        """Empty string removes your reaction."""
        return await self.client.send(self.chat, {'react': {'text': emoji, 'key': self.key}})

    async def edit(self, text: str) -> 'Message':
        """Edit a message you sent (e.g. the result of `await m.reply(...)`)."""
        return await self.client.send(self.chat, {'text': text, 'edit': self.key})

    async def delete(self) -> 'Message':
        """Delete for everyone (your own messages; or anybody's if you are a group admin)."""
        return await self.client.send(self.chat, {'delete': self.key})

    async def forward(self, to: str, **options) -> 'Message':
        return await self.client.send(to, {'forward': self.raw}, **options)

    async def read(self):
        return await self.client.sock.read_messages([self.key])

    async def download(self, type: str = 'buffer'):
        """Download and decrypt the attached media (`type='buffer'` -> bytes, `'stream'` -> async iterator)."""
        from .Utils.messages import download_media_message
        return await download_media_message(self.raw, type, {}, {
            'logger': self.client.logger, 'reuploadRequest': self.client.sock.update_media_message})

    async def save(self, path: str) -> str:
        """Download the attached media to `path` (adds an extension when `path` is a directory)."""
        data = await self.download()
        if os.path.isdir(path):
            ext = {'image': '.jpg', 'video': '.mp4', 'audio': '.ogg', 'sticker': '.webp'}.get(self.media_type, '.bin')
            path = os.path.join(path, f'{self.id}{ext}')
        await asyncio.get_running_loop().run_in_executor(None, _write_bytes, path, data)
        return path

    async def typing(self, seconds: float = 0):
        await self.client.sock.send_presence_update('composing', self.chat)
        if seconds:
            await asyncio.sleep(seconds)
            await self.client.sock.send_presence_update('paused', self.chat)

    # --- conversations ------------------------------------------------------------------------------------
    async def ask(self, prompt: Optional[str] = None, *, timeout: float = 60, filter: Optional['Filter'] = None,
                  same_sender: bool = True, quote: bool = True) -> 'Message':
        """Optionally send `prompt`, then wait for the next message in this chat (from the same sender) that
        passes `filter`. Raises asyncio.TimeoutError after `timeout` seconds. Messages consumed here do not reach
        the normal handlers."""
        waiter = self.client.wait_for(chat=self.chat, sender=self.sender if same_sender else None, filter=filter,
                                      timeout=timeout)
        if prompt:
            await (self.reply(prompt) if quote else self.send(prompt))
        return await waiter

    async def confirm(self, prompt: str, *, timeout: float = 30) -> Optional[bool]:
        """Yes/no question. Returns True/False, or None on timeout / unrecognised answer."""
        try:
            a = await self.ask(prompt, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        t = a.text or ''
        return True if _YES.match(t) else False if _NO.match(t) else None

    async def choose(self, prompt: str, options: Sequence[str], *, timeout: float = 30) -> Optional[str]:
        """Numbered menu; returns the chosen option or None on timeout / invalid answer."""
        menu = prompt + '\n' + '\n'.join(f'{i}. {o}' for i, o in enumerate(options, 1))
        try:
            a = await self.ask(menu, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        t = (a.text or '').strip()
        if t.isdigit() and 1 <= int(t) <= len(options):
            return options[int(t) - 1]
        return next((o for o in options if o.lower() == t.lower()), None)

    def __repr__(self):
        return f'<Message {self.chat} from={self.sender} text={self.text!r}>'


def _write_bytes(path: str, data: bytes):
    with open(path, 'wb') as f:
        f.write(data)


# kept for type checkers: a handler is a (maybe async) callable
Handler = Callable[..., Any]
__all__ = ['Message', 'unwrap', 'source', 'is_lid_user']
