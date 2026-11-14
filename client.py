"""High level, decorator based API on top of the 1:1 port.

    from baileys import Client, filters

    client = Client('auth')

    @client.command('ping', cooldown=3)
    async def ping(m):
        await m.reply('pong')

    @client.command('kick', admin_only=True, bot_admin=True, group_only=True)
    async def kick(m, args):
        g = await m.group()
        await g.remove(*m.mentions)

    @client.message(filters.text & ~filters.from_me)
    async def echo(m):
        print(m.sender, m.text)

    client.run()

The low level socket (`make_wa_socket`) is still available as `client.sock` and every unknown attribute of the
client is forwarded to it (`await client.group_metadata(...)`, `await client.reject_call(...)`, ...).
"""
from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import shlex
import signal
import sys
import time
import types
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

from .Defaults import DEFAULT_CONNECTION_CONFIG
from .Socket import make_wa_socket
from .Types import DisconnectReason
from .Utils.auth_utils import make_cacheable_signal_key_store
from .Utils.generics import fetch_latest_wa_web_version
from .Utils.logger import ILogger
from .Utils.use_multi_file_auth_state import use_multi_file_auth_state
from .WABinary.jid_utils import is_lid_user, is_pn_user, jid_normalized_user
from .commands import DEFAULT_DENIED, Command
from .events import TYPED
from .filters import Filter
from .message import Message
from .models import Contact, Group
from .plugins import Plugin
from .queue import make_queue

Handler = Callable[..., Any]
F = TypeVar("F", bound=Callable[..., Any])
Middleware = Callable[[Message, Callable[[], Awaitable[None]]], Awaitable[None]]


class Client:
    def __init__(self, auth: Any = 'baileys_auth_info', *, prefix: Union[str, tuple] = '/', print_qr: bool = True,
                 pairing_phone: Optional[str] = None, auto_reconnect: bool = True, fetch_version: bool = False,
                 ignore_from_me: bool = True, only_notify: bool = True, log_level: str = 'warn',
                 owners: Union[list, tuple] = (), help_command: bool = True, denied_messages: Any = None,
                 rate_limit: Any = None, auto_read: bool = False, group_cache_ttl: float = 300,
                 name: Optional[str] = None, **config: Any):
        """
        auth            folder (multi-file store), a '*.db' / '*.sqlite' path (SQLite store), a `(state, save)`
                        tuple, or an async callable returning one (see baileys.storage for Redis / custom)
        prefix          command prefix(es) for @client.command
        print_qr        print the QR code in the terminal (needs `pip install qrcode`, falls back to the raw string)
        pairing_phone   login with a pairing code for this phone number instead of the QR
        auto_reconnect  reconnect on every close except logged-out
        ignore_from_me  @message / @command skip messages sent by this account (override per handler)
        only_notify     only real incoming messages (type == 'notify'), skipping history appends
        owners          phone numbers (digits) allowed to use `owner_only` commands; the bot's own account always is
        help_command    register a built-in /help listing the commands that have `help=...` text
        denied_messages dict overriding the replies sent when a command is refused (False = stay silent)
        rate_limit      None -> sensible anti-flood defaults; False -> off; dict -> SendQueue options
                        ({'per_chat': 0.6, 'global_': 0.12, 'jitter': 0.25, 'retries': 2})
        auto_read       mark incoming messages as read
        group_cache_ttl seconds group metadata stays cached (invalidated by group events)
        log_level       'trace'|'debug'|'info'|'warn'|'error'|'silent' (default 'warn')
        **config        any socket option (browser, markOnlineOnConnect, syncFullHistory, ...)
        """
        self.name = name or (auth if isinstance(auth, str) else 'client')
        self.auth = auth
        self.prefixes = (prefix,) if isinstance(prefix, str) else tuple(prefix)
        self.print_qr, self.pairing_phone = print_qr, pairing_phone
        self._pairing_requested = False
        self.auto_reconnect, self.fetch_version = auto_reconnect, fetch_version
        self.ignore_from_me, self.only_notify, self.auto_read = ignore_from_me, only_notify, auto_read
        self.owners = {''.join(ch for ch in str(o) if ch.isdigit()) for o in owners}
        self.help_command = help_command
        self.denied_messages = False if denied_messages is False else {**DEFAULT_DENIED, **(denied_messages or {})}
        self.group_cache_ttl = group_cache_ttl
        self.config = config
        self.logger = config.pop('logger', None) or ILogger('baileys', log_level)
        self.queue = make_queue(rate_limit, self.logger)
        self.sock = None
        self._handlers: dict[str, list[Handler]] = {}
        self._message_handlers: list[tuple[Optional[Filter], bool, Handler]] = []
        self._commands: dict[str, Command] = {}
        self._middleware: list[Middleware] = []
        self._waiters: list[tuple[Callable[[Message], bool], asyncio.Future, bool]] = []
        self._plugins: dict[str, dict] = {}
        self._groups: dict[str, tuple[float, dict]] = {}
        self.contacts: dict[str, dict] = {}   # filled from contacts.upsert / contacts.update events
        self.chats: dict[str, dict] = {}      # filled from chats.upsert / chats.update events
        self._unsub: Optional[Callable] = None
        self._closed: Optional[asyncio.Future] = None
        self._stopping = False
        self._ready: Optional[asyncio.Event] = None  # created in start(), inside the running loop
        self._logged_out = False
        self._save_creds: Optional[Callable] = None

    # =========================================================================================================
    # decorators
    # =========================================================================================================
    def on(self, event: str) -> Callable[[F], F]:
        """Raw socket event: @client.on('messages.upsert'), @client.on('group-participants.update'), ..."""
        def deco(fn: F) -> F:
            self._handlers.setdefault(event, []).append(fn)
            return fn
        return deco

    def message(self, *filters: Filter, from_me: Optional[bool] = None) -> Callable[[F], F]:
        """@client.message(filters.text & filters.private) -> handler(m: Message)"""
        combined: Optional[Filter] = None
        for f in filters:
            combined = f if combined is None else combined & f

        def deco(fn: F) -> F:
            self._message_handlers.append((combined, self.ignore_from_me if from_me is None else not from_me, fn))
            return fn
        return deco

    def command(self, name: str, *aliases: str, help: Optional[str] = None, admin_only: bool = False,
                bot_admin: bool = False, group_only: bool = False, private_only: bool = False,
                owner_only: bool = False, cooldown: float = 0, hidden: bool = False,
                filter: Optional[Filter] = None, from_me: Optional[bool] = None) -> Callable[[F], F]:
        """@client.command('ping', 'p', cooldown=3, group_only=True) -> handler(m) or handler(m, args)

        help         text shown by the built-in /help
        admin_only   sender must be a group admin (implies group_only)
        bot_admin    the bot must be a group admin (implies group_only)
        owner_only   sender must be the bot's own account or one of `owners`
        cooldown     seconds per user and command
        filter       extra Filter that must match, otherwise the command is ignored silently
        Inside the handler: m.command, m.args (list), m.arg_text (string after the command)."""
        def deco(fn: F) -> F:
            try:
                n = len(inspect.signature(fn).parameters)
            except (TypeError, ValueError):
                n = 1
            cmd = Command(name=name.lower(), fn=fn, aliases=tuple(a.lower() for a in aliases), help=help,
                          skip_me=self.ignore_from_me if from_me is None else not from_me,
                          admin_only=admin_only, bot_admin=bot_admin, group_only=group_only or admin_only or bot_admin,
                          private_only=private_only, owner_only=owner_only, cooldown=cooldown, hidden=hidden,
                          filter=filter, n_params=n)
            for n_ in (cmd.name, *cmd.aliases):
                self._commands[n_] = cmd
            return fn
        return deco

    def use(self, fn: Middleware) -> Middleware:
        """Middleware: `async def mw(m, call_next)`. Runs around every incoming message (handlers and commands).
        Do your thing, then `await call_next()` to continue, or return to stop processing the message.

            @client.use
            async def blacklist(m, call_next):
                if m.sender in BANNED:
                    return
                await call_next()
        """
        self._middleware.append(fn)
        return fn

    def on_ready(self, fn: Handler) -> Handler:
        """Connection opened."""
        return self.on('ready')(fn)

    def on_pairing_code(self, fn: Handler) -> Handler:
        """handler(code: str) -- called with the 8-char pairing code ('ABCD-1234') when `pairing_phone` is set;
        replaces the default terminal message."""
        return self.on('pairing_code')(fn)

    def on_qr(self, fn: Handler) -> Handler:
        """handler(qr: str) -- replaces the default terminal rendering."""
        return self.on('qr')(fn)

    def on_close(self, fn: Handler) -> Handler:
        """handler(reason: DisconnectReason | None)"""
        return self.on('close')(fn)

    def on_error(self, fn: Handler) -> Handler:
        """handler(error, handler_fn) for exceptions raised inside handlers."""
        return self.on('error')(fn)

    def on_denied(self, fn: Handler) -> Handler:
        """handler(m, reason, command): a command was refused. reason is one of 'admin_only', 'bot_admin',
        'group_only', 'private_only', 'owner_only', 'cooldown'. Replaces the default reply."""
        return self.on('denied')(fn)

    # --- typed events (see baileys/events.py): one call per affected item, payload is an `Event` --------------
    def _typed(self, name: str, fn: Handler) -> Handler:
        event, convert, pred = TYPED[name]

        async def wrapper(data):
            if pred is not None and not pred(data):
                return
            for e in convert(data):
                e.client = self
                await self._safe(fn, e)
        wrapper.__name__ = getattr(fn, '__name__', name)
        self._handlers.setdefault(event, []).append(wrapper)
        return fn

    def on_connection(self, fn: Handler) -> Handler:
        """handler(e): e.connection ('connecting'|'open'|'close'), e.qr, e.last_disconnect, e.is_new_login ..."""
        return self._typed('on_connection', fn)

    def on_history(self, fn: Handler) -> Handler:
        """handler(e): e.chats, e.contacts, e.messages, e.is_latest, e.sync_type (history sync chunks)."""
        return self._typed('on_history', fn)

    def on_message_update(self, fn: Handler) -> Handler:
        """handler(e): e.key, e.id, e.chat, e.status ('sent'|'delivered'|'read'|'played'), e.edited, e.message"""
        return self._typed('on_message_update', fn)

    def on_message_delete(self, fn: Handler) -> Handler:
        """handler(e): e.id, e.chat, e.key -- or e.all=True when the whole chat was cleared."""
        return self._typed('on_message_delete', fn)

    def on_reaction(self, fn: Handler) -> Handler:
        """handler(e): e.emoji (None if removed), e.removed, e.reactor, e.chat, e.message_id"""
        return self._typed('on_reaction', fn)

    def on_receipt(self, fn: Handler) -> Handler:
        """handler(e): e.id, e.chat, e.user, e.status ('delivered'|'read'|'played')"""
        return self._typed('on_receipt', fn)

    def on_presence(self, fn: Handler) -> Handler:
        """handler(e): e.chat, e.jid, e.presence ('composing'|'recording'|'available'|...), e.last_seen"""
        return self._typed('on_presence', fn)

    def on_chat_upsert(self, fn: Handler) -> Handler:
        return self._typed('on_chat_upsert', fn)

    def on_chat_update(self, fn: Handler) -> Handler:
        return self._typed('on_chat_update', fn)

    def on_chat_delete(self, fn: Handler) -> Handler:
        return self._typed('on_chat_delete', fn)

    def on_contact_upsert(self, fn: Handler) -> Handler:
        return self._typed('on_contact_upsert', fn)

    def on_contact_update(self, fn: Handler) -> Handler:
        return self._typed('on_contact_update', fn)

    def on_group_upsert(self, fn: Handler) -> Handler:
        """handler(e): a group you were added to / created -- e.id, e.subject, e.participants ..."""
        return self._typed('on_group_upsert', fn)

    def on_group_update(self, fn: Handler) -> Handler:
        """handler(e): e.id plus the changed fields (subject, desc, announce, restrict ...)"""
        return self._typed('on_group_update', fn)

    def on_participants(self, fn: Handler) -> Handler:
        """handler(e): any change -- e.chat, e.action ('add'|'remove'|'promote'|'demote'|'modify'), e.participants, e.author"""
        return self._typed('on_participants', fn)

    def on_group_join(self, fn: Handler) -> Handler:
        """handler(e): participants added to a group."""
        return self._typed('on_group_join', fn)

    def on_group_leave(self, fn: Handler) -> Handler:
        """handler(e): participants removed / left."""
        return self._typed('on_group_leave', fn)

    def on_group_promote(self, fn: Handler) -> Handler:
        return self._typed('on_group_promote', fn)

    def on_group_demote(self, fn: Handler) -> Handler:
        return self._typed('on_group_demote', fn)

    def on_join_request(self, fn: Handler) -> Handler:
        return self._typed('on_join_request', fn)

    def on_call(self, fn: Handler) -> Handler:
        """handler(e): e.id, e.chat_id, e.from_, e.status ('offer'|'accept'|'reject'|'timeout'|'terminate'), e.is_video"""
        return self._typed('on_call', fn)

    def on_blocklist(self, fn: Handler) -> Handler:
        """handler(e): e.jid, e.type ('add'|'remove') -- or the full list on first load (type 'set')."""
        return self._typed('on_blocklist', fn)

    def on_label(self, fn: Handler) -> Handler:
        return self._typed('on_label', fn)

    def on_label_association(self, fn: Handler) -> Handler:
        return self._typed('on_label_association', fn)

    def on_settings_update(self, fn: Handler) -> Handler:
        return self._typed('on_settings_update', fn)

    def on_lid_mapping(self, fn: Handler) -> Handler:
        return self._typed('on_lid_mapping', fn)

    def on_newsletter_reaction(self, fn: Handler) -> Handler:
        return self._typed('on_newsletter_reaction', fn)

    def on_newsletter_view(self, fn: Handler) -> Handler:
        return self._typed('on_newsletter_view', fn)

    # =========================================================================================================
    # actions
    # =========================================================================================================
    async def send(self, jid: str, content: Union[str, dict], **options) -> Message:
        """send('123@s.whatsapp.net', 'text') -- strings become {'text': ...}; kwargs are send_message options
        (quoted=..., ephemeralExpiration=...). Goes through the rate-limited queue and returns the sent `Message`
        (so you can `await sent.edit('new text')` or `await sent.delete()`)."""
        if isinstance(content, str):
            content = {'text': content}

        async def do():
            return await self.sock.send_message(jid, content, options or None)
        raw = await (self.queue.submit(jid, do) if self.queue else do())
        return Message(self, raw or {})

    async def send_now(self, jid: str, content: Union[str, dict], **options) -> Message:
        """Like send() but skips the queue's spacing (still retried)."""
        if isinstance(content, str):
            content = {'text': content}

        async def do():
            return await self.sock.send_message(jid, content, options or None)
        raw = await (self.queue.submit(jid, do, priority_bypass=True) if self.queue else do())
        return Message(self, raw or {})

    async def relay(self, jid: str, message: dict, *, message_id: Optional[str] = None,
                    participant: Optional[dict] = None, additional_attributes: Optional[dict] = None,
                    additional_nodes: Optional[list] = None, use_cached_group_metadata: Optional[bool] = None,
                    status_jid_list: Optional[list] = None, use_user_devices_cache: Optional[bool] = None) -> str:
        """relayMessage: encrypt and ship an already-built proto Message dict (e.g. `{'conversation': 'hi'}` or
        another message's `raw['message']`) without send_message's content generation. Returns the message id.
        `participant={'jid': ..., 'count': n}` re-sends to a single device (retry flow)."""
        opts = {'messageId': message_id, 'participant': participant, 'additionalAttributes': additional_attributes,
                'additionalNodes': additional_nodes, 'useCachedGroupMetadata': use_cached_group_metadata,
                'statusJidList': status_jid_list, 'useUserDevicesCache': use_user_devices_cache}
        opts = {k: v for k, v in opts.items() if v is not None}

        async def do():
            return await self.sock.relay_message(jid, message, opts)
        return await (self.queue.submit(jid, do) if self.queue else do())

    async def broadcast(self, jids: list, content: Union[str, dict], **options) -> list:
        """Send the same content to many chats one by one through the queue (spaced to avoid flooding)."""
        return [await self.send(j, content, **options) for j in jids]

    # --- groups / contacts / identities ---------------------------------------------------------------------
    async def group(self, jid: str, refresh: bool = False) -> Group:
        """Group object with cached metadata (cache is dropped on group events)."""
        hit = self._groups.get(jid)
        if hit and not refresh and time.monotonic() - hit[0] < self.group_cache_ttl:
            return Group(self, hit[1])
        meta = await self.sock.group_metadata(jid)
        self._groups[jid] = (time.monotonic(), meta)
        return Group(self, meta)

    async def groups(self) -> list[Group]:
        """All groups you take part in."""
        all_ = await self.sock.group_fetch_all_participating()
        now = time.monotonic()
        for jid, meta in all_.items():
            self._groups[jid] = (now, meta)
        return [Group(self, m) for m in all_.values()]

    def contact(self, jid: str) -> Optional[Contact]:
        """Contact info synced from the phone (None if unknown)."""
        data = self.contacts.get(jid_normalized_user(jid)) or self.contacts.get(jid)
        return Contact(self, data) if data else None

    def name_of(self, jid: str) -> Optional[str]:
        c = self.contact(jid)
        return c.name if c else None

    def _lid_mapping(self):
        return self.sock.signal_repository.lid_mapping

    async def phone_of(self, jid: str) -> Optional[str]:
        """`<number>@s.whatsapp.net` for a phone-number or LID jid (None if the mapping is not known yet)."""
        if not jid:
            return None
        if is_pn_user(jid):
            return jid_normalized_user(jid)
        if is_lid_user(jid):
            try:
                pn = await self._lid_mapping().get_pn_for_lid(jid)
            except Exception:
                pn = None
            return jid_normalized_user(pn) if pn else None
        return None

    async def lid_of(self, jid: str) -> Optional[str]:
        """`<id>@lid` for a phone-number or LID jid."""
        if not jid:
            return None
        if is_lid_user(jid):
            return jid_normalized_user(jid)
        if is_pn_user(jid):
            try:
                lid = await self._lid_mapping().get_lid_for_pn(jid)
            except Exception:
                lid = None
            return jid_normalized_user(lid) if lid else None
        return None

    async def same_user(self, a: str, b: str) -> bool:
        """True if both jids belong to the same account, whether written as phone number or LID, any device."""
        if not a or not b:
            return False
        na, nb = jid_normalized_user(a), jid_normalized_user(b)
        if na == nb:
            return True
        pa, pb = await self.phone_of(a), await self.phone_of(b)
        return bool(pa and pb and pa == pb)

    @property
    def me(self) -> dict:
        """Your own account: {'id': '<number>:<device>@s.whatsapp.net', 'lid': ..., 'name': ...}"""
        return (self.sock.auth_state['creds'].get('me') or {}) if self.sock else {}

    async def is_owner(self, m: Message) -> bool:
        if m.from_me:
            return True
        num = await m.sender_number()
        return bool(num and num in self.owners)

    # --- waiting for messages -------------------------------------------------------------------------------
    def wait_for(self, *, chat: Optional[str] = None, sender: Optional[str] = None, filter: Optional[Filter] = None,
                 timeout: Optional[float] = None, consume: bool = True) -> Awaitable[Message]:
        """Await the next incoming message matching chat / sender / filter. Registers immediately, so you can
        send a prompt and then await the result without a race. `consume=False` lets the message also reach the
        normal handlers. Raises asyncio.TimeoutError."""
        fut: asyncio.Future = asyncio.get_running_loop().create_future()

        def match(m: Message) -> bool:
            return ((chat is None or m.chat == chat) and (sender is None or m.sender == sender)
                    and (filter is None or filter(m)))
        entry = (match, fut, consume)
        self._waiters.append(entry)

        async def wait():
            try:
                return await asyncio.wait_for(fut, timeout)
            finally:
                if entry in self._waiters:
                    self._waiters.remove(entry)
        return wait()

    def __getattr__(self, name: str):
        # forward everything else (group_metadata, update_profile_status, ...) to the underlying socket
        if name.startswith('_') or name == 'sock':
            raise AttributeError(name)
        if self.sock is None:
            raise AttributeError(f'{name!r}: client not started yet')
        return getattr(self.sock, name)

    # =========================================================================================================
    # plugins
    # =========================================================================================================
    def _snapshot(self) -> dict:
        return {'handlers': {e: {id(f) for f in fs} for e, fs in self._handlers.items()},
                'messages': {id(h) for h in self._message_handlers},
                'commands': {id(c) for c in self._commands.values()},
                'middleware': {id(f) for f in self._middleware}}

    def _added_since(self, before: dict) -> dict:
        """Compute what was registered since `before`."""
        return {'handlers': [(e, f) for e, fs in self._handlers.items() for f in fs
                             if id(f) not in before['handlers'].get(e, set())],
                'messages': [h for h in self._message_handlers if id(h) not in before['messages']],
                'commands': [(k, c) for k, c in self._commands.items() if id(c) not in before['commands']],
                'middleware': [f for f in self._middleware if id(f) not in before['middleware']]}

    def load_plugin(self, plugin: Plugin, name: Optional[str] = None) -> str:
        """Apply a `Plugin` object (see baileys.Plugin)."""
        name = name or plugin.name or f'plugin{len(self._plugins)}'
        before = self._snapshot()
        plugin.apply(self)
        self._plugins[name] = {'added': self._added_since(before), 'source': None}
        return name

    def load(self, target: Union[str, os.PathLike]) -> list[str]:
        """Load plugins from a folder (every *.py), a single file, or a dotted module name. A plugin module either
        defines `plugin = Plugin('name')` or `def setup(client)`. Returns the plugin names loaded."""
        target = os.fspath(target)
        if os.path.isdir(target):
            files = sorted(os.path.join(target, f) for f in os.listdir(target)
                           if f.endswith('.py') and not f.startswith('_'))
            return [n for f in files for n in self.load(f)]
        if os.path.isfile(target):
            stem = os.path.splitext(os.path.basename(target))[0]
            # exec the source directly: bypasses the .pyc cache, so reload() always sees the latest edit
            module = types.ModuleType(f'baileys_plugin_{stem}')
            module.__file__ = target
            with open(target, 'r', encoding='utf-8') as fh:
                exec(compile(fh.read(), target, 'exec'), module.__dict__)
            source = ('file', target)
        else:
            module = importlib.import_module(target)
            stem, source = target.rsplit('.', 1)[-1], ('module', target)
        name = getattr(getattr(module, 'plugin', None), 'name', None) or stem
        if name in self._plugins:
            self.unload(name)
        before = self._snapshot()
        if isinstance(getattr(module, 'plugin', None), Plugin):
            module.plugin.apply(self)
        elif callable(getattr(module, 'setup', None)):
            module.setup(self)
        else:
            raise ValueError(f'{target}: plugin module needs a `plugin = Plugin(...)` or a `setup(client)` function')
        self._plugins[name] = {'added': self._added_since(before), 'source': source}
        return [name]

    def unload(self, name: str) -> None:
        info = self._plugins.pop(name)
        added = info['added']
        for e, f in added['handlers']:
            if f in self._handlers.get(e, []):
                self._handlers[e].remove(f)
        for h in added['messages']:
            if h in self._message_handlers:
                self._message_handlers.remove(h)
        for k, c in added['commands']:
            if self._commands.get(k) is c:
                del self._commands[k]
        for f in added['middleware']:
            if f in self._middleware:
                self._middleware.remove(f)

    def reload(self, name: str) -> list[str]:
        """Re-import a plugin loaded from a file or module and re-register it."""
        source = self._plugins[name]['source']
        if source is None:
            raise ValueError(f'plugin {name!r} was added with load_plugin(); nothing to re-import')
        kind, target = source
        if kind == 'module':
            sys.modules.pop(target, None)
        self.unload(name)
        return self.load(target)

    @property
    def plugins(self) -> list[str]:
        return list(self._plugins)

    # =========================================================================================================
    # lifecycle
    # =========================================================================================================
    async def _emit(self, event: str, *args):
        for fn in list(self._handlers.get(event, [])):
            await self._safe(fn, *args)

    async def _safe(self, fn: Handler, *args):
        try:
            await _call(fn, *args)
        except Exception as e:  # a failing handler must never kill the event loop
            await self._report(e, fn)

    async def _report(self, e: Exception, fn: Handler):
        if self._handlers.get('error') and fn not in self._handlers['error']:
            for h in list(self._handlers['error']):
                try:
                    await _call(h, e, fn)
                except Exception:
                    self.logger.error({'err': repr(e)}, 'error handler failed')
        else:
            self.logger.error({'err': repr(e), 'handler': getattr(fn, '__name__', str(fn))}, 'handler failed')

    async def _load_auth(self):
        a = self.auth
        if isinstance(a, tuple):
            return a
        if callable(a):
            r = a()
            return await r if inspect.isawaitable(r) else r
        if isinstance(a, (str, os.PathLike)) and os.fspath(a).endswith(('.db', '.sqlite', '.sqlite3')):
            from .storage import use_sqlite_auth_state
            return await use_sqlite_auth_state(os.fspath(a))
        return await use_multi_file_auth_state(os.fspath(a))

    async def _connect(self):
        state, save_creds = await self._load_auth()
        self._save_creds = save_creds
        cfg = dict(self.config)
        if self.fetch_version and 'version' not in cfg:
            cfg['version'] = (await fetch_latest_wa_web_version())['version']
        cfg.setdefault('logger', self.logger)
        cfg['auth'] = {'creds': state['creds'], 'keys': make_cacheable_signal_key_store(state['keys'], self.logger)}
        cfg.setdefault('getMessage', lambda key: asyncio.sleep(0, None))
        self.sock = make_wa_socket({**DEFAULT_CONNECTION_CONFIG, **cfg})
        self._closed = asyncio.get_running_loop().create_future()

        async def on_events(events: dict):
            if 'creds.update' in events:
                await save_creds()
            if 'connection.update' in events:
                await self._on_connection(events['connection.update'])
            self._track(events)
            for name, data in events.items():
                await self._emit(name, data)
            if 'messages.upsert' in events:
                await self._on_upsert(events['messages.upsert'])

        self._unsub = self.sock.ev.process(on_events)

    def _track(self, events: dict):
        """Keep the in-memory caches (groups, contacts, chats) fresh."""
        for g in _as_list(events.get('groups.update')):
            self._groups.pop(g.get('id'), None)
        for g in _as_list(events.get('groups.upsert')):
            self._groups.pop(g.get('id'), None)
        if 'group-participants.update' in events:
            self._groups.pop(events['group-participants.update'].get('id'), None)
        for ev, store in (('contacts.upsert', self.contacts), ('contacts.update', self.contacts),
                          ('chats.upsert', self.chats), ('chats.update', self.chats)):
            for c in _as_list(events.get(ev)):
                if c.get('id'):
                    store.setdefault(c['id'], {}).update({k: v for k, v in c.items() if v is not None})
        for jid in _as_list(events.get('chats.delete')):
            self.chats.pop(jid, None)

    async def _on_connection(self, update: dict):
        qr, connection = update.get('qr'), update.get('connection')
        if qr:
            if self.pairing_phone and not self.sock.auth_state['creds'].get('registered'):
                if not self._pairing_requested:          # the QR refreshes every ~20s: ask for ONE code per connection
                    self._pairing_requested = True
                    phone = ''.join(ch for ch in str(self.pairing_phone) if ch.isdigit())
                    code = await self.sock.request_pairing_code(phone)
                    code = f'{code[:4]}-{code[4:]}'
                    if self._handlers.get('pairing_code'):
                        await self._emit('pairing_code', code)
                    else:
                        print(f'\nPairing code: {code}\n'
                              'WhatsApp > Linked devices > Link a device > Link with phone number instead\n')
            elif self._handlers.get('qr'):
                await self._emit('qr', qr)
            elif self.print_qr:
                _print_qr(qr)
        if connection == 'open':
            if self._ready is not None:
                self._ready.set()
            await self._emit('ready')
        if connection == 'close':
            self._pairing_requested = False
            err = (update.get('lastDisconnect') or {}).get('error')
            code = getattr(err, 'output', {}).get('statusCode') if err is not None else None
            try:
                reason = DisconnectReason(code) if code is not None else None
            except ValueError:
                reason = code
            await self._emit('close', reason)
            if self._ready is not None:
                self._ready.clear()
            self._logged_out = code == DisconnectReason.loggedOut
            if self._closed and not self._closed.done():
                self._closed.set_result(code)

    # --- incoming messages ----------------------------------------------------------------------------------
    async def _on_upsert(self, upsert: dict):
        if self.only_notify and upsert.get('type') != 'notify':
            return
        for raw in upsert.get('messages', []):
            m = Message(self, raw)
            if not m.content or 'protocolMessage' in m.content or 'senderKeyDistributionMessage' in m.content \
                    and len(m.content) == 1:
                continue
            if self.auto_read and not m.from_me:
                await self._safe(lambda: m.read())
            if self._consume(m):
                continue
            await self._run_middleware(m)

    def _consume(self, m: Message) -> bool:
        """Hand the message to a pending wait_for/ask. True if it was consumed."""
        for entry in list(self._waiters):
            match, fut, consume = entry
            if not fut.done() and match(m):
                fut.set_result(m)
                self._waiters.remove(entry)
                if consume:
                    return True
        return False

    async def _run_middleware(self, m: Message):
        chain = list(self._middleware)

        async def call(i: int):
            if i >= len(chain):
                await self._dispatch(m)
                return
            done = False

            async def call_next():
                nonlocal done
                if done:
                    return
                done = True
                await call(i + 1)
            try:
                await _call(chain[i], m, call_next)
            except Exception as e:
                await self._report(e, chain[i])
        await call(0)

    async def _dispatch(self, m: Message):
        for flt, skip_me, fn in list(self._message_handlers):
            if (skip_me and m.from_me) or (flt is not None and not flt(m)):
                continue
            await self._safe(fn, m)
        await self._dispatch_command(m)

    async def _dispatch_command(self, m: Message):
        text = (m.text or '').strip()
        prefix = next((p for p in self.prefixes if text.startswith(p)), None)
        if prefix is None or not text[len(prefix):]:
            return
        body = text[len(prefix):]
        try:
            parts = shlex.split(body)
        except ValueError:
            parts = body.split()
        if not parts:
            return
        cmd = self._commands.get(parts[0].lower())
        if cmd is None or (cmd.skip_me and m.from_me) or (cmd.filter is not None and not cmd.filter(m)):
            return
        m.command, m.args = cmd.name, parts[1:]
        rest = body.split(None, 1)
        m.arg_text = rest[1].strip() if len(rest) > 1 else ''
        reason = await self._check_command(cmd, m)
        if reason:
            await self._deny(m, cmd, reason)
            return
        await self._safe(cmd.fn, *((m, m.args) if cmd.n_params >= 2 else (m,)))

    async def _check_command(self, cmd: Command, m: Message) -> Optional[Union[str, tuple]]:
        if cmd.owner_only and not await self.is_owner(m):
            return 'owner_only'
        if cmd.group_only and not m.is_group:
            return 'group_only'
        if cmd.private_only and m.is_group:
            return 'private_only'
        if cmd.admin_only or cmd.bot_admin:
            g = await m.group()
            if cmd.admin_only and not (m.from_me or await self.is_owner(m) or (g and g.is_admin(m.sender))):
                return 'admin_only'
            if cmd.bot_admin and not (g and g.bot_is_admin):
                return 'bot_admin'
        left = cmd.check_cooldown(m.sender)
        if left:
            return ('cooldown', left)
        return None

    async def _deny(self, m: Message, cmd: Command, reason: Union[str, tuple]):
        seconds = None
        if isinstance(reason, tuple):
            reason, seconds = reason
        if self._handlers.get('denied'):
            for h in list(self._handlers['denied']):
                await self._safe(h, m, reason, cmd)
            return
        if self.denied_messages:
            text = self.denied_messages.get(reason)
            if text:
                await self._safe(lambda: m.reply(text.format(seconds=int(seconds + 0.999) if seconds else 0)))

    def _register_builtins(self):
        if self.help_command and 'help' not in self._commands:
            self.command('help', help='show this list')(self._builtin_help)

    async def _builtin_help(self, m: Message):
        seen, lines = set(), []
        for cmd in self._commands.values():
            if id(cmd) in seen or cmd.hidden or not cmd.help or cmd.name == 'help':
                continue
            seen.add(id(cmd))
            lines.append(f'{self.prefixes[0]}{cmd.name} - {cmd.help}')
        await m.reply('Commands:\n' + '\n'.join(sorted(lines)) if lines else 'No commands available.')

    # --- run ------------------------------------------------------------------------------------------------
    async def start(self):
        """Connect and return (does not block). Use `await client.wait_closed()` / `client.run()` to keep alive."""
        self._ready = asyncio.Event()
        self._stopping = False
        self._logged_out = False
        self._register_builtins()
        await self._connect()

    async def wait_ready(self, timeout: Optional[float] = None):
        await asyncio.wait_for(self._ready.wait(), timeout)

    async def stop(self):
        self._stopping = True
        if self._unsub:
            self._unsub()
        if self.sock:
            await self.sock.end(None)

    async def logout(self):
        await self.sock.logout()

    async def run_forever(self):
        """Connect and keep reconnecting until stopped or logged out."""
        await self.start()
        while True:
            code = await self._closed
            if self._stopping or self._logged_out or not self.auto_reconnect:
                if self._logged_out:
                    self.logger.error('Connection closed. You are logged out. Delete the auth folder to re-pair.')
                return
            if self._unsub:
                self._unsub()
            self.logger.info({'code': code}, 'reconnecting')
            await asyncio.sleep(0 if code == DisconnectReason.restartRequired else 2)
            await self._connect()

    def run(self):
        """Blocking entry point: `client.run()`. Ctrl+C to stop."""
        async def main():
            loop = asyncio.get_running_loop()
            task = asyncio.ensure_future(self.run_forever())
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, task.cancel)
                except (NotImplementedError, RuntimeError):
                    pass
            try:
                await task
            except asyncio.CancelledError:
                await self.stop()
        asyncio.run(main())

    async def __aenter__(self) -> 'Client':
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()


def _as_list(x) -> list:
    if x is None:
        return []
    return list(x) if isinstance(x, (list, tuple, set)) else [x]


async def _call(fn: Callable, *args):
    r = fn(*args)
    if inspect.isawaitable(r):
        r = await r
    return r


def _print_qr(qr: str):
    try:
        import qrcode
        q = qrcode.QRCode(border=1)
        q.add_data(qr)
        q.print_ascii(invert=True)
    except ImportError:
        print('Scan this QR (pip install qrcode to render it):', qr)
