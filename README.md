<div align="center">

# baileys
<img src="https://media1.tenor.com/m/2Yar1sBkiXUAAAAC/tokyo-ghoul-anime.gif" alt="Header Image" width="30%"/>

**Build WhatsApp bots in Python with decorators.**
A full Python port of [Baileys](https://github.com/WhiskeySockets/Baileys) (v7.0.0-rc14) with a friendly high-level API on top.

[Quick start](#-quick-start) · [Features](#-features) · [Examples](#-examples) · [Docs](DOCS.md) · [FAQ](#-faq)

</div>

> ⚠️ **Unofficial.** Not affiliated with WhatsApp or Meta. Automating WhatsApp can get an account banned, so use it at your own risk and never spam.

---

## ✨ Why this library

- **Pythonic.** `@client.command`, `@client.on_group_join`, `await m.reply(...)`. No raw dictionaries and no giant event switch.
- **No browser, no Selenium.** It talks to WhatsApp directly over WebSocket, like WhatsApp Web, and runs on `asyncio`.
- **Batteries included.** Commands, cooldowns, admin checks, conversations, plugins, a rate-limited send queue, SQLite/Redis sessions.
- **Typed.** Autocomplete for `m.` and `e.` in your editor, plus `TypedDict`s for every payload.
- **Faithful to upstream.** The whole Baileys API is still available underneath (`client.sock`) if you need it.

## 📦 Install

Requires **Python 3.10+**. `ffmpeg` is only needed for video stickers, thumbnails and voice notes.

```bash
pip install git+https://github.com/ChillatoOfficial/baileys.git
```

Pin a version, or add Redis session storage:

```bash
pip install "git+https://github.com/ChillatoOfficial/baileys.git@v7.0.0rc14"
pip install "baileys[redis] @ git+https://github.com/ChillatoOfficial/baileys.git"
```

Update later with `pip install -U git+https://github.com/ChillatoOfficial/baileys.git`.

<details>
<summary>Install for development</summary>

```bash
git clone https://github.com/ChillatoOfficial/baileys.git && cd baileys
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```
</details>

## 🚀 Quick start

Create `bot.py`:

```python
from baileys import Client, Message

client = Client('auth')                      # session is saved in ./auth

@client.command('ping')
async def ping(m: Message):
    await m.reply('pong 🏓')

client.run()
```

```bash
python bot.py
```

Scan the QR with **WhatsApp → Linked devices → Link a device**. The next run reconnects by itself.

> Prefer a code over a QR? `Client('auth', pairing_phone='391234567890')` prints a pairing code to type into the phone. Full example: [examples/pairing_code.py](examples/pairing_code.py).

Send `/ping` to the bot's number from another phone (messages you send from the same account are ignored by default). `/help` is built in.

## 🧩 Features

| | |
|---|---|
| **Commands** | prefix, aliases, `admin_only`, `bot_admin`, `group_only`, `private_only`, `owner_only`, `cooldown`, automatic `/help` |
| **Messages** | `reply`, `react`, `edit`, `delete`, `forward`, `relay_to`, `reply_image / video / audio / document / sticker / location / contact / poll`, `download`, `mention_all` |
| **Stickers** | send any image, GIF or video and it is converted to a proper WebP sticker for you |
| **Conversations** | `await m.ask(...)`, `await m.confirm(...)`, `await m.choose(...)` |
| **Events** | about 30 typed decorators: `on_group_join`, `on_reaction`, `on_call`, `on_message_update`, ... |
| **Filters** | combine with `&`, `\|`, `~`: `filters.group & filters.regex('hello')` |
| **Middleware** | `@client.use` runs around every incoming message |
| **Groups & contacts** | cached `Group` object: `is_admin`, `add`, `remove`, `promote`, `invite_link`, ... |
| **Safe sending** | send queue with per-chat ordering, spacing, jitter and automatic retry |
| **Plugins** | split the bot into files, hot `load / unload / reload`, run several accounts with `ClientPool` |
| **Storage** | session in a folder, SQLite, Redis or your own backend |
| **Low level** | every original Baileys function (`relay_message`, `group_metadata`, app-state, newsletters, ...) via `client.sock` |

## 📚 Examples

### Commands with permissions

```python
@client.command('kick', admin_only=True, bot_admin=True, group_only=True)
async def kick(m: Message, args):
    if not m.mentions:
        return await m.reply('mention who to remove')
    await (await m.group()).remove(*m.mentions)
    await m.react('✅')
```

### Ask questions in a conversation

```python
@client.command('survey')
async def survey(m: Message):
    name = await m.ask('What is your name?')
    color = await m.choose('Favourite color?', ['red', 'green', 'blue'])
    if await m.confirm(f'Save {name.text} / {color}?'):
        await m.reply('saved ✅')
```

### Stickers

```python
@client.command('sticker')
async def sticker(m: Message):
    q = m.quoted or m
    if q.media_type not in ('image', 'video'):
        return await m.reply('send or quote an image')
    await m.reply_sticker(await q.download(), pack='My pack', author='me')
```

### Welcome new members

```python
from baileys import ParticipantsEvent

@client.on_group_join
async def welcome(e: ParticipantsEvent):
    tags = ' '.join('@' + j.split('@')[0] for j in e.jids)
    await e.send({'text': f'welcome {tags}! 👋', 'mentions': e.jids})
```

### Filters and middleware

```python
from baileys import filters

@client.message(filters.group & filters.regex(r'\bciao\b'))
async def greet(m: Message):
    await m.reply('ciao!')

@client.use
async def ignore_banned(m: Message, call_next):
    if m.sender not in BANNED:
        await call_next()
```

### Plugins

```python
# plugins/fun.py
import random

def setup(client):
    @client.command('dice')
    async def dice(m):
        await m.reply(f'🎲 {random.randint(1, 6)}')
```

```python
client.load('plugins')          # load a folder; client.reload('fun') while the bot is running
```

### Relay and raw access

```python
await client.relay(jid, {'conversation': 'hi'})      # ship an already-built message as is
await client.group_metadata(jid)                    # anything else goes to the low-level socket
```

More: [examples/simple_bot.py](examples/simple_bot.py) is a complete bot, [examples/plugins/](examples/plugins) has plugin examples.

## 💡 Tips

- **Autocomplete:** annotate your handlers: `async def f(m: Message)` and `async def f(e: ReactionEvent)`. The editor can't infer it from the decorator.
- **Logs:** the default level is `warn`. Use `Client(..., log_level='info')` or `'debug'` while developing.
- **Several accounts:** `ClientPool({'a': 'auth_a', 'b': 'auth_b'})` applies the same decorators to all of them.
- **Group members in LID groups** can arrive as dictionaries. Use `e.jids` / `e.phones` instead of `e.participants`.

## 📖 Documentation

| | |
|---|---|
| [DOCS.md](DOCS.md) | complete reference: every type, method, event, filter and option |
| [docs/commands.md](docs/commands.md) | commands, permissions, middleware |
| [docs/messages.md](docs/messages.md) | sending, receiving, conversations, media |
| [docs/events.md](docs/events.md) | typed and raw events |
| [docs/groups.md](docs/groups.md) | groups, contacts, LID / phone numbers |
| [docs/storage.md](docs/storage.md) | folder, SQLite, Redis, custom session storage |
| [docs/plugins.md](docs/plugins.md) | plugins, hot reload, multiple accounts |
| [PORTING.md](PORTING.md) | how the Python code maps to the TypeScript original |

## ❓ FAQ

**It says `ModuleNotFoundError: No module named 'baileys'`.**
Install the library in the same virtual environment you use to run the bot: `pip install git+https://github.com/ChillatoOfficial/baileys.git`.

**The QR doesn't show up.**
Make sure the terminal is wide enough to draw it, or use `pairing_phone=` to get a code instead.

**My own messages don't trigger commands.**
That's intentional (`ignore_from_me=True`). Send from another number, or set `ignore_from_me=False` to test with yourself.

**My sticker/media doesn't load.**
Stickers must be WebP: `reply_sticker` converts images, GIFs and videos for you (video needs `ffmpeg`).

**How do I log out or reset the session?**
`await client.logout()`, or delete the auth folder / database.

**Can I use the original JavaScript examples?**
The API is the same with `snake_case` names (the `camelCase` aliases also work), so Baileys tutorials translate almost line by line. See the mapping table in [PORTING.md](PORTING.md#typescript--python-mapping).

## 🧪 Tests

```bash
pip install pytest
pytest tests -q
BAILEYS_LIVE_AUTH=./auth pytest tests/test_live.py -s     # optional: against real WhatsApp
```

## 🔍 Status

The whole Baileys source tree has a Python counterpart. Verified locally: the binary codec, Signal protocol interop with the original `libsignal-node`, app-state patches, media encryption, and end-to-end messaging (1:1 and groups) through an in-process fake server. The connection, QR/pairing and history sync have been seen working on a real account, but the newsletter, community and business features have not been tested against real WhatsApp servers. Please open an issue if something behaves differently from upstream.

## 🙏 Credits

Based on [WhiskeySockets/Baileys](https://github.com/WhiskeySockets/Baileys). All protocol knowledge comes from that project and its contributors. See [LICENSE](LICENSE).
