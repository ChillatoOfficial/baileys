# Commands, permissions and middleware

```python
client = Client('auth', prefix=('/', '!'), owners=['391234567890'])

@client.command('ping', 'p', help='pong', cooldown=3)
async def ping(m):                 # handler(m)  or  handler(m, args)
    await m.reply('pong')
```

Inside a command handler: `m.command` (canonical name), `m.args` (shell-style split, quotes respected) and
`m.arg_text` (the raw text after the command).

| option | meaning |
|---|---|
| `help='...'` | listed by the built-in `/help` (define your own `help` command to replace it, or `Client(help_command=False)`) |
| `hidden=True` | never listed |
| `group_only` / `private_only` | where the command works |
| `admin_only` | sender must be a group admin (bot owner and your own account also pass); implies `group_only` |
| `bot_admin` | the bot must be admin of the group; implies `group_only` |
| `owner_only` | sender is your own account or one of `Client(owners=[...])` (phone numbers, digits only; LIDs are resolved) |
| `cooldown=seconds` | per user and command |
| `filter=filters.xxx` | extra condition; when it fails the command is ignored silently |
| `from_me=True` | also react to messages sent by the bot's own account |

When a command is refused the bot replies with a short text. Change it with `Client(denied_messages={'cooldown':
'wait {seconds}s'})`, silence it with `denied_messages=False`, or take over with a hook:

```python
@client.on_denied
async def denied(m, reason, command):     # reason: admin_only | bot_admin | group_only | private_only | owner_only | cooldown
    await m.react('⛔')
```

## Message handlers

```python
@client.message(filters.group & filters.regex(r'\bhello\b') & ~filters.from_me)
async def hello(m): ...
```

Filters: `text`, `from_me`, `group`, `private`, `media`, `quoted`, `media_type('image', 'video')`, `regex(...)`,
`equals(...)`, `chat(...)`, `sender(...)`, combined with `&`, `|`, `~`.

## Middleware

```python
@client.use
async def log_and_ban(m, call_next):
    if m.sender in BANNED:
        return                          # stop: nothing else runs for this message
    m.state['t0'] = time.monotonic()    # share data with handlers
    await call_next()
```

Middleware runs in registration order around handlers and commands. Exceptions anywhere are logged (or passed to
`@client.on_error`) and never stop the connection.
