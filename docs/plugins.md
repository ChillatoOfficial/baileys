# Plugins and multiple accounts

```python
# plugins/admin.py
from baileys import Plugin
plugin = Plugin('admin')

@plugin.command('kick', admin_only=True, bot_admin=True)
async def kick(m, args): ...

@plugin.on_group_join
async def welcome(e): await e.send('welcome')
```

or without the `Plugin` object:

```python
def setup(client):
    @client.command('dice')
    async def dice(m): ...
```

```python
client.load('plugins')           # folder (every *.py not starting with _), a file, or 'package.module'
client.reload('admin')           # re-reads the file from disk, hot swap
client.unload('admin')
client.plugins                   # ['admin', ...]
```

## Several accounts

```python
pool = ClientPool({'sales': 'auth_sales', 'support': 'auth_support.db'}, prefix='!', owners=['39...'])

@pool.command('ping')
async def ping(m): await m.reply(f'pong from {m.client.name}')

pool.load('plugins')
pool.run()
```

Each account keeps its own auth, queue and caches; handlers are shared.
