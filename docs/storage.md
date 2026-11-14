# Storage backends

`Client(auth=...)` accepts:

| value | backend |
|---|---|
| `'folder'` | multi-file JSON (default; fine for one bot) |
| `'bot.db'` / `'.sqlite'` | SQLite, one file, atomic writes |
| `(state, save_creds)` | anything returned by the helpers below |
| `async def factory(): return state, save` | lazy creation |

```python
from baileys import use_sqlite_auth_state, use_redis_auth_state, use_custom_auth_state

state, save = await use_sqlite_auth_state('bot.db')
state, save = await use_redis_auth_state(redis.asyncio.Redis(), prefix='sales')   # pip install baileys[redis]
```

Your own database: implement two coroutines and wrap them:

```python
class MyBackend:
    async def get_many(self, keys): ...          # -> {key: json string | None}
    async def set_many(self, items): ...         # {key: json string | None}; None deletes; make it atomic if you can

state, save = await use_custom_auth_state(MyBackend())
```
