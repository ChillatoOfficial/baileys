"""Plugins: split a bot into files, and share handlers between several accounts.

    # plugins/admin.py
    from baileys import Plugin
    plugin = Plugin('admin')

    @plugin.command('kick', admin_only=True, group_only=True)
    async def kick(m, args): ...

    # main.py
    client.load('plugins')            # a folder, a file or a dotted module name
    client.reload('admin')            # hot reload one plugin
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

# decorators used as `@x.name(...)` (return a decorator) vs `@x.name` (take the function directly)
FACTORY = {'on', 'message', 'command'}
DIRECT_PREFIX = 'on_'
DIRECT = {'use'}


def is_decorator(name: str) -> bool:
    return name in FACTORY or name in DIRECT or name.startswith(DIRECT_PREFIX)


class Plugin:
    """Records decorator calls so they can be applied to any `Client` later (`client.load_plugin(plugin)`)."""

    def __init__(self, name: str | None = None):
        self.name = name
        self.calls: list[tuple[str, tuple, dict, Callable]] = []

    def __getattr__(self, attr: str):
        if attr.startswith('_') or not is_decorator(attr):
            raise AttributeError(attr)

        def call(*args, **kwargs):
            if attr in FACTORY:
                def deco(fn):
                    self.calls.append((attr, args, kwargs, fn))
                    return fn
                return deco
            fn = args[0]
            self.calls.append((attr, (), {}, fn))
            return fn
        return call

    def apply(self, client) -> None:
        for name, args, kwargs, fn in self.calls:
            if name in FACTORY:
                getattr(client, name)(*args, **kwargs)(fn)
            else:
                getattr(client, name)(fn)


class ClientPool:
    """Run several accounts in one process with the same handlers.

        pool = ClientPool({'sales': 'auth_sales', 'support': 'auth_support'}, prefix='!')

        @pool.command('ping')
        async def ping(m): await m.reply(f'pong from {m.client.name}')

        pool.run()
    """

    def __init__(self, accounts: dict, **client_kwargs: Any):
        from .client import Client
        self.clients: dict[str, Client] = {}
        for name, auth in accounts.items():
            c = Client(auth, **client_kwargs)
            c.name = name
            self.clients[name] = c

    def __getitem__(self, name: str):
        return self.clients[name]

    def __iter__(self):
        return iter(self.clients.values())

    def __getattr__(self, attr: str):
        if attr.startswith('_') or not is_decorator(attr):
            raise AttributeError(attr)

        def call(*args, **kwargs):
            if attr in FACTORY:
                decos = [getattr(c, attr)(*args, **kwargs) for c in self.clients.values()]

                def deco(fn):
                    for d in decos:
                        d(fn)
                    return fn
                return deco
            for c in self.clients.values():
                getattr(c, attr)(args[0])
            return args[0]
        return call

    def load(self, target: str) -> None:
        for c in self.clients.values():
            c.load(target)

    async def run_forever(self):
        await asyncio.gather(*(c.run_forever() for c in self.clients.values()))

    def run(self):
        import signal

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
                await asyncio.gather(*(c.stop() for c in self.clients.values()), return_exceptions=True)
        asyncio.run(main())
