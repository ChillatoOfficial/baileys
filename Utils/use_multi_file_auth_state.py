from __future__ import annotations
import asyncio
import os
from .auth_utils import init_auth_creds
from .generics import BufferJSON

_file_locks: dict[str, asyncio.Lock] = {}


def _get_file_lock(path: str) -> asyncio.Lock:
    return _file_locks.setdefault(path, asyncio.Lock())


async def use_multi_file_auth_state(folder: str):
    """Stores the full authentication state in a single folder.
    Not meant for production use other than perhaps a bot; implement your own SQL / NoSQL state instead."""
    fix_file_name = lambda f: f.replace('/', '__').replace(':', '-') if f else f

    async def write_data(data, file: str):
        path = os.path.join(folder, fix_file_name(file))
        async with _get_file_lock(path):
            content = BufferJSON.dumps(_to_plain(data))
            await asyncio.get_running_loop().run_in_executor(None, _write, path, content)

    async def read_data(file: str):
        try:
            path = os.path.join(folder, fix_file_name(file))
            async with _get_file_lock(path):
                content = await asyncio.get_running_loop().run_in_executor(None, _read, path)
            return BufferJSON.loads(content)
        except Exception:
            return None

    async def remove_data(file: str):
        try:
            path = os.path.join(folder, fix_file_name(file))
            async with _get_file_lock(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        except Exception:
            pass

    if os.path.exists(folder):
        if not os.path.isdir(folder):
            raise RuntimeError(f'found something that is not a directory at {folder}, either delete it or '
                               'specify a different location')
    else:
        os.makedirs(folder, exist_ok=True)

    creds = await read_data('creds.json') or init_auth_creds()

    class _Keys:
        async def get(self, type_, ids):
            data = {}

            async def one(i):
                value = await read_data(f'{type_}-{i}.json')
                data[i] = value
            await asyncio.gather(*[one(i) for i in ids])
            return data

        async def set(self, data):
            tasks = []
            for category, entries in data.items():
                for i, value in entries.items():
                    f = f'{category}-{i}.json'
                    tasks.append(write_data(value, f) if value else remove_data(f))
            await asyncio.gather(*tasks)

    state = {'creds': creds, 'keys': _Keys()}

    async def save_creds():
        return await write_data(creds, 'creds.json')

    return state, save_creds


def _write(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


def _read(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def _to_plain(obj):
    from google.protobuf.message import Message
    from .proto_utils import to_dict
    if isinstance(obj, Message):
        return _to_plain(to_dict(obj))
    if isinstance(obj, dict):
        return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_plain(v) for v in obj]
    return obj
