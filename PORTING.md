# Baileys -> Python port: conventions & progress

Source: https://github.com/WhiskeySockets/Baileys (7.0.0-rc14), cloned at
`/tmp/claude-1000/-home-Chillato-Desktop-progetti-framework-baileyspy/eb5dcaf1-a194-4eb0-9994-2092a8e29758/scratchpad/Baileys` (src/ = TS source)

## Conventions
- Package `baileys/`, mirrors `src/` 1:1 (Utils/, Socket/, Signal/, WABinary/, WAUSync/, WAM/, Types/, Defaults/).
- File names snake_case (`messages-recv.ts` -> `messages_recv.py`), functions snake_case (`getBinaryNodeChild` -> `get_binary_node_child`).
- **Dict keys stay camelCase exactly like JS** (`{'remoteJid': ..., 'fromMe': ...}`). Binary nodes = dict `{'tag','attrs','content'}`.
- Messages (WAMessage), message content (proto.IMessage), keys, chats, contacts, creds, group metadata: **plain dicts**,
  bytes as `bytes`, uint64/Long as `int`. protobuf only at the edges:
  `Utils/proto_utils.py`: `from_object(proto.Message, d)` (== `fromObject`), `to_dict(msg)`, `decode(cls, bytes)`.
  Generated protobuf: `baileys/WAProto/WAProto_pb2.py` (`from baileys.WAProto import WAProto_pb2 as proto`);
  nested enums accessible like JS: `proto.WebMessageInfo.Status.READ`, `proto.Message.ProtocolMessage.EPHEMERAL_SETTING`.
- async/await via asyncio; `Promise.all` -> `asyncio.gather`; Mutex -> `Utils/make_mutex.py`; caches -> `Utils/cache.TTLCache`.
- Event emitter: `ev.on(event, listener)`, `ev.emit(event, data)`; listeners may be sync or async.
- Boom errors: `from baileys.WABinary import Boom` (`Boom(msg, data=..., status_code=...)`, `.output['statusCode']`).
- Key store API: `await keys.get(type, ids) -> dict`, `await keys.set({type: {id: value|None}})`, `await keys.transaction(fn, key)`.
- Signal repository methods are snake_case with kwargs: `decrypt_message(jid=, type=, ciphertext=)`.
- Missing (JS `undefined`) == `None` / missing key.

## Status: complete port of src/ (every TS module has a Python counterpart)
Verified: WABinary roundtrip, Signal protocol interop with the original libsignal-node (X3DH + ratchet + XEdDSA, both
directions: tests/test_interop_node.py, run with LIBSIGNAL_NODE_DIR=<dir with `npm i libsignal` + peer.js>),
app-state patch encode/decode, media encrypt/download roundtrip, and an in-process end-to-end 1:1 + group
send/receive through a fake server (tests/test_e2e_loopback.py).
NOT verified against the real WhatsApp servers (needs a live account): Noise handshake, pairing, history sync,
app-state sync against the server, newsletter/community/business IQs.
Not ported: TS-only things (types, typedoc, eslint), the TS jest suites (replaced by tests/), proto-extract tooling.

## Beyond the 1:1 port

`baileys/client.py`, `message.py`, `commands.py`, `events.py`, `filters.py`, `models.py`, `queue.py`, `storage.py`,
`plugins.py` and `schemas.py` are **not** ports: they are an optional high level layer written for Python (see
`docs/`). Nothing in the 1:1 modules depends on them.


## TypeScript / Python mapping

| TypeScript | Python |
|---|---|
| `src/Socket/*.ts`, `Utils`, `Signal`, `WABinary`, `WAUSync`, `WAM`, `Defaults`, `Types` | `baileys/Socket`, `Utils`, `Signal`, `WABinary`, `WAUSync`, `WAM`, `Defaults`, `Types` (same names, `snake_case.py`) |
| `makeWASocket(config)` | `make_wa_socket(config)` (alias `makeWASocket`) |
| `sock.sendMessage(...)` | `await sock.send_message(...)` (the camelCase name also works) |
| messages, keys, chats, creds, group metadata | plain `dict`s with the same camelCase keys as JS |
| `Buffer` / `Uint8Array` | `bytes` |
| protobufjs `proto.Message...` | `baileys.proto` (generated `WAProto_pb2`); `from_object` / `to_dict` convert at the edges |
| `ev.on('messages.upsert', fn)` | `sock.ev.on('messages.upsert', fn)` (sync or `async`), `sock.ev.process(fn)` for batches |
| `Boom` | `baileys.Boom` (`.output['statusCode']`, `.data`) |
| `Promise` / `async` | `asyncio` |
| `libsignal`, `whatsapp-rust-bridge` | pure Python on top of `cryptography` (`Signal/curve.py`, `Signal/session.py`, `Utils/lt_hash.py`) |

```python
await sock.send_message(jid, {'text': 'hi', 'mentions': [other_jid]})
await sock.send_message(jid, {'image': {'url': './pic.jpg'}, 'caption': 'look'})
await sock.send_message(jid, {'poll': {'name': 'Lunch?', 'values': ['Pizza', 'Sushi'], 'selectableCount': 1}})
data = await download_media_message(msg, 'buffer')
```
