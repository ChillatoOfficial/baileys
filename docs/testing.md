# Testing

```bash
pip install -e ".[dev]"
pytest -q
```

What the suite covers without any account: the Signal implementation (including interop with the original
`libsignal-node` when `LIBSIGNAL_NODE_DIR` is set, see README), media encryption, app-state patches, the binary
codec, the handshake payloads, an in-process WhatsApp "server" relaying messages between two sockets, and the whole
high level layer (commands, permissions, middleware, conversations, plugins, queue, storage backends, two real
`Client`s talking through the fake server).

## Against real WhatsApp

Pair once (`python examples/simple_bot.py`, scan the QR), then:

```bash
BAILEYS_LIVE_AUTH=./baileys_auth_info pytest tests/test_live.py -s
```

It connects, sends a unique text to **your own number**, waits for it to arrive, lists groups, deletes the message.
Set `BAILEYS_LIVE_TO` to target another chat. Use a throwaway number if you can.
