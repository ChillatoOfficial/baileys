# Messages, media, conversations, sending

## Replying

```python
await m.reply('text')                               # quotes the message
await m.send('text')                                # no quote
await m.reply_image('photo.jpg', 'caption')         # path, URL, bytes or {'stream': ...}
await m.reply_video('clip.mp4', gif=True)
await m.reply_audio('voice.ogg', voice_note=True)
await m.reply_document(open('a.pdf', 'rb').read(), 'a.pdf', 'application/pdf')
await m.reply_sticker('sticker.webp')
await m.reply_location(45.46, 9.19, 'Milano')
await m.reply_contact('Bob', '+39 333 1234567')
await m.reply_poll('Lunch?', ['pizza', 'sushi'])
await m.reply_mentioning('hi @391234567890', ['391234567890@s.whatsapp.net'])
await m.mention_all('meeting now')                  # groups
await m.react('👍')                                 # '' removes the reaction
```

`send()` / `reply()` return the **sent** `Message`:

```python
sent = await m.reply('working...')
await sent.edit('done')
await sent.delete()
```

Other: `m.forward(jid)`, `m.read()`, `m.typing(2)`, `await m.download()` (bytes), `await m.save('dir/')`,
`m.quoted`, `m.mentions`, `m.media_type`, `await m.group()`, `await m.sender_phone()`.

## Conversations

```python
name = await m.ask('Your name?', timeout=30)       # raises asyncio.TimeoutError
ok = await m.confirm('Sure?')                      # True / False / None (timeout or unclear)
color = await m.choose('Color?', ['red', 'blue'])  # numbered menu, returns the option or None
```

`ask` waits for the next message from the same sender in the same chat; that message is **consumed** (normal
handlers do not see it). Use `client.wait_for(chat=..., sender=..., filter=..., timeout=..., consume=False)` for
custom waits.

## Send queue

Every `client.send` / `m.reply` goes through a queue: messages to one chat keep their order, sends are spaced out
with a little random jitter, and transient failures (connection closed, timeout, 503) are retried with backoff.

```python
Client('auth', rate_limit={'per_chat': 1.0, 'global_': 0.3, 'jitter': 0.4, 'retries': 3})
Client('auth', rate_limit=False)            # disable
await client.send_now(jid, 'urgent')        # skip the spacing
await client.broadcast(jids, 'hello all')   # one by one through the queue
```

This reduces the chance of looking like a flood. It does **not** make bulk or unsolicited messaging safe.
