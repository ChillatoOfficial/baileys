# Events

Typed decorators call your handler once per affected item with an `Event` (a dict with attribute access; `e.raw` is
the original payload, `e.client` the client, and `await e.send(...)` writes to `e.chat`).

| decorator | fields |
|---|---|
| `on_ready`, `on_close(reason)`, `on_qr(qr)`, `on_error(err, handler)`, `on_denied(m, reason, cmd)` | |
| `on_connection` | `connection`, `qr`, `last_disconnect` |
| `on_history` | `chats`, `contacts`, `messages`, `is_latest`, `sync_type` |
| `on_message_update` | `id`, `chat`, `status` (`sent/delivered/read/played`), `edited`, `message` |
| `on_message_delete` | `id`, `chat`, `key`, or `all=True` |
| `on_reaction` | `emoji`, `removed`, `reactor`, `chat`, `message_id` |
| `on_receipt` | `id`, `chat`, `user`, `status` |
| `on_presence` | `chat`, `jid`, `presence`, `last_seen` |
| `on_group_join/leave/promote/demote`, `on_participants` | `chat`, `participants`, `action`, `author` |
| `on_group_upsert`, `on_group_update`, `on_join_request` | group fields |
| `on_chat_upsert/update/delete`, `on_contact_upsert/update` | chat / contact fields |
| `on_call` | `id`, `from_`, `status`, `is_video` |
| `on_blocklist`, `on_label`, `on_label_association`, `on_settings_update`, `on_lid_mapping`, `on_newsletter_*` | |

Raw events (the Baileys names) still work: `@client.on('messages.upsert')`, `@client.on('creds.update')`, ...

`client.contacts` and `client.chats` are dicts kept up to date from the sync events.
