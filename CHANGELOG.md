# Changelog

## 7.0.0rc14 (unreleased port)
- Complete Python port of WhiskeySockets/Baileys 7.0.0-rc14 (same module layout, snake_case names).
- High level API: `Client`, `Message`, `filters`, decorators for commands / messages / typed events.
- Command options (`admin_only`, `bot_admin`, `group_only`, `private_only`, `owner_only`, `cooldown`), middleware,
  conversations (`m.ask/confirm/choose`, `client.wait_for`), built-in `/help`.
- `Group` / `Contact` objects with cached metadata, LID <-> phone helpers (`phone_of`, `lid_of`, `same_user`).
- Send queue with per-chat ordering, spacing, jitter and retry.
- Auth backends: multi-file, SQLite, Redis, custom (`use_custom_auth_state`).
- Plugins (`Plugin`, `client.load/unload/reload`) and `ClientPool` for several accounts.
- TypedDict schemas and `py.typed`.
