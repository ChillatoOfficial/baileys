# Groups, contacts, LID and phone numbers

```python
g = await m.group()                      # or: await client.group('123@g.us'); await client.groups()
g.subject, g.size, g.description, g.owner
g.admins, g.participants
g.is_admin(m.sender), g.is_superadmin(jid), g.has(jid), g.bot_is_admin

await g.add(jid1, jid2); await g.remove(jid); await g.promote(jid); await g.demote(jid)
await g.rename('New name'); await g.set_description('...'); await g.lock(only_admins_send=True)
await g.invite_link(); await g.revoke_invite(); await g.leave()
await g.send('hello'); await g.mention_all('hey')
```

Metadata is cached (`Client(group_cache_ttl=300)`) and dropped automatically on group events.
`g.is_admin` knows a member by all its identities (LID and phone JID), so it works whichever one WhatsApp used.

## LID <-> phone number

Since Baileys 7, users can appear as `<id>@lid`. Helpers:

```python
await client.phone_of('555@lid')          # '39333...@s.whatsapp.net' or None if unknown yet
await client.lid_of('39333...@s.whatsapp.net')
await client.same_user(a, b)              # any mix of LID / phone, any device
await m.sender_phone(); await m.sender_number()   # '393331234567'
```

## Contacts

`client.contact(jid)` -> `Contact` (`name`, `push_name`, `number`, `picture_url()`), `client.name_of(jid)`.
Filled from the contacts the phone syncs after pairing.
