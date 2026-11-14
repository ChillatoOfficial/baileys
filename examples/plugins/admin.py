from baileys import Message, ParticipantsEvent, Plugin

plugin = Plugin('admin')


@plugin.command('kick', help='remove the mentioned members', admin_only=True, bot_admin=True)
async def kick(m: Message, args: list[str]):
    group = await m.group()
    if not m.mentions:
        return await m.reply('mention who to remove')
    await group.remove(*m.mentions)
    await m.react('✅')


@plugin.command('everyone', help='mention all members', admin_only=True, cooldown=60)
async def everyone(m: Message):
    await m.mention_all(m.arg_text or 'attention!')


@plugin.on_group_join
async def welcome(e: ParticipantsEvent):
    tags = ' '.join('@' + p.split('@')[0] for p in e.jids)
    await e.send({'text': f'welcome {tags}! 👋', 'mentions': list(e.jids)})
