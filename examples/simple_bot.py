from baileys import CallEvent, Client, Message, ReactionEvent, filters

# auth: a folder, or 'bot.db' for SQLite. owners: numbers allowed to run owner_only commands.
client = Client('baileys_auth_info', owners=['391234567890'])
client.load('examples/plugins')                   # commands from plugins/admin.py and plugins/fun.py


@client.on_ready
async def ready():
    print('connected as', client.me.get('name'))


@client.use                                       # middleware: runs around every incoming message
async def ignore_banned(m: Message, call_next):
    if m.sender in {'0@s.whatsapp.net'}:
        return
    await call_next()


@client.command('ping', help='pong with a reaction', cooldown=3)
async def ping(m: Message):
    await m.react('🏓')
    sent = await m.reply('pong')
    await sent.edit('pong ✔')                     # send() returns the sent Message: edit / delete it


@client.command('say', help='repeat text', group_only=False)
async def say(m: Message, args: list[str]):                           # /say hello world   (m.arg_text == 'hello world')
    await m.reply(m.arg_text or 'say what?')


@client.command('info', help='group info', group_only=True)
async def info(m: Message):
    g = await m.group()                           # cached Group object
    await m.reply(f'{g.subject}: {g.size} members, {len(g.admins)} admins, you are '
                  f'{"" if g.is_admin(m.sender) else "not "}admin')


@client.command('sticker', help='reply to an image with /sticker')
async def sticker(m: Message):
    q = m.quoted or m
    if q.media_type != 'image':
        return await m.reply('send or quote an image')
    await m.reply_sticker(await q.download())


@client.message(filters.group & filters.regex(r'\bciao\b'))
async def greet(m: Message):
    await m.reply('ciao!')


@client.on_reaction
async def reacted(e: ReactionEvent):
    print(f'{e.reactor} reacted {e.emoji} to {e.message_id} in {e.chat}')

@client.on_call
async def call(e: CallEvent):
    if e.status == 'offer':
        await client.reject_call(e.id, e.from_)   # anything not defined on Client goes to the socket


client.run()
