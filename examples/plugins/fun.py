from baileys import Message
import random


def setup(client):
    @client.command('dice', help='roll a die', cooldown=2)
    async def dice(m: Message):
        await m.reply(f'🎲 {random.randint(1, 6)}')

    @client.command('survey', help='ask a few questions in a conversation')
    async def survey(m: Message):
        name = await m.ask('What is your name?')
        color = await m.choose('Favourite color?', ['red', 'green', 'blue'])
        if await m.confirm(f'Save {name.text} / {color}?'):
            await m.reply('saved ✅')
        else:
            await m.reply('cancelled')
