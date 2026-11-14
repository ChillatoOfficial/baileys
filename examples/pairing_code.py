"""Log in with a pairing code instead of scanning a QR.

    python examples/pairing_code.py 391234567890

The number is the phone you want to link, with country code and digits only (no +, spaces or dashes).
On that phone: WhatsApp > Linked devices > Link a device > "Link with phone number instead", then type the code.
"""
import sys

from baileys import Client, Message

if len(sys.argv) < 2:
    sys.exit('usage: python examples/pairing_code.py <phone number with country code, e.g. 391234567890>')

phone = sys.argv[1]
client = Client('auth_pairing', pairing_phone=phone)     # the code is only requested while the session is not linked


@client.on_pairing_code                                   # optional: without it the code is printed for you
async def show_code(code: str):
    print(f'\n  Your pairing code: {code}\n')


@client.on_ready
async def ready():
    print('linked and connected as', client.me.get('name'))


@client.command('ping')
async def ping(m: Message):
    await m.reply('pong 🏓')


client.run()
