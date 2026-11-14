"""Port of Baileys' Example/example.ts.

    python examples/example.py                  # scan the QR code printed in the terminal
    python examples/example.py --use-pairing-code
    python examples/example.py --do-reply       # reply to incoming "ping" messages
"""
import asyncio
import sys

from baileys import (DisconnectReason, fetch_latest_wa_web_version, make_cacheable_signal_key_store, make_wa_socket,
                     use_multi_file_auth_state, logger)
from baileys.Utils.logger import ILogger

do_replies = '--do-reply' in sys.argv
use_pairing_code = '--use-pairing-code' in sys.argv
log = ILogger('example', 'info')


def print_qr(qr: str):
    try:
        import qrcode  # optional: pip install qrcode
        q = qrcode.QRCode(border=1)
        q.add_data(qr)
        q.print_ascii(invert=True)
    except ImportError:
        print('QR (install `qrcode` to render it):', qr)


async def start_sock():
    state, save_creds = await use_multi_file_auth_state('baileys_auth_info')
    info = await fetch_latest_wa_web_version()  # fetch latest version of WA Web
    log.info({'version': '.'.join(map(str, info['version'])), 'isLatest': info['isLatest']}, 'using WA version')

    sock = make_wa_socket({
        'version': info['version'],
        'logger': log,
        # caching makes the store faster to send/recv messages
        'auth': {'creds': state['creds'], 'keys': make_cacheable_signal_key_store(state['keys'], log)},
        'generateHighQualityLinkPreview': True,
        'getMessage': lambda key: asyncio.sleep(0, None),  # implement to handle retries & poll updates
    })
    done = asyncio.get_running_loop().create_future()

    async def on_events(events: dict):
        if 'connection.update' in events:
            update = events['connection.update']
            connection, last_disconnect, qr = update.get('connection'), update.get('lastDisconnect'), update.get('qr')
            if connection == 'close':
                err = (last_disconnect or {}).get('error')
                code = getattr(err, 'output', {}).get('statusCode') if err is not None else None
                if code != DisconnectReason.loggedOut:  # reconnect if not logged out
                    asyncio.ensure_future(start_sock())
                else:
                    log.error('Connection closed. You are logged out.')
                if not done.done():
                    done.set_result(None)
            if qr:
                if use_pairing_code and not sock.auth_state['creds'].get('registered'):
                    phone = await asyncio.get_running_loop().run_in_executor(None, input,
                                                                            'Please enter your phone number:\n')
                    print('Pairing code:', await sock.request_pairing_code(phone))
                else:
                    print_qr(qr)
            log.debug(update, 'connection update')

        if 'creds.update' in events:  # credentials updated -- save them
            await save_creds()

        if 'messages.upsert' in events:
            upsert = events['messages.upsert']
            if upsert['type'] == 'notify':
                for msg in upsert['messages']:
                    content = msg.get('message') or {}
                    text = content.get('conversation') or (content.get('extendedTextMessage') or {}).get('text')
                    if text and do_replies and not msg['key'].get('fromMe') and text.strip().lower() == 'ping':
                        await sock.read_messages([msg['key']])
                        await sock.send_message(msg['key']['remoteJid'], {'text': 'pong'}, {'quoted': msg})

    sock.ev.process(on_events)
    await done


if __name__ == '__main__':
    asyncio.run(start_sock())
