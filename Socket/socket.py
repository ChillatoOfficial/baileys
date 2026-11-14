from __future__ import annotations
import asyncio
import base64
import json
import os
import time
from datetime import datetime
from typing import Callable, Optional
from urllib.parse import urlparse

from ..Defaults import (DEF_CALLBACK_PREFIX, DEF_TAG_PREFIX, INITIAL_PREKEY_COUNT, MIN_PREKEY_COUNT, NOISE_WA_HEADER,
                        PROCESSABLE_HISTORY_TYPES, TimeMs, UPLOAD_TIMEOUT)
from ..Types import DisconnectReason, QueryIds, ReachoutTimelockEnforcementType, XWAPaths
from ..Utils.auth_utils import add_transaction_capability
from ..Utils.companion_reg_client_utils import build_pairing_qr_data, get_companion_platform_id
from ..Utils.crypto import Curve, aes_encrypt_ctr, derive_pairing_code_key, signed_key_pair
from ..Utils.event_buffer import make_event_buffer
from ..Utils.generics import (bind_wait_for_connection_update, bytes_to_crockford, generate_md_tag_prefix,
                              get_code_from_ws_error, get_error_code_from_stream_error, promise_timeout)
from ..Utils.namespace import Namespace
from ..Utils.noise_handler import make_noise_handler
from ..Utils.signal import get_next_pre_keys_node, xmpp_signed_pre_key
from ..Utils.validate_connection import (configure_successful_pairing, generate_login_node,
                                         generate_registration_node)
from ..WABinary import (Boom, assert_node_error_free, binary_node_to_string, encode_binary_node,
                        get_all_binary_node_children, get_binary_node_child, get_binary_node_children, is_lid_user,
                        jid_decode, jid_encode, S_WHATSAPP_NET)
from ..WAM.BinaryInfo import BinaryInfo
from ..WAProto import WAProto_pb2 as proto
from ..WAUSync import USyncQuery, USyncUser
from .Client import WebSocketClient
from .mex import execute_w_mex_query


def _map_web_socket_error(handler: Callable):
    """map the websocket error to the right type so it can be retried by the caller"""
    def inner(error=None, *_):
        if isinstance(error, int):  # (code, reason) close events
            return
        handler(Boom(f'WebSocket Error ({error})', status_code=get_code_from_ws_error(error), data=error))
    return inner


def make_socket(config: dict) -> Namespace:
    """Connects to WA servers and performs:
    - simple queries (no retry mechanism, wait for connection establishment)
    - listen to messages and emit events
    - query phone connection
    Must be called from within a running asyncio loop."""
    wa_web_socket_url = config['waWebSocketUrl']
    connect_timeout_ms = config['connectTimeoutMs']
    logger = config['logger']
    keep_alive_interval_ms = config['keepAliveIntervalMs']
    browser = config['browser']
    auth_state = config['auth']
    default_query_timeout_ms = config.get('defaultQueryTimeoutMs')
    transaction_opts = config['transactionOpts']
    qr_timeout = config.get('qrTimeout')
    make_signal_repository = config['makeSignalRepository']

    public_wam_buffer = BinaryInfo()
    state = {'serverTimeOffsetMs': 0, 'epoch': 1, 'lastDateRecv': None, 'closed': False,
             'keepAliveTask': None, 'qrTimer': None, 'uploadPreKeysPromise': None, 'didStartBuffer': False}

    uq_tag_id = generate_md_tag_prefix()

    def generate_message_tag() -> str:
        tag = f"{uq_tag_id}{state['epoch']}"
        state['epoch'] += 1
        return tag

    if config.get('printQRInTerminal'):
        logger.warn({}, '⚠️ The printQRInTerminal option has been deprecated. You will no longer receive QR codes in '
                        'the terminal automatically. Please listen to the connection.update event yourself and '
                        'handle the QR your way.')
    if 'android' in browser[1].lower():
        logger.warn('⚠️ Using the Android browser is experimental and may lead to unexpected behavior. '
                    'Use at your own risk.')

    class _Hist:
        pass
    sync_disabled = len([x for x in (config['shouldSyncHistoryMessage'](_hist_msg(t))
                                     for t in PROCESSABLE_HISTORY_TYPES) if x is False]) == len(PROCESSABLE_HISTORY_TYPES)
    if sync_disabled:
        logger.warn('⚠️ DANGER: DISABLING ALL SYNC BY shouldSyncHistoryMsg PREVENTS BAILEYS FROM ACCESSING INITIAL '
                    'LID MAPPINGS, LEADING TO INSTABILIY AND SESSION ERRORS')

    url = str(wa_web_socket_url)
    parsed_url = urlparse(url)
    if config.get('mobile') or parsed_url.scheme == 'tcp':
        raise Boom('Mobile API is not supported anymore', status_code=DisconnectReason.loggedOut)

    # ephemeral key pair used to encrypt/decrypt communication. Unique for each connection
    ephemeral_key_pair = Curve.generate_key_pair()
    # WA noise protocol wrapper
    noise = make_noise_handler(ephemeral_key_pair, NOISE_WA_HEADER, logger,
                               (auth_state.get('creds') or {}).get('routingInfo') if auth_state else None)

    ws = WebSocketClient(url, config)
    ws.connect()

    async def send_raw_message(data: bytes):
        """send a raw buffer"""
        if not ws.is_open:
            raise Boom('Connection Closed', status_code=DisconnectReason.connectionClosed)
        frame = noise.encode_frame(data)
        fut = asyncio.get_running_loop().create_future()

        def cb(err):
            if fut.done():
                return
            if err:
                fut.set_exception(err)
            else:
                fut.set_result(None)
        ws.send(frame, cb)

        async def work(resolve, reject):
            try:
                await fut
                resolve(None)
            except Exception as error:
                reject(error)
        await promise_timeout(connect_timeout_ms, work)

    def send_node(frame: dict):
        """send a binary node"""
        if logger.level == 'trace':
            logger.trace({'xml': binary_node_to_string(frame), 'msg': 'xml send'})
        buff = encode_binary_node(frame)
        return send_raw_message(buff)

    async def wait_for_message(msg_id: str, timeout_ms=default_query_timeout_ms):
        """Wait for a message with a certain tag to be received"""
        holder = {}

        def setup(resolve, reject):
            def on_recv(data):
                resolve(data)

            def on_err(err=None, *rest):
                reject(err if isinstance(err, BaseException) else
                       Boom('Connection Closed', status_code=DisconnectReason.connectionClosed))
            holder['onRecv'], holder['onErr'] = on_recv, on_err
            ws.on(f'TAG:{msg_id}', on_recv)
            ws.on('close', on_err)
            ws.on('error', on_err)
        try:
            return await promise_timeout(timeout_ms, setup)
        except Boom as error:
            # Catch timeout and return None instead of throwing
            if error.output.get('statusCode') == DisconnectReason.timedOut:
                logger.warn({'msgId': msg_id}, 'timed out waiting for message')
                return None
            raise
        finally:
            if holder.get('onRecv'):
                ws.off(f'TAG:{msg_id}', holder['onRecv'])
            if holder.get('onErr'):
                ws.off('close', holder['onErr'])
                ws.off('error', holder['onErr'])

    async def query(node: dict, timeout_ms: Optional[float] = None):
        """send a query, and wait for its response. auto-generates message ID if not provided"""
        if not node['attrs'].get('id'):
            node['attrs']['id'] = generate_message_tag()
        msg_id = node['attrs']['id']

        async def work(resolve, reject):
            wait_task = asyncio.ensure_future(wait_for_message(msg_id, timeout_ms))
            try:
                await send_node(node)
                resolve(await wait_task)
            except Exception as e:
                wait_task.cancel()
                reject(e)
        result = await promise_timeout(timeout_ms, work)
        if isinstance(result, dict) and 'tag' in result:
            assert_node_error_free(result)
        return result

    async def digest_key_bundle():
        """Validate current key-bundle on server; on failure, trigger pre-key upload and rethrow"""
        res = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'get', 'xmlns': 'encrypt'},
                           'content': [{'tag': 'digest', 'attrs': {}}]})
        digest_node = get_binary_node_child(res, 'digest')
        if not digest_node:
            await upload_pre_keys()
            raise Exception('encrypt/get digest returned no digest node')

    async def rotate_signed_pre_key():
        """Rotate our signed pre-key on server"""
        new_id = (creds['signedPreKey'].get('keyId') or 0) + 1
        skey = signed_key_pair(creds['signedIdentityKey'], new_id)
        await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'encrypt'},
                     'content': [{'tag': 'rotate', 'attrs': {}, 'content': [xmpp_signed_pre_key(skey)]}]})
        ev.emit('creds.update', {'signedPreKey': skey})  # Persist new signed pre-key in creds

    async def execute_usync_query(usync_query: USyncQuery):
        if len(usync_query.protocols) == 0:
            raise Boom('USyncQuery must have at least one protocol')
        # todo: validate users, throw WARNING on no valid users
        valid_users = usync_query.users
        user_nodes = []
        for user in valid_users:
            els = [e for e in (p.get_user_element(user) for p in usync_query.protocols) if e is not None]
            user_nodes.append({'tag': 'user', 'attrs': {'jid': user.id if not user.phone else None}, 'content': els})
        list_node = {'tag': 'list', 'attrs': {}, 'content': user_nodes}
        query_node = {'tag': 'query', 'attrs': {}, 'content': [p.get_query_element() for p in usync_query.protocols]}
        iq = {'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'get', 'xmlns': 'usync'},
              'content': [{'tag': 'usync',
                           'attrs': {'context': usync_query.context, 'mode': usync_query.mode,
                                     'sid': generate_message_tag(), 'last': 'true', 'index': '0'},
                           'content': [query_node, list_node]}]}
        result = await query(iq)
        return usync_query.parse_usync_query_result(result)

    async def on_whatsapp(*phone_number: str):
        usync_query = USyncQuery()
        contact_enabled = False
        for jid in phone_number:
            if is_lid_user(jid):
                logger.warn('LIDs are not supported with onWhatsApp')
                continue
            if not contact_enabled:
                contact_enabled = True
                usync_query = usync_query.with_contact_protocol()
            phone = '+' + jid.replace('+', '').split('@')[0].split(':')[0]
            usync_query.with_user(USyncUser().with_phone(phone))
        if len(usync_query.users) == 0:
            return []  # return early without forcing an empty query
        results = await execute_usync_query(usync_query)
        if results:
            return [{'jid': a['id'], 'exists': a['contact']} for a in results['list'] if a.get('contact')]
        return None

    async def pn_from_lid_usync(jids: list):
        usync_query = USyncQuery().with_lid_protocol().with_context('background')
        for jid in jids:
            if is_lid_user(jid):
                logger.warn('LID user found in LID fetch call')
                continue
            usync_query.with_user(USyncUser().with_id(jid))
        if len(usync_query.users) == 0:
            return []  # return early without forcing an empty query
        results = await execute_usync_query(usync_query)
        if results:
            return [{'pn': a['id'], 'lid': a['lid']} for a in results['list'] if a.get('lid')]
        return []

    ev = make_event_buffer(logger)

    creds = auth_state['creds']
    # add transaction capability
    keys = add_transaction_capability(auth_state['keys'], logger, **transaction_opts)
    signal_repository = make_signal_repository({'creds': creds, 'keys': keys}, logger, pn_from_lid_usync)

    socket_end_handlers: list = []

    def on_unexpected_error(err, msg: str):
        """log & process any unexpected errors"""
        logger.error({'err': err}, f"unexpected error in '{msg}'")

    async def await_next_message(send_msg: Optional[bytes] = None):
        """await the next incoming message"""
        if not ws.is_open:
            raise Boom('Connection Closed', status_code=DisconnectReason.connectionClosed)
        holder = {}

        def setup(resolve, reject):
            holder['onOpen'] = resolve
            holder['onClose'] = _map_web_socket_error(reject)
            ws.on('frame', holder['onOpen'])
            ws.on('close', holder['onClose'])
            ws.on('error', holder['onClose'])
        fut = asyncio.ensure_future(promise_timeout(connect_timeout_ms, setup))
        if send_msg:
            async def _send():
                try:
                    await send_raw_message(send_msg)
                except Exception as e:
                    holder['onClose'](e)
            asyncio.ensure_future(_send())
        try:
            return await fut
        finally:
            ws.off('frame', holder['onOpen'])
            ws.off('close', holder['onClose'])
            ws.off('error', holder['onClose'])

    async def validate_connection():
        """connection handshake"""
        hello_msg = proto.HandshakeMessage(clientHello=proto.HandshakeMessage.ClientHello(
            ephemeral=ephemeral_key_pair['public']))
        logger.info({'browser': browser, 'helloMsg': str(hello_msg)}, 'connected to WA')
        init = hello_msg.SerializeToString()
        result = await await_next_message(init)
        handshake = proto.HandshakeMessage()
        handshake.ParseFromString(bytes(result))
        logger.trace({'handshake': str(handshake)}, 'handshake recv from WA')
        key_enc = noise.process_handshake(handshake.serverHello, creds['noiseKey'])
        if not creds.get('me'):
            node = generate_registration_node(creds, config)
            logger.info({'node': str(node)}, 'not logged in, attempting registration...')
        else:
            node = generate_login_node(creds['me']['id'], config)
            logger.info({'node': str(node)}, 'logging in...')
        payload_enc = noise.encrypt(node.SerializeToString())
        await send_raw_message(proto.HandshakeMessage(clientFinish=proto.HandshakeMessage.ClientFinish(
            static=key_enc, payload=payload_enc)).SerializeToString())
        await noise.finish_init()
        start_keep_alive_request()

    async def get_available_pre_keys_on_server() -> int:
        result = await query({'tag': 'iq', 'attrs': {'id': generate_message_tag(), 'xmlns': 'encrypt', 'type': 'get',
                                                      'to': S_WHATSAPP_NET},
                              'content': [{'tag': 'count', 'attrs': {}}]})
        count_child = get_binary_node_child(result, 'count')
        return int(count_child['attrs']['value'])

    async def upload_pre_keys(count: int = MIN_PREKEY_COUNT):
        """generates and uploads a set of pre-keys to the server"""
        if state['uploadPreKeysPromise']:
            logger.debug('Pre-key upload already in progress, waiting for completion')
            await state['uploadPreKeysPromise']
            return

        async def upload_logic(retry_count: int):
            logger.info({'count': count, 'retryCount': retry_count}, 'uploading pre-keys')

            # Generate and save pre-keys atomically (prevents ID collisions on retry)
            async def gen():
                logger.debug({'requestedCount': count}, 'generating pre-keys with requested count')
                r = await get_next_pre_keys_node({'creds': creds, 'keys': keys}, count)
                # Update credentials immediately to prevent duplicate IDs on retry
                ev.emit('creds.update', r['update'])
                return r['node']
            node = await keys.transaction(gen, (creds.get('me') or {}).get('id') or 'upload-pre-keys')
            # Upload to server (outside transaction, can fail without affecting local keys)
            try:
                await query(node)
                logger.info({'count': count}, 'uploaded pre-keys successfully')
            except Exception as upload_error:
                logger.error({'uploadError': str(upload_error), 'count': count}, 'Failed to upload pre-keys to server')
                if retry_count < 3:
                    backoff = min(1000 * (2 ** retry_count), 10000)
                    logger.info(f'Retrying pre-key upload in {backoff}ms')
                    await asyncio.sleep(backoff / 1000)
                    return await upload_logic(retry_count + 1)
                raise

        async def with_timeout():
            try:
                await asyncio.wait_for(upload_logic(0), UPLOAD_TIMEOUT / 1000)
            except asyncio.TimeoutError:
                raise Boom('Pre-key upload timeout', status_code=408)
        state['uploadPreKeysPromise'] = asyncio.ensure_future(with_timeout())
        try:
            await state['uploadPreKeysPromise']
        finally:
            state['uploadPreKeysPromise'] = None

    async def verify_current_pre_key_exists():
        current_pre_key_id = creds['nextPreKeyId'] - 1
        if current_pre_key_id <= 0:
            return {'exists': False, 'currentPreKeyId': 0}
        pre_keys = await keys.get('pre-key', [str(current_pre_key_id)])
        return {'exists': bool(pre_keys.get(str(current_pre_key_id))), 'currentPreKeyId': current_pre_key_id}

    async def upload_pre_keys_to_server_if_required():
        try:
            pre_key_count = await get_available_pre_keys_on_server()
            count = INITIAL_PREKEY_COUNT if pre_key_count == 0 else MIN_PREKEY_COUNT
            v = await verify_current_pre_key_exists()
            current_exists, current_id = v['exists'], v['currentPreKeyId']
            logger.info(f'{pre_key_count} pre-keys found on server')
            logger.info(f'Current prekey ID: {current_id}, exists in storage: {current_exists}')
            low_server_count = pre_key_count <= count
            missing_current = (not current_exists) and current_id > 0
            if low_server_count or missing_current:
                reasons = []
                if low_server_count:
                    reasons.append(f'server count low ({pre_key_count})')
                if missing_current:
                    reasons.append(f'current prekey {current_id} missing from storage')
                logger.info(f"Uploading PreKeys due to: {', '.join(reasons)}")
                await upload_pre_keys(count)
            else:
                logger.info(f'PreKey validation passed - Server: {pre_key_count}, Current prekey {current_id} exists')
        except Exception as error:
            logger.error({'error': error}, 'Failed to check/upload pre-keys during initialization')
            # Don't throw - allow connection to continue even if pre-key check fails

    def _on_frame(frame):
        # reset ping timeout
        state['lastDateRecv'] = time.time()
        any_triggered = ws.emit('frame', frame)
        # if it's a binary node
        if isinstance(frame, dict):
            msg_id = frame['attrs'].get('id')
            if logger.level == 'trace':
                logger.trace({'xml': binary_node_to_string(frame), 'msg': 'recv xml'})
            # Check if this is a response to a message we sent
            any_triggered = ws.emit(f'{DEF_TAG_PREFIX}{msg_id}', frame) or any_triggered
            # Check if this is a response to a message we are expecting
            l0 = frame['tag']
            l1 = frame.get('attrs') or {}
            l2 = frame['content'][0]['tag'] if isinstance(frame.get('content'), list) and frame['content'] else ''
            for key in l1.keys():
                any_triggered = ws.emit(f'{DEF_CALLBACK_PREFIX}{l0},{key}:{l1[key]},{l2}', frame) or any_triggered
                any_triggered = ws.emit(f'{DEF_CALLBACK_PREFIX}{l0},{key}:{l1[key]}', frame) or any_triggered
                any_triggered = ws.emit(f'{DEF_CALLBACK_PREFIX}{l0},{key}', frame) or any_triggered
            any_triggered = ws.emit(f'{DEF_CALLBACK_PREFIX}{l0},,{l2}', frame) or any_triggered
            any_triggered = ws.emit(f'{DEF_CALLBACK_PREFIX}{l0}', frame) or any_triggered
            if not any_triggered and logger.level == 'debug':
                logger.debug({'unhandled': True, 'msgId': msg_id, 'fromMe': False, 'frame': frame},
                             'communication recv')

    async def on_message_received(data: bytes):
        await noise.decode_frame(data, _on_frame)

    async def end(error: Optional[BaseException]):
        if state['closed']:
            logger.trace({'trace': str(error)}, 'connection already closed')
            return
        state['closed'] = True
        logger.info({'trace': str(error)}, 'connection errored' if error else 'connection closed')
        if state['keepAliveTask']:
            state['keepAliveTask'].cancel()
        if state['qrTimer']:
            state['qrTimer'].cancel()
        ws.remove_all_listeners('close')
        ws.remove_all_listeners('open')
        ws.remove_all_listeners('message')
        if hasattr(signal_repository, 'close'):
            signal_repository.close()
        if not ws.is_closed and not ws.is_closing:
            try:
                await ws.close()
            except Exception:
                pass
        for handler in socket_end_handlers:
            try:
                r = handler(error)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as err:
                logger.error({'err': err}, 'error in socket end handler')
        ev.emit('connection.update', {'connection': 'close', 'lastDisconnect': {'error': error, 'date': datetime.now()}})
        ev.remove_all_listeners('connection.update')
        ev.destroy()

    def end_nowait(error):
        asyncio.ensure_future(end(error))

    async def wait_for_socket_open():
        if ws.is_open:
            return
        if ws.is_closed or ws.is_closing:
            raise Boom('Connection Closed', status_code=DisconnectReason.connectionClosed)
        holder = {}
        fut = asyncio.get_running_loop().create_future()

        def on_open(*_):
            if not fut.done():
                fut.set_result(None)
        mapper = _map_web_socket_error(lambda e: fut.done() or fut.set_exception(e))
        holder['onOpen'], holder['onClose'] = on_open, mapper
        ws.on('open', on_open)
        ws.on('close', mapper)
        ws.on('error', mapper)
        try:
            await fut
        finally:
            ws.off('open', on_open)
            ws.off('close', mapper)
            ws.off('error', mapper)

    def start_keep_alive_request():
        async def loop():
            while True:
                await asyncio.sleep(keep_alive_interval_ms / 1000)
                if not state['lastDateRecv']:
                    state['lastDateRecv'] = time.time()
                diff = (time.time() - state['lastDateRecv']) * 1000
                # check if it's been a suspicious amount of time since the server responded with our last seen
                if diff > keep_alive_interval_ms + 5000:
                    end_nowait(Boom('Connection was lost', status_code=DisconnectReason.connectionLost))
                elif ws.is_open:
                    # if its all good, send a keep alive request
                    async def ping():
                        try:
                            await query({'tag': 'iq', 'attrs': {'id': generate_message_tag(), 'to': S_WHATSAPP_NET,
                                                                'type': 'get', 'xmlns': 'w:p'},
                                         'content': [{'tag': 'ping', 'attrs': {}}]})
                        except Exception as err:
                            logger.error({'trace': str(err)}, 'error in sending keep alive')
                    asyncio.ensure_future(ping())
                else:
                    logger.warn('keep alive called when WS not open')
        state['keepAliveTask'] = asyncio.ensure_future(loop())

    def send_passive_iq(tag: str):
        return query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'xmlns': 'passive', 'type': 'set'},
                      'content': [{'tag': tag, 'attrs': {}}]})

    async def logout(msg: Optional[str] = None):
        """logout & invalidate connection"""
        jid = (auth_state['creds'].get('me') or {}).get('id')
        if jid:
            await send_node({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'id': generate_message_tag(),
                                                    'xmlns': 'md'},
                             'content': [{'tag': 'remove-companion-device',
                                          'attrs': {'jid': jid, 'reason': 'user_initiated'}}]})
        end_nowait(Boom(msg or 'Intentional Logout', status_code=DisconnectReason.loggedOut))

    async def generate_pairing_key() -> bytes:
        salt = os.urandom(32)
        random_iv = os.urandom(16)
        key = await derive_pairing_code_key(auth_state['creds']['pairingCode'], salt)
        ciphered = aes_encrypt_ctr(auth_state['creds']['pairingEphemeralKeyPair']['public'], key, random_iv)
        return salt + random_iv + ciphered

    async def request_pairing_code(phone_number: str, custom_pairing_code: Optional[str] = None) -> str:
        pairing_code = custom_pairing_code if custom_pairing_code is not None else bytes_to_crockford(os.urandom(5))
        if custom_pairing_code and len(custom_pairing_code) != 8:
            raise Exception('Custom pairing code must be exactly 8 chars')
        auth_state['creds']['pairingCode'] = pairing_code
        auth_state['creds']['me'] = {'id': jid_encode(phone_number, 's.whatsapp.net'), 'name': '~'}
        ev.emit('creds.update', auth_state['creds'])
        await send_node({
            'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'id': generate_message_tag(), 'xmlns': 'md'},
            'content': [{
                'tag': 'link_code_companion_reg',
                'attrs': {'jid': auth_state['creds']['me']['id'], 'stage': 'companion_hello',
                          'should_show_push_notification': 'true'},
                'content': [
                    {'tag': 'link_code_pairing_wrapped_companion_ephemeral_pub', 'attrs': {},
                     'content': await generate_pairing_key()},
                    {'tag': 'companion_server_auth_key_pub', 'attrs': {},
                     'content': auth_state['creds']['noiseKey']['public']},
                    {'tag': 'companion_platform_id', 'attrs': {}, 'content': get_companion_platform_id(browser)},
                    {'tag': 'companion_platform_display', 'attrs': {}, 'content': f'{browser[1]} ({browser[0]})'},
                    {'tag': 'link_code_pairing_nonce', 'attrs': {}, 'content': '0'},
                ]}]})
        return auth_state['creds']['pairingCode']

    def send_wam_buffer(wam_buffer: bytes):
        return query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'id': generate_message_tag(), 'xmlns': 'w:stats'},
                      'content': [{'tag': 'add', 'attrs': {'t': str(round(time.time()))}, 'content': wam_buffer}]})

    ws.on('message', on_message_received)

    async def on_open():
        try:
            await validate_connection()
        except Exception as err:
            logger.error({'err': err}, 'error in validating connection')
            await end(err)
    ws.on('open', on_open)
    ws.on('error', _map_web_socket_error(end_nowait))
    ws.on('close', lambda *a: end_nowait(Boom('Connection Terminated', status_code=DisconnectReason.connectionClosed)))
    # the server terminated the connection
    ws.on('CB:xmlstreamend', lambda *a: end_nowait(
        Boom('Connection Terminated by Server', status_code=DisconnectReason.connectionClosed)))

    # QR gen
    async def on_pair_device(stanza: dict):
        await send_node({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'result', 'id': stanza['attrs']['id']}})
        pair_device_node = get_binary_node_child(stanza, 'pair-device')
        ref_nodes = get_binary_node_children(pair_device_node, 'ref')
        noise_key_b64 = base64.b64encode(bytes(creds['noiseKey']['public'])).decode()
        identity_key_b64 = base64.b64encode(bytes(creds['signedIdentityKey']['public'])).decode()
        adv_b64 = creds['advSecretKey']
        qr_ms = {'v': qr_timeout or 60_000}  # time to let a QR live

        def gen_pair_qr():
            if not ws.is_open:
                return
            if not ref_nodes:
                end_nowait(Boom('QR refs attempts ended', status_code=DisconnectReason.timedOut))
                return
            ref_node = ref_nodes.pop(0)
            ref = bytes(ref_node['content']).decode('utf-8')
            qr = build_pairing_qr_data(ref, noise_key_b64, identity_key_b64, adv_b64, browser)
            ev.emit('connection.update', {'qr': qr})
            state['qrTimer'] = asyncio.get_running_loop().call_later(qr_ms['v'] / 1000, gen_pair_qr)
            qr_ms['v'] = qr_timeout or 20_000  # shorter subsequent qrs
        gen_pair_qr()
    ws.on('CB:iq,type:set,pair-device', on_pair_device)

    def update_server_time_offset(node: dict):
        t_value = (node.get('attrs') or {}).get('t')
        if not t_value:
            return
        try:
            parsed = float(t_value)
        except ValueError:
            return
        if parsed != parsed or parsed <= 0:
            return
        state['serverTimeOffsetMs'] = parsed * 1000 - time.time() * 1000
        logger.debug({'offset': state['serverTimeOffsetMs']}, 'calculated server time offset')

    # device paired for the first time; if device pairs successfully, the server asks to restart the connection
    async def on_pair_success(stanza: dict):
        logger.debug('pair success recv')
        try:
            update_server_time_offset(stanza)
            r = configure_successful_pairing(stanza, creds['advSecretKey'], creds['signedIdentityKey'],
                                             creds.get('signalIdentities'))
            reply, updated_creds = r['reply'], r['creds']
            logger.info({'me': updated_creds.get('me'), 'platform': updated_creds.get('platform')},
                        'pairing configured successfully, expect to restart the connection...')
            ev.emit('creds.update', updated_creds)
            ev.emit('connection.update', {'isNewLogin': True, 'qr': None})
            await send_node(reply)
            asyncio.ensure_future(send_unified_session())
        except Exception as error:
            logger.info({'trace': str(error)}, 'error in pairing')
            await end(error)
    ws.on('CB:iq,,pair-success', on_pair_success)

    # login complete
    async def on_success(node: dict):
        try:
            update_server_time_offset(node)
            await upload_pre_keys_to_server_if_required()
            await send_passive_iq('active')
            # After successful login, validate our key-bundle against server
            try:
                await digest_key_bundle()
            except Exception as e:
                logger.warn({'e': e}, 'failed to run digest after login')
        except Exception as err:
            logger.warn({'err': err}, 'failed to send initial passive iq')
        logger.info('opened connection to WA')
        if state['qrTimer']:
            state['qrTimer'].cancel()  # will never happen in all likelyhood -- but just in case
        ev.emit('creds.update', {'me': {**auth_state['creds']['me'], 'lid': node['attrs'].get('lid')}})
        ev.emit('connection.update', {'connection': 'open'})
        asyncio.ensure_future(send_unified_session())

        if node['attrs'].get('lid') and (auth_state['creds'].get('me') or {}).get('id'):
            my_lid = node['attrs']['lid']

            async def own_lid_session():
                try:
                    my_pn = auth_state['creds']['me']['id']
                    # Store our own LID-PN mapping
                    await signal_repository.lid_mapping.store_lid_pn_mappings([{'lid': my_lid, 'pn': my_pn}])
                    # Create device list for our own user (needed for bulk migration)
                    d = jid_decode(my_pn)
                    await auth_state['keys'].set({'device-list': {d.user: [str(d.device) if d.device else '0']}})
                    # migrate our own session
                    await signal_repository.migrate_session(my_pn, my_lid)
                    logger.info({'myPN': my_pn, 'myLID': my_lid}, 'Own LID session created successfully')
                except Exception as error:
                    logger.error({'error': error, 'lid': my_lid}, 'Failed to create own LID session')
            asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(own_lid_session()))
    ws.on('CB:success', on_success)

    def on_stream_error(node: dict):
        children = get_all_binary_node_children(node)
        reason_node = children[0] if children else None
        logger.error({'reasonNode': reason_node, 'fullErrorNode': node}, 'stream errored out')
        r = get_error_code_from_stream_error(node)
        end_nowait(Boom(f"Stream Errored ({r['reason']})", status_code=r['statusCode'], data=reason_node or node))
    ws.on('CB:stream:error', on_stream_error)

    # stream fail, possible logout
    ws.on('CB:failure', lambda node: end_nowait(Boom('Connection Failure', status_code=int(node['attrs'].get('reason') or 500),
                                                     data=node['attrs'])))
    ws.on('CB:ib,,downgrade_webclient', lambda node: end_nowait(
        Boom('Multi-device beta not joined', status_code=DisconnectReason.multideviceMismatch)))

    async def on_offline_preview(node: dict):
        logger.info('offline preview received ' + json.dumps(node, default=str))
        await send_node({'tag': 'ib', 'attrs': {}, 'content': [{'tag': 'offline_batch', 'attrs': {'count': '100'}}]})
    ws.on('CB:ib,,offline_preview', on_offline_preview)

    def on_edge_routing(node: dict):
        edge_routing_node = get_binary_node_child(node, 'edge_routing')
        routing_info = get_binary_node_child(edge_routing_node, 'routing_info')
        if routing_info and routing_info.get('content'):
            auth_state['creds']['routingInfo'] = bytes(routing_info['content'])
            ev.emit('creds.update', auth_state['creds'])
    ws.on('CB:ib,,edge_routing', on_edge_routing)

    def _next_tick():
        if (creds.get('me') or {}).get('id'):
            # start buffering important events if we're logged in
            ev.buffer()
            state['didStartBuffer'] = True
        ev.emit('connection.update', {'connection': 'connecting', 'receivedPendingNotifications': False, 'qr': None})
    asyncio.get_running_loop().call_soon(_next_tick)

    # called when all offline notifs are handled
    def on_offline(node: dict):
        child = get_binary_node_child(node, 'offline')
        offline_notifs = int(((child or {}).get('attrs') or {}).get('count') or 0)
        logger.info(f'handled {offline_notifs} offline messages/notifications')
        if state['didStartBuffer']:
            ev.flush()
            logger.trace('flushed events for initial buffer')
        ev.emit('connection.update', {'receivedPendingNotifications': True})
    ws.on('CB:ib,,offline', on_offline)

    # update credentials when required
    def on_creds_update(update: dict):
        name = (update.get('me') or {}).get('name')
        # if name has just been received
        if (creds.get('me') or {}).get('name') != name:
            logger.debug({'name': name}, 'updated pushName')

            async def send_presence():
                try:
                    await send_node({'tag': 'presence', 'attrs': {'name': name}})
                except Exception as err:
                    logger.warn({'trace': str(err)}, 'error in sending presence update on name change')
            asyncio.ensure_future(send_presence())
        creds.update(update)
    ev.on('creds.update', on_creds_update)

    def get_unified_session_id() -> str:
        offset_ms = 3 * TimeMs.Day
        now = time.time() * 1000 + state['serverTimeOffsetMs']
        return str(int((now + offset_ms) % TimeMs.Week))

    async def send_unified_session():
        if not ws.is_open:
            return
        node = {'tag': 'ib', 'attrs': {}, 'content': [{'tag': 'unified_session',
                                                       'attrs': {'id': get_unified_session_id()}}]}
        try:
            await send_node(node)
        except Exception as error:
            logger.debug({'error': error}, 'failed to send unified_session telemetry')

    def register_socket_end_handler(handler: Callable):
        socket_end_handlers.append(handler)

    async def fetch_account_reachout_timelock() -> dict:
        """Fetches your account's standing when it comes to restrictions."""
        query_result = await execute_w_mex_query({}, QueryIds.REACHOUT_TIMELOCK.value,
                                                 XWAPaths.xwa2_fetch_account_reachout_timelock.value, query,
                                                 generate_message_tag)
        ends = (query_result or {}).get('time_enforcement_ends')
        result = {
            'isActive': bool((query_result or {}).get('is_active')),
            'timeEnforcementEnds': datetime.fromtimestamp(int(ends)) if ends and ends != '0' else None,
            'enforcementType': (query_result or {}).get('enforcement_type') or ReachoutTimelockEnforcementType.DEFAULT,
        }
        ev.emit('connection.update', {'reachoutTimeLock': result})
        return result

    async def fetch_new_chat_message_cap():
        """Fetches your account's new chat limits."""
        return await execute_w_mex_query({'input': {'type': 'INDIVIDUAL_NEW_CHAT_MSG'}},
                                         QueryIds.MESSAGE_CAPPING_INFO.value,
                                         XWAPaths.xwa2_message_capping_info.value, query, generate_message_tag)

    return Namespace(
        {'user': lambda: auth_state['creds'].get('me')},
        type='md', ws=ws, ev=ev, authState={'creds': creds, 'keys': keys}, auth_state={'creds': creds, 'keys': keys},
        signalRepository=signal_repository, signal_repository=signal_repository,
        generate_message_tag=generate_message_tag, query=query, wait_for_message=wait_for_message,
        wait_for_socket_open=wait_for_socket_open, send_raw_message=send_raw_message, send_node=send_node,
        logout=logout, end=end, register_socket_end_handler=register_socket_end_handler,
        on_unexpected_error=on_unexpected_error, upload_pre_keys=upload_pre_keys,
        upload_pre_keys_to_server_if_required=upload_pre_keys_to_server_if_required,
        digest_key_bundle=digest_key_bundle, rotate_signed_pre_key=rotate_signed_pre_key,
        request_pairing_code=request_pairing_code, update_server_time_offset=update_server_time_offset,
        send_unified_session=send_unified_session, wam_buffer=public_wam_buffer,
        # Waits for the connection to WA to reach a state
        wait_for_connection_update=bind_wait_for_connection_update(ev), send_wam_buffer=send_wam_buffer,
        execute_usync_query=execute_usync_query, on_whatsapp=on_whatsapp,
        fetch_account_reachout_timelock=fetch_account_reachout_timelock,
        fetch_new_chat_message_cap=fetch_new_chat_message_cap,
    )


def _hist_msg(sync_type):
    """stand-in for the `{syncType}` object passed to shouldSyncHistoryMessage"""
    return {'syncType': sync_type}
