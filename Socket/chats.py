from __future__ import annotations
import asyncio
import base64
import time
from typing import Optional

from ..Defaults import DEFAULT_CACHE_TTLS, HISTORY_SYNC_PAUSED_TIMEOUT_MS, PROCESSABLE_HISTORY_TYPES
from ..Types import ALL_WA_PATCH_NAMES, SyncState
from ..Utils.cache import TTLCache
from ..Utils.chat_utils import (chat_modification_to_app_patch, decode_patches, decode_syncd_snapshot,
                                encode_syncd_patch, ensure_lt_hash_state_version, extract_syncd_patches,
                                is_app_state_sync_irrecoverable, is_missing_key_error, MAX_SYNC_ATTEMPTS,
                                new_lt_hash_state, process_sync_action)
from ..Utils.history import get_history_msg
from ..Utils.make_mutex import make_mutex
from ..Utils.messages_media import generate_profile_picture
from ..Utils.process_message import process_message
from ..Utils.tc_token_utils import build_tc_token_from_jid
from ..WABinary import (Boom, get_binary_node_child, get_binary_node_children, is_hosted_lid_user, is_hosted_pn_user,
                        is_lid_user, is_pn_user, jid_decode, jid_normalized_user, reduce_binary_node_to_dictionary,
                        S_WHATSAPP_NET)
from ..WAProto import WAProto_pb2 as proto
from ..WAUSync import USyncQuery, USyncUser
from .socket import make_socket


def build_profile_picture_query_content(type_: str, tc_token_content: Optional[list] = None) -> list:
    picture = {'tag': 'picture', 'attrs': {'type': type_, 'query': 'url'}}
    if tc_token_content:
        picture['content'] = tc_token_content
    return [picture]


def make_chats_socket(config: dict):
    logger = config['logger']
    mark_online_on_connect = config['markOnlineOnConnect']
    fire_init_queries = config['fireInitQueries']
    app_state_mac_verification = config['appStateMacVerification']
    should_ignore_jid = config['shouldIgnoreJid']
    should_sync_history_message = config['shouldSyncHistoryMessage']
    get_message = config.get('getMessage')

    sock = make_socket(config)
    ev, ws, auth_state = sock.ev, sock.ws, sock.auth_state
    generate_message_tag, send_node, query = sock.generate_message_tag, sock.send_node, sock.query
    signal_repository = sock.signal_repository
    on_unexpected_error = sock.on_unexpected_error
    send_unified_session = sock.send_unified_session
    register_socket_end_handler = sock.register_socket_end_handler

    get_lid_for_pn = signal_repository.lid_mapping.get_lid_for_pn

    st = {'privacySettings': None, 'syncState': SyncState.Connecting, 'awaitingSyncTimeout': None,
          'historySyncPausedTimeout': None}

    # Server-assigned AB props for protocol behavior.
    server_props = {
        'privacyTokenOn1to1': True,  # AB prop 10518: gate tctoken on 1:1 messages. Default true (avoids 463).
        'profilePicPrivacyToken': True,  # AB prop 9666: gate tctoken on profile picture IQs.
        'lidTrustedTokenIssueToLid': False,  # AB prop 14303: issue tctokens to LID instead of PN.
    }

    message_mutex = make_mutex()  # ensures that messages are processed in order
    receipt_mutex = make_mutex()  # ensures that receipts are processed in order
    app_state_patch_mutex = make_mutex()  # ensures that app state patches are processed in order
    notification_mutex = make_mutex()  # ensures that notifications are processed in order

    # In-memory history sync completion tracking (resets on reconnection)
    history_sync_status = {'initialBootstrapComplete': False, 'recentSyncComplete': False}

    # Collections blocked on missing app state sync keys (mirrors WA Web's "Blocked" state).
    blocked_collections: set = set()

    placeholder_resend_cache = config.get('placeholderResendCache') or TTLCache(ttl=DEFAULT_CACHE_TTLS['MSG_RETRY'])

    async def get_app_state_sync_key(key_id: str):
        """helper function to fetch the given app state sync key"""
        return (await auth_state['keys'].get('app-state-sync-key', [key_id])).get(key_id)

    async def fetch_privacy_settings(force: bool = False):
        if not st['privacySettings'] or force:
            result = await query({'tag': 'iq', 'attrs': {'xmlns': 'privacy', 'to': S_WHATSAPP_NET, 'type': 'get'},
                                  'content': [{'tag': 'privacy', 'attrs': {}}]})
            st['privacySettings'] = reduce_binary_node_to_dictionary(result['content'][0], 'category')
        return st['privacySettings']

    async def privacy_query(name: str, value: str):
        """helper function to run a privacy IQ query"""
        await query({'tag': 'iq', 'attrs': {'xmlns': 'privacy', 'to': S_WHATSAPP_NET, 'type': 'set'},
                     'content': [{'tag': 'privacy', 'attrs': {},
                                  'content': [{'tag': 'category', 'attrs': {'name': name, 'value': value}}]}]})

    async def update_messages_privacy(value): await privacy_query('messages', value)
    async def update_call_privacy(value): await privacy_query('calladd', value)
    async def update_last_seen_privacy(value): await privacy_query('last', value)
    async def update_online_privacy(value): await privacy_query('online', value)
    async def update_profile_picture_privacy(value): await privacy_query('profile', value)
    async def update_status_privacy(value): await privacy_query('status', value)
    async def update_read_receipts_privacy(value): await privacy_query('readreceipts', value)
    async def update_groups_add_privacy(value): await privacy_query('groupadd', value)

    async def update_default_disappearing_mode(duration: int):
        await query({'tag': 'iq', 'attrs': {'xmlns': 'disappearing_mode', 'to': S_WHATSAPP_NET, 'type': 'set'},
                     'content': [{'tag': 'disappearing_mode', 'attrs': {'duration': str(duration)}}]})

    async def get_bot_list_v2():
        resp = await query({'tag': 'iq', 'attrs': {'xmlns': 'bot', 'to': S_WHATSAPP_NET, 'type': 'get'},
                            'content': [{'tag': 'bot', 'attrs': {'v': '2'}}]})
        bot_node = get_binary_node_child(resp, 'bot')
        bot_list = []
        for section in get_binary_node_children(bot_node, 'section'):
            if section['attrs'].get('type') == 'all':
                for bot in get_binary_node_children(section, 'bot'):
                    bot_list.append({'jid': bot['attrs'].get('jid'), 'personaId': bot['attrs'].get('persona_id')})
        return bot_list

    async def fetch_status(*jids: str):
        usync_query = USyncQuery().with_status_protocol()
        for jid in jids:
            usync_query.with_user(USyncUser().with_id(jid))
        result = await sock.execute_usync_query(usync_query)
        if result:
            return result['list']

    async def fetch_disappearing_duration(*jids: str):
        usync_query = USyncQuery().with_disappearing_mode_protocol()
        for jid in jids:
            usync_query.with_user(USyncUser().with_id(jid))
        result = await sock.execute_usync_query(usync_query)
        if result:
            return result['list']

    def _target_jid_for_profile(jid: str) -> Optional[str]:
        if not jid:
            raise Boom('Illegal no-jid profile update. Please specify either your ID or the ID of the chat you '
                       'wish to update')
        if jid_normalized_user(jid) != jid_normalized_user(auth_state['creds']['me']['id']):
            return jid_normalized_user(jid)  # in case it is someone other than us
        return None

    async def update_profile_picture(jid: str, content, dimensions: Optional[dict] = None):
        """update the profile picture for yourself or a group"""
        target_jid = _target_jid_for_profile(jid)
        img = (await generate_profile_picture(content, dimensions))['img']
        await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:profile:picture',
                                           **({'target': target_jid} if target_jid else {})},
                     'content': [{'tag': 'picture', 'attrs': {'type': 'image'}, 'content': img}]})

    async def remove_profile_picture(jid: str):
        """remove the profile picture for yourself or a group"""
        target_jid = _target_jid_for_profile(jid)
        await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:profile:picture',
                                           **({'target': target_jid} if target_jid else {})}})

    async def update_profile_status(status: str):
        """update the profile status for yourself"""
        await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'status'},
                     'content': [{'tag': 'status', 'attrs': {}, 'content': status.encode('utf-8')}]})

    async def update_profile_name(name: str):
        await chat_modify({'pushNameSetting': name}, '')

    async def fetch_blocklist():
        result = await query({'tag': 'iq', 'attrs': {'xmlns': 'blocklist', 'to': S_WHATSAPP_NET, 'type': 'get'}})
        list_node = get_binary_node_child(result, 'list')
        return [n['attrs'].get('jid') for n in get_binary_node_children(list_node, 'item')]

    async def update_block_status(jid: str, action: str):
        normalized_jid = jid_normalized_user(jid)
        pn_jid = None
        if is_lid_user(normalized_jid) or is_hosted_lid_user(normalized_jid):
            lid = normalized_jid
            if action == 'block':
                pn = await signal_repository.lid_mapping.get_pn_for_lid(normalized_jid)
                if not pn:
                    raise Boom(f'Unable to resolve PN JID for LID: {jid}', status_code=400)
                pn_jid = jid_normalized_user(pn)
        elif is_pn_user(normalized_jid) or is_hosted_pn_user(normalized_jid):
            mapped = await signal_repository.lid_mapping.get_lid_for_pn(normalized_jid)
            if not mapped:
                raise Boom(f'Unable to resolve LID for PN JID: {jid}', status_code=400)
            lid = mapped
            if action == 'block':
                pn_jid = jid_normalized_user(normalized_jid)
        else:
            raise Boom(f'Invalid jid: {jid}', status_code=400)
        item_attrs = {'action': action, 'jid': lid}
        if action == 'block':
            if not pn_jid:
                raise Boom(f'pn_jid required for block: {jid}', status_code=400)
            item_attrs['pn_jid'] = pn_jid
        await query({'tag': 'iq', 'attrs': {'xmlns': 'blocklist', 'to': S_WHATSAPP_NET, 'type': 'set'},
                     'content': [{'tag': 'item', 'attrs': item_attrs}]})

    def _text(node):
        c = node.get('content') if node else None
        if c is None:
            return None
        return c.decode() if isinstance(c, (bytes, bytearray)) else str(c)

    async def get_business_profile(jid: str):
        results = await query({'tag': 'iq', 'attrs': {'to': 's.whatsapp.net', 'xmlns': 'w:biz', 'type': 'get'},
                               'content': [{'tag': 'business_profile', 'attrs': {'v': '244'},
                                            'content': [{'tag': 'profile', 'attrs': {'jid': jid}}]}]})
        profile_node = get_binary_node_child(results, 'business_profile')
        profiles = get_binary_node_child(profile_node, 'profile')
        if profiles:
            address = get_binary_node_child(profiles, 'address')
            description = get_binary_node_child(profiles, 'description')
            website = get_binary_node_child(profiles, 'website')
            email = get_binary_node_child(profiles, 'email')
            category = get_binary_node_child(get_binary_node_child(profiles, 'categories'), 'category')
            business_hours = get_binary_node_child(profiles, 'business_hours')
            business_hours_config = get_binary_node_children(business_hours, 'business_hours_config') \
                if business_hours else None
            website_str = _text(website)
            return {
                'wid': (profiles.get('attrs') or {}).get('jid'),
                'address': _text(address),
                'description': _text(description) or '',
                'website': [website_str] if website_str else [],
                'email': _text(email),
                'category': _text(category),
                'business_hours': {
                    'timezone': ((business_hours or {}).get('attrs') or {}).get('timezone'),
                    'business_config': [c['attrs'] for c in business_hours_config] if business_hours_config else None,
                },
            }
        return None

    async def clean_dirty_bits(type_: str, from_timestamp=None):
        logger.info({'fromTimestamp': from_timestamp}, 'clean dirty bits ' + type_)
        await send_node({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set',
                                               'xmlns': 'urn:xmpp:whatsapp:dirty', 'id': generate_message_tag()},
                         'content': [{'tag': 'clean', 'attrs': {
                             'type': type_, **({'timestamp': str(from_timestamp)} if from_timestamp else {})}}]})

    def new_app_state_chunk_handler(is_initial_sync: bool):
        def on_mutation(mutation):
            process_sync_action(mutation, ev, auth_state['creds'].get('me'),
                                {'accountSettings': auth_state['creds'].get('accountSettings')}
                                if is_initial_sync else None, logger)
        return {'onMutation': on_mutation}

    async def _resync_app_state(collections, is_initial_sync: bool):
        app_state_sync_key_cache: dict = {}

        async def get_cached_app_state_sync_key(key_id: str):
            if key_id in app_state_sync_key_cache:
                return app_state_sync_key_cache[key_id]
            key = await get_app_state_sync_key(key_id)
            app_state_sync_key_cache[key_id] = key
            return key

        # we use this to determine which events to fire; otherwise when we resync from scratch all notifs fire
        initial_version_map: dict = {}
        global_mutation_map: dict = {}

        async def work():
            collections_to_handle = list(dict.fromkeys(collections))
            # in case something goes wrong -- ensure we don't enter a loop that cannot be exited from
            attempts_map: dict = {}
            # collections that failed and need a full snapshot on retry (mirrors WA Web's ErrorFatal)
            force_snapshot_collections: set = set()
            # keep executing till all collections are done (determined by the "has_more_patches" flag)
            while collections_to_handle:
                states: dict = {}
                nodes = []
                for name in collections_to_handle:
                    result = await auth_state['keys'].get('app-state-sync-version', [name])
                    state = result.get(name)
                    if state:
                        state = ensure_lt_hash_state_version(state)
                        if name not in initial_version_map:
                            initial_version_map[name] = state['version']
                    else:
                        state = new_lt_hash_state()
                    states[name] = state
                    should_force_snapshot = name in force_snapshot_collections
                    force_snapshot_collections.discard(name)
                    logger.info(f"resyncing {name} from v{state['version']}"
                                f"{' (forcing snapshot)' if should_force_snapshot else ''}")
                    nodes.append({'tag': 'collection', 'attrs': {
                        'name': name, 'version': str(state['version']),
                        # return snapshot if syncing from scratch or forcing after a failed attempt
                        'return_snapshot': str(bool(should_force_snapshot or not state['version'])).lower()}})
                result = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'xmlns': 'w:sync:app:state',
                                                              'type': 'set'},
                                      'content': [{'tag': 'sync', 'attrs': {}, 'content': nodes}]})
                # extract from binary node
                decoded = await extract_syncd_patches(result, config.get('options'))
                for name, d in decoded.items():
                    patches, has_more_patches, snapshot = d['patches'], d['hasMorePatches'], d['snapshot']
                    try:
                        if snapshot:
                            r = await decode_syncd_snapshot(name, snapshot, get_cached_app_state_sync_key,
                                                            initial_version_map.get(name),
                                                            app_state_mac_verification['snapshot'], logger)
                            states[name] = r['state']
                            global_mutation_map.update(r['mutationMap'])
                            logger.info(f"restored state of {name} from snapshot to v{r['state']['version']} "
                                        'with mutations')
                            await auth_state['keys'].set({'app-state-sync-version': {name: r['state']}})
                        # only process if there are syncd patches
                        if patches:
                            r = await decode_patches(name, patches, states[name], get_cached_app_state_sync_key,
                                                     config.get('options'), initial_version_map.get(name), logger,
                                                     app_state_mac_verification['patch'])
                            await auth_state['keys'].set({'app-state-sync-version': {name: r['state']}})
                            logger.info(f"synced {name} to v{r['state']['version']}")
                            initial_version_map[name] = r['state']['version']
                            global_mutation_map.update(r['mutationMap'])
                        if has_more_patches:
                            logger.info(f'{name} has more patches...')
                        elif name in collections_to_handle:
                            collections_to_handle.remove(name)  # collection is done with sync
                    except Exception as error:
                        attempts_map[name] = attempts_map.get(name, 0) + 1
                        log_data = {'name': name, 'attempt': attempts_map[name], 'version': states[name]['version'],
                                    'statusCode': getattr(error, 'output', {}).get('statusCode')
                                    if hasattr(error, 'output') else None,
                                    'errorType': type(error).__name__, 'error': str(error)}
                        if is_missing_key_error(error) and attempts_map[name] >= MAX_SYNC_ATTEMPTS:
                            # WA Web treats missing keys as "Blocked" — park the collection until the key arrives
                            logger.warn(log_data, f"{name} blocked on missing key from v{states[name]['version']}, "
                                                  f'parking after {attempts_map[name]} attempts')
                            blocked_collections.add(name)
                            if name in collections_to_handle:
                                collections_to_handle.remove(name)
                        elif is_missing_key_error(error):
                            # Retry with a snapshot which may use a different key.
                            logger.info(log_data, f"{name} blocked on missing key from v{states[name]['version']}, "
                                                  'retrying with snapshot')
                            force_snapshot_collections.add(name)
                        elif is_app_state_sync_irrecoverable(error, attempts_map[name]):
                            logger.warn(log_data, f"failed to sync {name} from v{states[name]['version']}, giving up")
                            if name in collections_to_handle:
                                collections_to_handle.remove(name)
                        else:
                            logger.info(log_data, f"failed to sync {name} from v{states[name]['version']}, "
                                                  'forcing snapshot retry')
                            # force a full snapshot on retry to recover from corrupted local state
                            force_snapshot_collections.add(name)
        await auth_state['keys'].transaction(work, ((auth_state.get('creds') or {}).get('me') or {}).get('id')
                                             or 'resync-app-state')
        on_mutation = new_app_state_chunk_handler(is_initial_sync)['onMutation']
        for key in list(global_mutation_map):
            on_mutation(global_mutation_map[key])

    resync_app_state = ev.create_buffered_function(_resync_app_state)

    async def profile_picture_url(jid: str, type_: str = 'preview', timeout_ms: Optional[float] = None):
        """fetch the profile picture of a user/group. type = "preview" for low res, "image" for high res"""
        # WA Web only includes tctoken for user JIDs (not groups/newsletters) and never for own profile pic
        normalized_jid = jid_normalized_user(jid)
        is_user_jid = is_pn_user(normalized_jid) or is_lid_user(normalized_jid)
        me = auth_state['creds'].get('me')
        is_self = me and (normalized_jid == jid_normalized_user(me['id']) or
                          (me.get('lid') and normalized_jid == jid_normalized_user(me['lid'])))
        tc_token_content = None
        if server_props['profilePicPrivacyToken'] and is_user_jid and not is_self:
            tc_token_content = await build_tc_token_from_jid(auth_state, normalized_jid, get_lid_for_pn)
        jid = jid_normalized_user(jid)
        result = await query({'tag': 'iq', 'attrs': {'target': jid, 'to': S_WHATSAPP_NET, 'type': 'get',
                                                      'xmlns': 'w:profile:picture'},
                              'content': build_profile_picture_query_content(type_, tc_token_content)}, timeout_ms)
        child = get_binary_node_child(result, 'picture')
        return ((child or {}).get('attrs') or {}).get('url')

    async def create_call_link(type_: str, event: Optional[dict] = None, timeout_ms: Optional[float] = None):
        result = await query({'tag': 'call', 'attrs': {'id': generate_message_tag(), 'to': '@call'},
                              'content': [{'tag': 'link_create', 'attrs': {'media': type_},
                                           'content': [{'tag': 'event', 'attrs': {'start_time': str(event['startTime'])}}]
                                           if event else None}]}, timeout_ms)
        child = get_binary_node_child(result, 'link_create')
        return ((child or {}).get('attrs') or {}).get('token')

    async def send_presence_update(type_: str, to_jid: Optional[str] = None):
        me = auth_state['creds']['me']
        is_available_type = type_ == 'available'
        if is_available_type or type_ == 'unavailable':
            if not me.get('name'):
                logger.warn('no name present, ignoring presence update request...')
                return
            ev.emit('connection.update', {'isOnline': is_available_type})
            if is_available_type:
                asyncio.ensure_future(send_unified_session())
            await send_node({'tag': 'presence', 'attrs': {'name': me['name'].replace('@', ''), 'type': type_}})
        else:
            server = jid_decode(to_jid).server
            is_lid = server == 'lid'
            await send_node({'tag': 'chatstate',
                             'attrs': {'from': me.get('lid') if is_lid else me['id'], 'to': to_jid},
                             'content': [{'tag': 'composing' if type_ == 'recording' else type_,
                                          'attrs': {'media': 'audio'} if type_ == 'recording' else {}}]})

    async def presence_subscribe(to_jid: str):
        # Only include tctoken for user JIDs — groups/newsletters don't use tctokens
        normalized_to_jid = jid_normalized_user(to_jid)
        is_user_jid = is_pn_user(normalized_to_jid) or is_lid_user(normalized_to_jid)
        tc_token_content = await build_tc_token_from_jid(auth_state, normalized_to_jid, get_lid_for_pn) \
            if is_user_jid else None
        return await send_node({'tag': 'presence', 'attrs': {'to': to_jid, 'id': generate_message_tag(),
                                                              'type': 'subscribe'}, 'content': tc_token_content})

    def handle_presence_update(node: dict):
        tag, attrs, content = node['tag'], node['attrs'], node.get('content')
        presence = None
        jid = attrs.get('from')
        participant = attrs.get('participant') or attrs.get('from')
        if should_ignore_jid(jid) and jid != S_WHATSAPP_NET:
            return
        if tag == 'presence':
            presence = {'lastKnownPresence': 'unavailable' if attrs.get('type') == 'unavailable' else 'available',
                        'lastSeen': int(attrs['last']) if attrs.get('last') and attrs['last'] != 'deny' else None,
                        'groupOnlineCount': int(attrs['count']) if attrs.get('count') else None}
        elif isinstance(content, list):
            first_child = content[0]
            type_ = first_child['tag']
            if type_ == 'paused':
                type_ = 'available'
            if (first_child.get('attrs') or {}).get('media') == 'audio':
                type_ = 'recording'
            presence = {'lastKnownPresence': type_}
        else:
            logger.error({'tag': tag, 'attrs': attrs, 'content': content}, 'recv invalid presence node')
        if presence:
            ev.emit('presence.update', {'id': jid, 'presences': {participant: presence}})

    async def app_patch(patch_create: dict):
        name = patch_create['type']
        my_app_state_key_id = auth_state['creds'].get('myAppStateKeyId')
        if not my_app_state_key_id:
            raise Boom('App state key not present!', status_code=400)
        holder = {}

        async def under_mutex():
            async def tx():
                logger.debug({'patch': patch_create}, 'applying app patch')
                await resync_app_state([name], False)
                current_sync_version = (await auth_state['keys'].get('app-state-sync-version', [name])).get(name)
                holder['initial'] = ensure_lt_hash_state_version(current_sync_version) if current_sync_version \
                    else new_lt_hash_state()
                holder['encodeResult'] = await encode_syncd_patch(patch_create, my_app_state_key_id,
                                                                  holder['initial'], get_app_state_sync_key)
                patch, state = holder['encodeResult']['patch'], holder['encodeResult']['state']
                from ..Utils.proto_utils import from_object
                node = {'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:sync:app:state'},
                        'content': [{'tag': 'sync', 'attrs': {}, 'content': [{
                            'tag': 'collection',
                            'attrs': {'name': name, 'version': str(state['version'] - 1), 'return_snapshot': 'false'},
                            'content': [{'tag': 'patch', 'attrs': {},
                                         'content': from_object(proto.SyncdPatch, patch).SerializeToString()}]}]}]}
                await query(node)
                await auth_state['keys'].set({'app-state-sync-version': {name: state}})
            await auth_state['keys'].transaction(tx, ((auth_state.get('creds') or {}).get('me') or {}).get('id')
                                                 or 'app-patch')
        await app_state_patch_mutex.mutex(under_mutex)

        if config.get('emitOwnEvents'):
            on_mutation = new_app_state_chunk_handler(False)['onMutation']
            er = holder['encodeResult']
            r = await decode_patches(name, [{**er['patch'], 'version': {'version': er['state']['version']}}],
                                     holder['initial'], get_app_state_sync_key, config.get('options'), None, logger)
            for key in r['mutationMap']:
                on_mutation(r['mutationMap'][key])

    async def fetch_props():
        """fetch AB props"""
        creds = auth_state.get('creds') or {}
        result_node = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'xmlns': 'abt', 'type': 'get'},
                                   'content': [{'tag': 'props', 'attrs': {
                                       'protocol': '1',
                                       **({'hash': creds['lastPropHash']} if creds.get('lastPropHash') else {})}}]})
        props_node = get_binary_node_child(result_node, 'props')
        props: dict = {}
        if props_node:
            if (props_node.get('attrs') or {}).get('hash'):
                # on some clients, the hash is returning as undefined
                auth_state['creds']['lastPropHash'] = props_node['attrs']['hash']
                ev.emit('creds.update', auth_state['creds'])
            props = reduce_binary_node_to_dictionary(props_node, 'prop')

        def flag(*keys):
            for k in keys:
                if props.get(k) is not None:
                    return props[k] in ('true', '1')
            return None
        # Extract protocol-relevant AB props (only the ones we need)
        v = flag('10518', 'privacy_token_sending_on_all_1_on_1_messages')
        if v is not None:
            server_props['privacyTokenOn1to1'] = v
        v = flag('9666', 'profile_scraping_privacy_token_in_photo_iq')
        if v is not None:
            server_props['profilePicPrivacyToken'] = v
        v = flag('14303', 'lid_trusted_token_issue_to_lid')
        if v is not None:
            server_props['lidTrustedTokenIssueToLid'] = v
        logger.debug({'serverProps': server_props}, 'fetched props')
        return props

    def chat_modify(mod: dict, jid: str):
        """modify a chat -- mark unread, read etc. lastMessages must be sorted in reverse chronologically"""
        patch = chat_modification_to_app_patch(mod, jid)
        return app_patch(patch)

    def update_disable_link_previews_privacy(is_previews_disabled: bool):
        """Enable/Disable link preview privacy, not related to baileys link preview generation"""
        return chat_modify({'disableLinkPreviews': {'isPreviewsDisabled': is_previews_disabled}}, '')

    def star(jid: str, messages: list, star_: bool):
        """Star or Unstar a message"""
        return chat_modify({'star': {'messages': messages, 'star': star_}}, jid)

    def add_or_edit_contact(jid: str, contact: dict):
        return chat_modify({'contact': contact}, jid)

    def remove_contact(jid: str):
        return chat_modify({'contact': None}, jid)

    def add_label(jid: str, labels: dict):
        return chat_modify({'addLabel': {**labels}}, jid)

    def add_chat_label(jid: str, label_id: str):
        return chat_modify({'addChatLabel': {'labelId': label_id}}, jid)

    def remove_chat_label(jid: str, label_id: str):
        return chat_modify({'removeChatLabel': {'labelId': label_id}}, jid)

    def add_message_label(jid: str, message_id: str, label_id: str):
        return chat_modify({'addMessageLabel': {'messageId': message_id, 'labelId': label_id}}, jid)

    def remove_message_label(jid: str, message_id: str, label_id: str):
        return chat_modify({'removeMessageLabel': {'messageId': message_id, 'labelId': label_id}}, jid)

    def add_or_edit_quick_reply(quick_reply: dict):
        """Add or Edit Quick Reply"""
        return chat_modify({'quickReply': quick_reply}, '')

    def remove_quick_reply(timestamp: str):
        """Remove Quick Reply"""
        return chat_modify({'quickReply': {'timestamp': timestamp, 'deleted': True}}, '')

    async def execute_init_queries():
        """queries need to be fired on connection open; help ensure parity with WA Web"""
        await asyncio.gather(fetch_props(), fetch_blocklist(), fetch_privacy_settings())

    async def _upsert_message(msg: dict, type_: str):
        ev.emit('messages.upsert', {'messages': [msg], 'type': type_})
        if msg.get('pushName'):
            jid = auth_state['creds']['me']['id'] if msg['key'].get('fromMe') else \
                (msg['key'].get('participant') or msg['key'].get('remoteJid'))
            jid = jid_normalized_user(jid)
            if not msg['key'].get('fromMe'):
                ev.emit('contacts.update', [{'id': jid, 'notify': msg['pushName'],
                                             'verifiedName': msg.get('verifiedBizName')}])
            # update our pushname too
            if msg['key'].get('fromMe') and msg.get('pushName') and \
                    (auth_state['creds'].get('me') or {}).get('name') != msg['pushName']:
                ev.emit('creds.update', {'me': {**auth_state['creds']['me'], 'name': msg['pushName']}})

        history_msg = get_history_msg(msg.get('message')) if msg.get('message') else None
        should_process_history_msg = (bool(should_sync_history_message(history_msg)) and
                                      history_msg.get('syncType') in PROCESSABLE_HISTORY_TYPES) \
            if history_msg else False
        loop = asyncio.get_running_loop()

        if history_msg and should_process_history_msg:
            sync_type = history_msg.get('syncType')
            # INITIAL_BOOTSTRAP — fire immediately, no progress check (same as WA Web K function)
            if sync_type == proto.HistorySync.INITIAL_BOOTSTRAP and not history_sync_status['initialBootstrapComplete']:
                history_sync_status['initialBootstrapComplete'] = True
                ev.emit('messaging-history.status', {'syncType': sync_type, 'status': 'complete', 'explicit': True})
            # RECENT with progress === 100 — explicit completion
            if sync_type == proto.HistorySync.RECENT and history_msg.get('progress') == 100 \
                    and not history_sync_status['recentSyncComplete']:
                history_sync_status['recentSyncComplete'] = True
                if st['historySyncPausedTimeout']:
                    st['historySyncPausedTimeout'].cancel()
                st['historySyncPausedTimeout'] = None
                ev.emit('messaging-history.status', {'syncType': sync_type, 'status': 'complete', 'explicit': True})
            # Reset 120s paused timeout on any RECENT chunk (like WA Web's handleChunkProgress)
            if sync_type == proto.HistorySync.RECENT and not history_sync_status['recentSyncComplete']:
                if st['historySyncPausedTimeout']:
                    st['historySyncPausedTimeout'].cancel()

                def paused():
                    if not history_sync_status['recentSyncComplete']:
                        history_sync_status['recentSyncComplete'] = True
                        ev.emit('messaging-history.status', {'syncType': proto.HistorySync.RECENT,
                                                             'status': 'paused', 'explicit': False})
                    st['historySyncPausedTimeout'] = None
                st['historySyncPausedTimeout'] = loop.call_later(HISTORY_SYNC_PAUSED_TIMEOUT_MS / 1000, paused)

        # State machine: decide on sync and flush
        if history_msg and st['syncState'] == SyncState.AwaitingInitialSync:
            if st['awaitingSyncTimeout']:
                st['awaitingSyncTimeout'].cancel()
                st['awaitingSyncTimeout'] = None
            if should_process_history_msg:
                st['syncState'] = SyncState.Syncing
                logger.info('Transitioned to Syncing state')
                # Let doAppStateSync handle the final flush after it's done
            else:
                st['syncState'] = SyncState.Online
                logger.info('History sync skipped, transitioning to Online state and flushing buffer')
                ev.flush()

        async def do_app_state_sync():
            if st['syncState'] == SyncState.Syncing:
                # All collections will be synced, so clear any blocked ones
                blocked_collections.clear()
                logger.info('Doing app state sync')
                await resync_app_state(ALL_WA_PATCH_NAMES, True)
                # Sync is complete, go online and flush everything
                st['syncState'] = SyncState.Online
                logger.info('App state sync complete, transitioning to Online state and flushing buffer')
                ev.flush()
                account_sync_counter = (auth_state['creds'].get('accountSyncCounter') or 0) + 1
                ev.emit('creds.update', {'accountSyncCounter': account_sync_counter})

        async def maybe_sync():
            if should_process_history_msg:
                await do_app_state_sync()
        await asyncio.gather(
            maybe_sync(),
            process_message(msg, should_process_history_msg, auth_state['creds'], auth_state['keys'], ev,
                            signal_repository, get_message, config.get('options'), logger, placeholder_resend_cache))

        # If the app state key arrives and we are waiting to sync, trigger the sync now.
        if ((msg.get('message') or {}).get('protocolMessage') or {}).get('appStateSyncKeyShare') \
                and st['syncState'] == SyncState.Syncing:
            logger.info('App state sync key arrived, triggering app state sync')
            await do_app_state_sync()

    upsert_message = ev.create_buffered_function(_upsert_message)

    ws.on('CB:presence', handle_presence_update)
    ws.on('CB:chatstate', handle_presence_update)

    async def on_dirty(node: dict):
        attrs = get_binary_node_child(node, 'dirty')['attrs']
        type_ = attrs.get('type')
        if type_ == 'account_sync':
            if attrs.get('timestamp'):
                last = auth_state['creds'].get('lastAccountSyncTimestamp')
                if last:
                    await clean_dirty_bits('account_sync', last)
                ev.emit('creds.update', {'lastAccountSyncTimestamp': int(attrs['timestamp'])})
        elif type_ == 'groups':
            pass  # handled in groups.py
        else:
            logger.info({'node': node}, 'received unknown sync')
    ws.on('CB:ib,,dirty', on_dirty)

    def on_connection_update(update: dict):
        connection, received_pending_notifications = update.get('connection'), update.get('receivedPendingNotifications')
        if connection == 'close':
            blocked_collections.clear()
            if st['historySyncPausedTimeout']:
                st['historySyncPausedTimeout'].cancel()
            st['historySyncPausedTimeout'] = None
        if connection == 'open':
            if fire_init_queries:
                async def init():
                    try:
                        await execute_init_queries()
                    except Exception as error:
                        on_unexpected_error(error, 'init queries')
                asyncio.ensure_future(init())

            async def presence():
                try:
                    await send_presence_update('available' if mark_online_on_connect else 'unavailable')
                except Exception as error:
                    on_unexpected_error(error, 'presence update requests')
            asyncio.ensure_future(presence())

        if not received_pending_notifications or st['syncState'] != SyncState.Connecting:
            return
        history_sync_status['initialBootstrapComplete'] = False
        history_sync_status['recentSyncComplete'] = False
        if st['historySyncPausedTimeout']:
            st['historySyncPausedTimeout'].cancel()
        st['historySyncPausedTimeout'] = None

        st['syncState'] = SyncState.AwaitingInitialSync
        logger.info('Connection is now AwaitingInitialSync, buffering events')
        ev.buffer()

        will_sync_history = should_sync_history_message({'syncType': proto.HistorySync.RECENT})
        loop = asyncio.get_running_loop()
        if not will_sync_history:
            logger.info('History sync is disabled by config, not waiting for notification. Transitioning to Online.')
            st['syncState'] = SyncState.Online
            loop.call_soon(ev.flush)
            return

        # On reconnection (accountSyncCounter > 0), the server does not push history sync notifications —
        # the device already has its data. Skip the 20s wait and go online immediately.
        if (auth_state['creds'].get('accountSyncCounter') or 0) > 0:
            logger.info('Reconnection with existing sync data, skipping history sync wait. Transitioning to Online.')
            st['syncState'] = SyncState.Online
            loop.call_soon(ev.flush)
            return

        logger.info('First connection, awaiting history sync notification with a 20s timeout.')
        if st['awaitingSyncTimeout']:
            st['awaitingSyncTimeout'].cancel()

        def on_timeout():
            if st['syncState'] == SyncState.AwaitingInitialSync:
                logger.warn('Timeout in AwaitingInitialSync, forcing state to Online and flushing buffer')
                st['syncState'] = SyncState.Online
                ev.flush()
                # Increment so subsequent reconnections skip the 20s wait.
                account_sync_counter = (auth_state['creds'].get('accountSyncCounter') or 0) + 1
                ev.emit('creds.update', {'accountSyncCounter': account_sync_counter})
        st['awaitingSyncTimeout'] = loop.call_later(20, on_timeout)
    ev.on('connection.update', on_connection_update)

    # When an app state sync key arrives (myAppStateKeyId is set) and there are collections blocked on a missing
    # key, trigger a re-sync for just those collections. Mirrors WA Web's Blocked → retry-on-key-arrival behavior.
    def on_creds_update_key(update: dict):
        if not update.get('myAppStateKeyId') or len(blocked_collections) == 0:
            return
        # If we're in the middle of a full sync, doAppStateSync handles all collections
        if st['syncState'] == SyncState.Syncing:
            blocked_collections.clear()
            return
        collections = list(blocked_collections)
        blocked_collections.clear()
        logger.info({'collections': collections}, 'app state sync key arrived, re-syncing blocked collections')

        async def go():
            try:
                await resync_app_state(collections, False)
            except Exception as error:
                on_unexpected_error(error, 'blocked collections resync')
        asyncio.ensure_future(go())
    ev.on('creds.update', on_creds_update_key)

    async def on_lid_mapping_update(update: dict):
        lid, pn = update['lid'], update['pn']
        try:
            await signal_repository.lid_mapping.store_lid_pn_mappings([{'lid': lid, 'pn': pn}])
        except Exception as error:
            logger.warn({'lid': lid, 'pn': pn, 'error': error}, 'Failed to store LID-PN mapping')
    ev.on('lid-mapping.update', on_lid_mapping_update)

    def on_socket_end(_error=None):
        if st['awaitingSyncTimeout']:
            st['awaitingSyncTimeout'].cancel()
            st['awaitingSyncTimeout'] = None
        if not config.get('placeholderResendCache') and hasattr(placeholder_resend_cache, 'close'):
            placeholder_resend_cache.close()
        st['syncState'] = SyncState.Connecting
        st['privacySettings'] = None
    register_socket_end_handler(on_socket_end)

    return sock.extend(
        server_props=server_props, create_call_link=create_call_link, get_bot_list_v2=get_bot_list_v2,
        message_mutex=message_mutex, receipt_mutex=receipt_mutex, app_state_patch_mutex=app_state_patch_mutex,
        notification_mutex=notification_mutex, fetch_privacy_settings=fetch_privacy_settings,
        upsert_message=upsert_message, app_patch=app_patch, send_presence_update=send_presence_update,
        presence_subscribe=presence_subscribe, profile_picture_url=profile_picture_url,
        fetch_blocklist=fetch_blocklist, fetch_status=fetch_status,
        fetch_disappearing_duration=fetch_disappearing_duration, update_profile_picture=update_profile_picture,
        remove_profile_picture=remove_profile_picture, update_profile_status=update_profile_status,
        update_profile_name=update_profile_name, update_block_status=update_block_status,
        update_disable_link_previews_privacy=update_disable_link_previews_privacy,
        update_call_privacy=update_call_privacy, update_messages_privacy=update_messages_privacy,
        update_last_seen_privacy=update_last_seen_privacy, update_online_privacy=update_online_privacy,
        update_profile_picture_privacy=update_profile_picture_privacy, update_status_privacy=update_status_privacy,
        update_read_receipts_privacy=update_read_receipts_privacy, update_groups_add_privacy=update_groups_add_privacy,
        update_default_disappearing_mode=update_default_disappearing_mode,
        get_business_profile=get_business_profile, resync_app_state=resync_app_state, chat_modify=chat_modify,
        clean_dirty_bits=clean_dirty_bits, add_or_edit_contact=add_or_edit_contact, remove_contact=remove_contact,
        placeholder_resend_cache=placeholder_resend_cache, add_label=add_label, add_chat_label=add_chat_label,
        remove_chat_label=remove_chat_label, add_message_label=add_message_label,
        remove_message_label=remove_message_label, star=star, add_or_edit_quick_reply=add_or_edit_quick_reply,
        remove_quick_reply=remove_quick_reply)
