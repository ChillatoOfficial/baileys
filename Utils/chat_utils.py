from __future__ import annotations
import asyncio
import base64
import copy
import json
import time
from typing import Callable, Optional

from ..Types import LabelAssociationType
from ..WABinary import (Boom, get_binary_node_child, get_binary_node_children, is_jid_group, jid_normalized_user)
from ..WAProto import WAProto_pb2 as proto
from .crypto import aes_decrypt, aes_encrypt, hkdf, hmac_sign
from .generics import to_number
from .lt_hash import LT_HASH_ANTI_TAMPERING
from .messages_media import download_content_from_message, to_buffer
from .proto_utils import from_object, to_dict
from .sync_action_utils import emit_sync_action_results, process_contact_action

SET = proto.SyncdMutation.SET
REMOVE = proto.SyncdMutation.REMOVE


def expand_app_state_keys(key_data: bytes) -> dict:
    keys = hkdf(bytes(key_data), 160, info='WhatsApp Mutation Keys')
    return {'indexKey': keys[0:32], 'valueEncryptionKey': keys[32:64], 'valueMacKey': keys[64:96],
            'snapshotMacKey': keys[96:128], 'patchMacKey': keys[128:160]}


def _mutation_keys(key_data: bytes) -> dict:
    return expand_app_state_keys(key_data)


def _generate_mac(operation: int, data: bytes, key_id, key: bytes) -> bytes:
    op_byte = 0x01 if operation == SET else 0x02
    key_id_buffer = base64.b64decode(key_id) if isinstance(key_id, str) else bytes(key_id)
    key_data = bytes([op_byte]) + key_id_buffer
    last = bytes(7) + bytes([len(key_data)])
    total = key_data + bytes(data) + last
    return hmac_sign(total, key, 'sha512')[:32]


def _to_64_bit_network_order(e: int) -> bytes:
    return (e & 0xFFFFFFFF).to_bytes(8, 'big')


def make_lt_hash_generator(state: dict) -> dict:
    index_value_map = dict(state['indexValueMap'])
    hash_ = state['hash']
    add_buffs, sub_buffs = [], []

    def mix(indexMac, valueMac, operation):
        index_mac_b64 = base64.b64encode(bytes(indexMac)).decode()
        prev_op = index_value_map.get(index_mac_b64)
        if operation == REMOVE:
            if not prev_op:
                # WA Web does not throw here — it logs a warning and skips the subtract.
                return
            del index_value_map[index_mac_b64]
        else:
            add_buffs.append(valueMac)
            index_value_map[index_mac_b64] = {'valueMac': valueMac}
        if prev_op:
            sub_buffs.append(prev_op['valueMac'])

    def finish():
        result = LT_HASH_ANTI_TAMPERING.subtract_then_add(hash_, sub_buffs, add_buffs)
        return {'hash': bytes(result), 'indexValueMap': index_value_map}
    return {'mix': mix, 'finish': finish}


def _generate_snapshot_mac(lthash: bytes, version: int, name: str, key: bytes) -> bytes:
    return hmac_sign(bytes(lthash) + _to_64_bit_network_order(version) + name.encode(), key, 'sha256')


def _generate_patch_mac(snapshot_mac: bytes, value_macs: list, version: int, type_: str, key: bytes) -> bytes:
    total = bytes(snapshot_mac) + b''.join(bytes(v) for v in value_macs) + _to_64_bit_network_order(version) + \
        type_.encode()
    return hmac_sign(total, key)


def new_lt_hash_state() -> dict:
    return {'version': 0, 'hash': bytes(128), 'indexValueMap': {}}


def ensure_lt_hash_state_version(state: dict) -> dict:
    v = state.get('version')
    if not isinstance(v, (int, float)) or v != v:
        state['version'] = 0
    return state


MAX_SYNC_ATTEMPTS = 2


def is_missing_key_error(error) -> bool:
    """WA Web treats missing app state sync keys as "Blocked" (waits for key arrival), not fatal."""
    data = getattr(error, 'data', None)
    return isinstance(data, dict) and data.get('isMissingKey') is True


def is_app_state_sync_irrecoverable(error, attempts: int) -> bool:
    return attempts >= MAX_SYNC_ATTEMPTS or isinstance(error, TypeError)


async def encode_syncd_patch(patch_create: dict, my_app_state_key_id: str, state: dict, get_app_state_sync_key):
    type_, index, sync_action = patch_create['type'], patch_create['index'], patch_create['syncAction']
    api_version, operation = patch_create['apiVersion'], patch_create['operation']
    key = await get_app_state_sync_key(my_app_state_key_id) if my_app_state_key_id else None
    if not key:
        raise Boom(f'myAppStateKey ("{my_app_state_key_id}") not present', data={'isMissingKey': True})
    enc_key_id = base64.b64decode(my_app_state_key_id)
    state = {**state, 'indexValueMap': dict(state['indexValueMap'])}
    index_buffer = json.dumps(index, separators=(',', ':')).encode()
    data_proto = from_object(proto.SyncActionData, {'index': index_buffer, 'value': sync_action,
                                                    'padding': b'', 'version': api_version})
    encoded = data_proto.SerializeToString()
    key_value = _mutation_keys(key['keyData'] if isinstance(key, dict) else key.keyData)
    enc_value = aes_encrypt(encoded, key_value['valueEncryptionKey'])
    value_mac = _generate_mac(operation, enc_value, enc_key_id, key_value['valueMacKey'])
    index_mac = hmac_sign(index_buffer, key_value['indexKey'])
    generator = make_lt_hash_generator(state)
    generator['mix'](index_mac, value_mac, operation)
    state.update(generator['finish']())
    state['version'] += 1
    snapshot_mac = _generate_snapshot_mac(state['hash'], state['version'], type_, key_value['snapshotMacKey'])
    patch = {
        'patchMac': _generate_patch_mac(snapshot_mac, [value_mac], state['version'], type_, key_value['patchMacKey']),
        'snapshotMac': snapshot_mac,
        'keyId': {'id': enc_key_id},
        'mutations': [{'operation': operation, 'record': {
            'index': {'blob': index_mac}, 'value': {'blob': enc_value + value_mac}, 'keyId': {'id': enc_key_id}}}],
    }
    state['indexValueMap'][base64.b64encode(index_mac).decode()] = {'valueMac': value_mac}
    return {'patch': patch, 'state': state}


async def decode_syncd_mutations(msg_mutations: list, initial_state: dict, get_app_state_sync_key,
                                 on_mutation: Callable, validate_macs: bool) -> dict:
    lt_generator = make_lt_hash_generator(initial_state)
    derived_key_cache: dict = {}

    async def get_key(key_id: bytes):
        base64_key = base64.b64encode(bytes(key_id)).decode()
        if base64_key in derived_key_cache:
            return derived_key_cache[base64_key]
        key_enc = await get_app_state_sync_key(base64_key)
        if not key_enc:
            raise Boom(f'failed to find key "{base64_key}" to decode mutation',
                       data={'isMissingKey': True, 'msgMutations': msg_mutations})
        keys = _mutation_keys(key_enc['keyData'] if isinstance(key_enc, dict) else key_enc.keyData)
        derived_key_cache[base64_key] = keys
        return keys

    # indexKey used to HMAC sign record.index.blob; valueEncryptionKey used to AES-256-CBC encrypt
    # record.value.blob[0:-32]; the remaining bytes are the mac
    for msg_mutation in msg_mutations:
        # if it's a syncdmutation, get the operation property; otherwise it's only a record -- a SET mutation
        operation = msg_mutation['operation'] if 'operation' in msg_mutation else SET
        record = msg_mutation['record'] if msg_mutation.get('record') else msg_mutation
        try:
            key = await get_key(record['keyId']['id'])
        except Exception as err:
            # Missing-key errors must propagate so the orchestrator can park the collection (Blocked)
            if is_missing_key_error(err):
                raise
            continue
        content = bytes(record['value']['blob'])
        enc_content, og_value_mac = content[:-32], content[-32:]
        if validate_macs:
            content_hmac = _generate_mac(operation, enc_content, record['keyId']['id'], key['valueMacKey'])
            if content_hmac != og_value_mac:
                continue  # HMAC verification failed — skip this record
        try:
            result = aes_decrypt(enc_content, key['valueEncryptionKey'])
        except Exception:
            continue  # decrypt failed — skip this record instead of aborting
        sync_action = to_dict(_decode(proto.SyncActionData, result))
        if validate_macs:
            hmac = hmac_sign(sync_action['index'], key['indexKey'])
            if hmac != bytes(record['index']['blob']):
                raise Boom('HMAC index verification failed')
        index_str = bytes(sync_action['index']).decode('utf-8')
        on_mutation({'syncAction': sync_action, 'index': json.loads(index_str)})
        lt_generator['mix'](record['index']['blob'], og_value_mac, operation)
    return lt_generator['finish']()


def _decode(cls, data):
    m = cls()
    m.ParseFromString(bytes(data))
    return m


async def decode_syncd_patch(msg: dict, name: str, initial_state: dict, get_app_state_sync_key,
                             on_mutation: Callable, validate_macs: bool) -> dict:
    if validate_macs:
        base64_key = base64.b64encode(bytes(msg['keyId']['id'])).decode()
        main_key_obj = await get_app_state_sync_key(base64_key)
        if not main_key_obj:
            raise Boom(f'failed to find key "{base64_key}" to decode patch', data={'isMissingKey': True, 'msg': msg})
        main_key = _mutation_keys(main_key_obj['keyData'] if isinstance(main_key_obj, dict) else main_key_obj.keyData)
        mutation_macs = [bytes(m['record']['value']['blob'])[-32:] for m in msg.get('mutations', [])]
        patch_mac = _generate_patch_mac(msg['snapshotMac'], mutation_macs, to_number(msg['version']['version']), name,
                                        main_key['patchMacKey'])
        if patch_mac != bytes(msg['patchMac']):
            raise Boom('Invalid patch mac')
    return await decode_syncd_mutations(msg.get('mutations', []), initial_state, get_app_state_sync_key, on_mutation,
                                        validate_macs)


async def extract_syncd_patches(result: dict, options: Optional[dict] = None) -> dict:
    sync_node = get_binary_node_child(result, 'sync')
    collection_nodes = get_binary_node_children(sync_node, 'collection')
    final = {}

    async def one(collection_node):
        patches_node = get_binary_node_child(collection_node, 'patches')
        patches = get_binary_node_children(patches_node or collection_node, 'patch')
        snapshot_node = get_binary_node_child(collection_node, 'snapshot')
        syncds = []
        name = collection_node['attrs']['name']
        has_more_patches = collection_node['attrs'].get('has_more_patches') == 'true'
        snapshot = None
        if snapshot_node and snapshot_node.get('content'):
            blob_ref = to_dict(_decode(proto.ExternalBlobReference, snapshot_node['content']))
            data = await download_external_blob(blob_ref, options)
            snapshot = to_dict(_decode(proto.SyncdSnapshot, data))
        for p in patches:
            content = p.get('content')
            if content:
                syncd = to_dict(_decode(proto.SyncdPatch, content))
                if not syncd.get('version'):
                    syncd['version'] = {'version': int(collection_node['attrs'].get('version') or 0) + 1}
                syncds.append(syncd)
        final[name] = {'patches': syncds, 'hasMorePatches': has_more_patches, 'snapshot': snapshot}

    await asyncio.gather(*[one(c) for c in collection_nodes])
    return final


async def download_external_blob(blob: dict, options: Optional[dict] = None) -> bytes:
    stream = await download_content_from_message(blob, 'md-app-state', {'options': options or {}})
    return await to_buffer(stream)


async def download_external_patch(blob: dict, options: Optional[dict] = None) -> dict:
    buffer = await download_external_blob(blob, options)
    return to_dict(_decode(proto.SyncdMutations, buffer))


async def decode_syncd_snapshot(name: str, snapshot: dict, get_app_state_sync_key, minimum_version_number,
                                validate_macs: bool = True, logger=None) -> dict:
    new_state = new_lt_hash_state()
    new_state['version'] = to_number(snapshot['version']['version'])
    mutation_map = {}
    are_mutations_required = minimum_version_number is None or new_state['version'] > minimum_version_number

    def on_mut(mutation):
        index = bytes(mutation['syncAction']['index']).decode('utf-8', errors='replace')
        mutation_map[index] = mutation

    r = await decode_syncd_mutations(snapshot.get('records', []), new_state, get_app_state_sync_key,
                                     on_mut if are_mutations_required else (lambda m: None), validate_macs)
    new_state['hash'] = r['hash']
    new_state['indexValueMap'] = r['indexValueMap']
    if validate_macs:
        base64_key = base64.b64encode(bytes(snapshot['keyId']['id'])).decode()
        key_enc = await get_app_state_sync_key(base64_key)
        if not key_enc:
            raise Boom(f'failed to find key "{base64_key}" to decode mutation', data={'isMissingKey': True})
        result = _mutation_keys(key_enc['keyData'] if isinstance(key_enc, dict) else key_enc.keyData)
        computed = _generate_snapshot_mac(new_state['hash'], new_state['version'], name, result['snapshotMacKey'])
        if bytes(snapshot['mac']) != computed and logger:
            logger.warn({'name': name, 'version': new_state['version']},
                        'LTHash verification failed on snapshot, continuing with partial state')
    return {'state': new_state, 'mutationMap': mutation_map}


async def decode_patches(name: str, syncds: list, initial: dict, get_app_state_sync_key, options: Optional[dict],
                         minimum_version_number=None, logger=None, validate_macs: bool = True) -> dict:
    new_state = {**initial, 'indexValueMap': dict(initial['indexValueMap'])}
    mutation_map = {}
    for syncd in syncds:
        version, key_id, snapshot_mac = syncd.get('version'), syncd.get('keyId'), syncd.get('snapshotMac')
        if syncd.get('externalMutations'):
            if logger:
                logger.trace({'name': name, 'version': version}, 'downloading external patch')
            ref = await download_external_patch(syncd['externalMutations'], options)
            if logger:
                logger.debug({'name': name, 'version': version, 'mutations': len(ref.get('mutations', []))},
                             'downloaded external patch')
            syncd.setdefault('mutations', []).extend(ref.get('mutations', []))
        patch_version = to_number(version['version'])
        new_state['version'] = patch_version
        should_mutate = minimum_version_number is None or patch_version > minimum_version_number

        def on_mut(mutation):
            index = bytes(mutation['syncAction']['index']).decode('utf-8', errors='replace')
            mutation_map[index] = mutation

        try:
            decode_result = await decode_syncd_patch(syncd, name, new_state, get_app_state_sync_key,
                                                     on_mut if should_mutate else (lambda m: None), validate_macs)
        except Exception as err:
            if is_missing_key_error(err):
                raise
            if logger:
                logger.warn({'name': name, 'version': patch_version, 'error': str(err)},
                            'failed to decode patch, skipping')
            continue
        new_state['hash'] = decode_result['hash']
        new_state['indexValueMap'] = decode_result['indexValueMap']
        if validate_macs:
            base64_key = base64.b64encode(bytes(key_id['id'])).decode()
            key_enc = await get_app_state_sync_key(base64_key)
            if not key_enc:
                raise Boom(f'failed to find key "{base64_key}" to decode mutation', data={'isMissingKey': True})
            result = _mutation_keys(key_enc['keyData'] if isinstance(key_enc, dict) else key_enc.keyData)
            computed = _generate_snapshot_mac(new_state['hash'], new_state['version'], name, result['snapshotMacKey'])
            if bytes(snapshot_mac) != computed:
                if logger:
                    logger.warn({'name': name, 'version': new_state['version']},
                                'LTHash verification failed, skipping remaining patches')
                break
        syncd['mutations'] = []  # clear memory used up by the mutations
    return {'state': new_state, 'mutationMap': mutation_map}


def chat_modification_to_app_patch(mod: dict, jid: str) -> dict:
    def get_message_range(last_messages):
        if isinstance(last_messages, list):
            last_msg = last_messages[-1] if last_messages else None
            msgs = None
            if last_messages:
                msgs = []
                for m in last_messages:
                    k = m.get('key') or {}
                    if not k.get('id') or not k.get('remoteJid'):
                        raise Boom('Incomplete key', status_code=400, data=m)
                    if is_jid_group(k['remoteJid']) and not k.get('fromMe') and not k.get('participant'):
                        raise Boom('Expected not from me message to have participant', status_code=400, data=m)
                    if not m.get('messageTimestamp') or not to_number(m['messageTimestamp']):
                        raise Boom('Missing timestamp in last message list', status_code=400, data=m)
                    if k.get('participant'):
                        k['participant'] = jid_normalized_user(k['participant'])
                    msgs.append(m)
            return {'lastMessageTimestamp': last_msg.get('messageTimestamp') if last_msg else None, 'messages': msgs}
        return last_messages

    if 'mute' in mod:
        patch = {'syncAction': {'muteAction': {'muted': bool(mod['mute']), 'muteEndTimestamp': mod['mute'] or None}},
                 'index': ['mute', jid], 'type': 'regular_high', 'apiVersion': 2, 'operation': SET}
    elif 'archive' in mod:
        patch = {'syncAction': {'archiveChatAction': {'archived': bool(mod['archive']),
                                                      'messageRange': get_message_range(mod.get('lastMessages'))}},
                 'index': ['archive', jid], 'type': 'regular_low', 'apiVersion': 3, 'operation': SET}
    elif 'markRead' in mod:
        patch = {'syncAction': {'markChatAsReadAction': {'read': mod['markRead'],
                                                         'messageRange': get_message_range(mod.get('lastMessages'))}},
                 'index': ['markChatAsRead', jid], 'type': 'regular_low', 'apiVersion': 3, 'operation': SET}
    elif 'deleteForMe' in mod:
        d = mod['deleteForMe']
        key = d['key']
        patch = {'syncAction': {'deleteMessageForMeAction': {'deleteMedia': d.get('deleteMedia'),
                                                             'messageTimestamp': d.get('timestamp')}},
                 'index': ['deleteMessageForMe', jid, key['id'], '1' if key.get('fromMe') else '0', '0'],
                 'type': 'regular_high', 'apiVersion': 3, 'operation': SET}
    elif 'clear' in mod:
        patch = {'syncAction': {'clearChatAction': {'messageRange': get_message_range(mod.get('lastMessages'))}},
                 'index': ['clearChat', jid, '1', '0'],  # the option here is 0 when keep starred messages is enabled
                 'type': 'regular_high', 'apiVersion': 6, 'operation': SET}
    elif 'pin' in mod:
        patch = {'syncAction': {'pinAction': {'pinned': bool(mod['pin'])}}, 'index': ['pin_v1', jid],
                 'type': 'regular_low', 'apiVersion': 5, 'operation': SET}
    elif 'contact' in mod:
        patch = {'syncAction': {'contactAction': mod['contact'] or {}}, 'index': ['contact', jid],
                 'type': 'critical_unblock_low', 'apiVersion': 2, 'operation': SET if mod['contact'] else REMOVE}
    elif 'disableLinkPreviews' in mod:
        patch = {'syncAction': {'privacySettingDisableLinkPreviewsAction': mod['disableLinkPreviews'] or {}},
                 'index': ['setting_disableLinkPreviews'], 'type': 'regular', 'apiVersion': 8, 'operation': SET}
    elif 'star' in mod:
        key = mod['star']['messages'][0]
        patch = {'syncAction': {'starAction': {'starred': bool(mod['star']['star'])}},
                 'index': ['star', jid, key['id'], '1' if key.get('fromMe') else '0', '0'],
                 'type': 'regular_low', 'apiVersion': 2, 'operation': SET}
    elif 'delete' in mod:
        patch = {'syncAction': {'deleteChatAction': {'messageRange': get_message_range(mod.get('lastMessages'))}},
                 'index': ['deleteChat', jid, '1'], 'type': 'regular_high', 'apiVersion': 6, 'operation': SET}
    elif 'pushNameSetting' in mod:
        patch = {'syncAction': {'pushNameSetting': {'name': mod['pushNameSetting']}},
                 'index': ['setting_pushName'], 'type': 'critical_block', 'apiVersion': 1, 'operation': SET}
    elif 'quickReply' in mod:
        q = mod['quickReply']
        patch = {'syncAction': {'quickReplyAction': {'count': 0, 'deleted': q.get('deleted') or False, 'keywords': [],
                                                     'message': q.get('message') or '',
                                                     'shortcut': q.get('shortcut') or ''}},
                 'index': ['quick_reply', q.get('timestamp') or str(int(time.time()))],
                 'type': 'regular', 'apiVersion': 2, 'operation': SET}
    elif 'addLabel' in mod:
        l = mod['addLabel']
        patch = {'syncAction': {'labelEditAction': {'name': l.get('name'), 'color': l.get('color'),
                                                    'predefinedId': l.get('predefinedId'),
                                                    'deleted': l.get('deleted')}},
                 'index': ['label_edit', l['id']], 'type': 'regular', 'apiVersion': 3, 'operation': SET}
    elif 'addChatLabel' in mod:
        patch = {'syncAction': {'labelAssociationAction': {'labeled': True}},
                 'index': [LabelAssociationType.Chat.value, mod['addChatLabel']['labelId'], jid],
                 'type': 'regular', 'apiVersion': 3, 'operation': SET}
    elif 'removeChatLabel' in mod:
        patch = {'syncAction': {'labelAssociationAction': {'labeled': False}},
                 'index': [LabelAssociationType.Chat.value, mod['removeChatLabel']['labelId'], jid],
                 'type': 'regular', 'apiVersion': 3, 'operation': SET}
    elif 'addMessageLabel' in mod:
        patch = {'syncAction': {'labelAssociationAction': {'labeled': True}},
                 'index': [LabelAssociationType.Message.value, mod['addMessageLabel']['labelId'], jid,
                           mod['addMessageLabel']['messageId'], '0', '0'],
                 'type': 'regular', 'apiVersion': 3, 'operation': SET}
    elif 'removeMessageLabel' in mod:
        patch = {'syncAction': {'labelAssociationAction': {'labeled': False}},
                 'index': [LabelAssociationType.Message.value, mod['removeMessageLabel']['labelId'], jid,
                           mod['removeMessageLabel']['messageId'], '0', '0'],
                 'type': 'regular', 'apiVersion': 3, 'operation': SET}
    else:
        raise Boom('not supported')
    patch['syncAction']['timestamp'] = int(time.time() * 1000)
    return patch


def process_sync_action(sync_action: dict, ev, me: Optional[dict], initial_sync_opts: Optional[dict] = None,
                        logger=None):
    is_initial_sync = bool(initial_sync_opts)
    account_settings = (initial_sync_opts or {}).get('accountSettings')
    if logger:
        logger.trace({'syncAction': sync_action, 'initialSync': bool(initial_sync_opts)}, 'processing sync action')
    action = (sync_action['syncAction'] or {}).get('value') or {}
    index = sync_action['index']
    type_ = index[0] if len(index) > 0 else None
    id_ = index[1] if len(index) > 1 else None
    msg_id = index[2] if len(index) > 2 else None
    from_me = index[3] if len(index) > 3 else None

    def is_valid_patch_based_on_message_range(chat: dict, msg_range) -> bool:
        last = int((msg_range or {}).get('lastMessageTimestamp') or (msg_range or {}).get('lastSystemMessageTimestamp') or 0)
        chat_last = int(chat.get('lastMessageRecvTimestamp') or 0)
        return last >= chat_last

    def get_chat_update_conditional(cid: str, msg_range):
        if not is_initial_sync:
            return None

        def cond(data):
            chat = data['historySets']['chats'].get(cid) or data['chatUpserts'].get(cid)
            if chat:
                return is_valid_patch_based_on_message_range(chat, msg_range) if msg_range else True
            return None
        return cond

    if action.get('muteAction'):
        ev.emit('chats.update', [{
            'id': id_,
            'muteEndTime': to_number(action['muteAction'].get('muteEndTimestamp')) if action['muteAction'].get('muted') else None,
            'conditional': get_chat_update_conditional(id_, None)}])
    elif action.get('archiveChatAction') or type_ in ('archive', 'unarchive'):
        archive_action = action.get('archiveChatAction')
        is_archived = archive_action.get('archived') if archive_action else type_ == 'archive'
        msg_range = None if not (account_settings or {}).get('unarchiveChats') else \
            (archive_action or {}).get('messageRange')
        ev.emit('chats.update', [{'id': id_, 'archived': is_archived,
                                  'conditional': get_chat_update_conditional(id_, msg_range)}])
    elif action.get('markChatAsReadAction'):
        mark = action['markChatAsReadAction']
        is_null_update = is_initial_sync and mark.get('read')
        ev.emit('chats.update', [{
            'id': id_, 'unreadCount': None if is_null_update else (0 if mark.get('read') else -1),
            'conditional': get_chat_update_conditional(id_, mark.get('messageRange'))}])
    elif action.get('deleteMessageForMeAction') or type_ == 'deleteMessageForMe':
        ev.emit('messages.delete', {'keys': [{'remoteJid': id_, 'id': msg_id, 'fromMe': from_me == '1'}]})
    elif action.get('contactAction'):
        results = process_contact_action(action['contactAction'], id_, logger)
        emit_sync_action_results(ev, results)
    elif action.get('pushNameSetting'):
        name = action['pushNameSetting'].get('name')
        if name and (me or {}).get('name') != name:
            ev.emit('creds.update', {'me': {**(me or {}), 'name': name}})
    elif action.get('pinAction'):
        ev.emit('chats.update', [{
            'id': id_, 'pinned': to_number(action.get('timestamp')) if action['pinAction'].get('pinned') else None,
            'conditional': get_chat_update_conditional(id_, None)}])
    elif action.get('unarchiveChatsSetting'):
        unarchive_chats = bool(action['unarchiveChatsSetting'].get('unarchiveChats'))
        ev.emit('creds.update', {'accountSettings': {'unarchiveChats': unarchive_chats}})
        if logger:
            logger.info(f"archive setting updated => '{action['unarchiveChatsSetting'].get('unarchiveChats')}'")
        if account_settings is not None:
            account_settings['unarchiveChats'] = unarchive_chats
    elif action.get('starAction') or type_ == 'star':
        starred = (action.get('starAction') or {}).get('starred')
        if not isinstance(starred, bool):
            starred = index[-1] == '1'
        ev.emit('messages.update', [{'key': {'remoteJid': id_, 'id': msg_id, 'fromMe': from_me == '1'},
                                     'update': {'starred': starred}}])
    elif action.get('deleteChatAction') or type_ == 'deleteChat':
        if not is_initial_sync:
            ev.emit('chats.delete', [id_])
    elif action.get('labelEditAction'):
        l = action['labelEditAction']
        ev.emit('labels.edit', {'id': id_, 'name': l.get('name'), 'color': l.get('color'),
                                'deleted': l.get('deleted'),
                                'predefinedId': str(l['predefinedId']) if l.get('predefinedId') else None})
    elif action.get('labelAssociationAction'):
        if type_ == LabelAssociationType.Chat.value:
            association = {'type': LabelAssociationType.Chat.value, 'chatId': index[2], 'labelId': index[1]}
        else:
            association = {'type': LabelAssociationType.Message.value, 'chatId': index[2],
                           'messageId': index[3], 'labelId': index[1]}
        ev.emit('labels.association', {'type': 'add' if action['labelAssociationAction'].get('labeled') else 'remove',
                                       'association': association})
    elif (action.get('localeSetting') or {}).get('locale'):
        ev.emit('settings.update', {'setting': 'locale', 'value': action['localeSetting']['locale']})
    elif action.get('timeFormatAction'):
        ev.emit('settings.update', {'setting': 'timeFormat', 'value': action['timeFormatAction']})
    elif action.get('pnForLidChatAction'):
        if action['pnForLidChatAction'].get('pnJid'):
            ev.emit('lid-mapping.update', {'lid': id_, 'pn': action['pnForLidChatAction']['pnJid']})
    elif action.get('privacySettingRelayAllCalls'):
        ev.emit('settings.update', {'setting': 'privacySettingRelayAllCalls',
                                    'value': action['privacySettingRelayAllCalls']})
    elif action.get('statusPrivacy'):
        ev.emit('settings.update', {'setting': 'statusPrivacy', 'value': action['statusPrivacy']})
    elif action.get('lockChatAction'):
        ev.emit('chats.lock', {'id': id_, 'locked': bool(action['lockChatAction'].get('locked'))})
    elif action.get('privacySettingDisableLinkPreviewsAction'):
        ev.emit('settings.update', {'setting': 'disableLinkPreviews',
                                    'value': action['privacySettingDisableLinkPreviewsAction']})
    elif (action.get('notificationActivitySettingAction') or {}).get('notificationActivitySetting'):
        ev.emit('settings.update', {'setting': 'notificationActivitySetting',
                                    'value': action['notificationActivitySettingAction']['notificationActivitySetting']})
    elif action.get('lidContactAction'):
        lc = action['lidContactAction']
        ev.emit('contacts.upsert', [{'id': id_, 'name': lc.get('fullName') or lc.get('firstName') or lc.get('username') or None,
                                     'username': lc.get('username') or None, 'lid': id_, 'phoneNumber': None}])
    elif action.get('privacySettingChannelsPersonalisedRecommendationAction'):
        ev.emit('settings.update', {'setting': 'channelsPersonalisedRecommendation',
                                    'value': action['privacySettingChannelsPersonalisedRecommendationAction']})
    elif logger:
        logger.debug({'syncAction': sync_action, 'id': id_}, 'unprocessable update')
