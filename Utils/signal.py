from __future__ import annotations
from ..Defaults import KEY_BUNDLE_TYPE
from ..WABinary import (assert_node_error_free, get_binary_node_child, get_binary_node_child_buffer,
                        get_binary_node_children, get_binary_node_child_uint, get_server_from_domain_type,
                        jid_decode, S_WHATSAPP_NET, WAJIDDomains, FullJid)
from .crypto import Curve, generate_signal_pub_key
from .generics import encode_big_endian
from .auth_utils import maybe_await


def _chunk(array: list, size: int) -> list:
    return [array[i:i + size] for i in range(0, len(array), size)]


def create_signal_identity(wid: str, account_signature_key: bytes) -> dict:
    return {'identifier': {'name': wid, 'deviceId': 0},
            'identifierKey': generate_signal_pub_key(account_signature_key)}


async def get_pre_keys(keys, min_: int, limit: int):
    id_list = [str(i) for i in range(min_, limit)]
    return await maybe_await(keys.get('pre-key', id_list))


def generate_or_get_pre_keys(creds: dict, range_: int) -> dict:
    available = creds['nextPreKeyId'] - creds['firstUnuploadedPreKeyId']
    remaining = range_ - available
    last_pre_key_id = creds['nextPreKeyId'] + remaining - 1
    new_pre_keys = {}
    if remaining > 0:
        for i in range(creds['nextPreKeyId'], last_pre_key_id + 1):
            new_pre_keys[i] = Curve.generate_key_pair()
    return {'newPreKeys': new_pre_keys, 'lastPreKeyId': last_pre_key_id,
            'preKeysRange': (creds['firstUnuploadedPreKeyId'], range_)}


def xmpp_signed_pre_key(key: dict) -> dict:
    return {'tag': 'skey', 'attrs': {}, 'content': [
        {'tag': 'id', 'attrs': {}, 'content': encode_big_endian(key['keyId'], 3)},
        {'tag': 'value', 'attrs': {}, 'content': key['keyPair']['public']},
        {'tag': 'signature', 'attrs': {}, 'content': key['signature']},
    ]}


def xmpp_pre_key(pair: dict, id_: int) -> dict:
    return {'tag': 'key', 'attrs': {}, 'content': [
        {'tag': 'id', 'attrs': {}, 'content': encode_big_endian(id_, 3)},
        {'tag': 'value', 'attrs': {}, 'content': pair['public']},
    ]}


def _is_valid_uint(n) -> bool:
    return isinstance(n, int) and not isinstance(n, bool)


def extract_e2e_session_from_retry_receipt(receipt: dict):
    keys_node = get_binary_node_child(receipt, 'keys')
    if not keys_node:
        return None
    type_buf = get_binary_node_child_buffer(keys_node, 'type')
    if not type_buf or len(type_buf) != 1 or type_buf[0] != KEY_BUNDLE_TYPE[0]:
        return None
    identity = get_binary_node_child_buffer(keys_node, 'identity')
    skey = get_binary_node_child(keys_node, 'skey')
    if not identity or len(identity) != 32 or not skey:
        return None
    registration_id = get_binary_node_child_uint(receipt, 'registration', 4)
    if not _is_valid_uint(registration_id):
        return None
    signed_pub_key = get_binary_node_child_buffer(skey, 'value')
    signed_sig = get_binary_node_child_buffer(skey, 'signature')
    signed_key_id = get_binary_node_child_uint(skey, 'id', 3)
    if not signed_pub_key or len(signed_pub_key) != 32 or not signed_sig or not _is_valid_uint(signed_key_id):
        return None
    pre_key = None
    pre_key_node = get_binary_node_child(keys_node, 'key')
    if pre_key_node:
        pub = get_binary_node_child_buffer(pre_key_node, 'value')
        pid = get_binary_node_child_uint(pre_key_node, 'id', 3)
        if not pub or len(pub) != 32 or not _is_valid_uint(pid):
            return None
        pre_key = {'keyId': pid, 'publicKey': generate_signal_pub_key(pub)}
    return {
        'registrationId': registration_id,
        'identityKey': generate_signal_pub_key(identity),
        'signedPreKey': {'keyId': signed_key_id, 'publicKey': generate_signal_pub_key(signed_pub_key),
                         'signature': signed_sig},
        'preKey': pre_key,
    }


async def parse_and_inject_e2e_sessions(node: dict, repository):
    def extract_key(key):
        if not key:
            return None
        return {'keyId': get_binary_node_child_uint(key, 'id', 3),
                'publicKey': generate_signal_pub_key(get_binary_node_child_buffer(key, 'value')),
                'signature': get_binary_node_child_buffer(key, 'signature')}

    nodes = get_binary_node_children(get_binary_node_child(node, 'list'), 'user')
    for n in nodes:
        assert_node_error_free(n)
    for nodes_chunk in _chunk(nodes, 100):
        for n in nodes_chunk:
            signed_key = get_binary_node_child(n, 'skey')
            key = get_binary_node_child(n, 'key')
            identity = get_binary_node_child_buffer(n, 'identity')
            jid = n['attrs']['jid']
            registration_id = get_binary_node_child_uint(n, 'registration', 4)
            await repository.inject_e2e_session(
                jid=jid,
                session={'registrationId': registration_id,
                         'identityKey': generate_signal_pub_key(identity),
                         'signedPreKey': extract_key(signed_key),
                         'preKey': extract_key(key)})


def extract_device_jids(result: list, my_jid: str, my_lid: str, exclude_zero_devices: bool) -> list:
    me = jid_decode(my_jid)
    my_user, my_device = me.user, me.device
    extracted = []
    for user_result in result:
        devices = user_result.get('devices')
        id_ = user_result.get('id')
        decoded = jid_decode(id_)
        user, server = decoded.user, decoded.server
        domain_type = decoded.domainType
        device_list = (devices or {}).get('deviceList')
        if not isinstance(device_list, list):
            continue
        for d in device_list:
            device, key_index, is_hosted = d.get('id'), d.get('keyIndex'), d.get('isHosted')
            if ((not exclude_zero_devices or device != 0) and
                    ((my_user != user and my_lid != user) or my_device != device) and
                    (device == 0 or bool(key_index))):
                dt = domain_type
                if is_hosted:
                    dt = WAJIDDomains.HOSTED_LID if domain_type == WAJIDDomains.LID else WAJIDDomains.HOSTED
                extracted.append(FullJid(user=user, device=device, domainType=int(dt),
                                         server=get_server_from_domain_type(server, dt)))
    return extracted


async def get_next_pre_keys(state: dict, count: int):
    creds, keys = state['creds'], state['keys']
    g = generate_or_get_pre_keys(creds, count)
    update = {
        'nextPreKeyId': max(g['lastPreKeyId'] + 1, creds['nextPreKeyId']),
        'firstUnuploadedPreKeyId': max(creds['firstUnuploadedPreKeyId'], g['lastPreKeyId'] + 1),
    }
    await maybe_await(keys.set({'pre-key': g['newPreKeys']}))
    pre_keys = await get_pre_keys(keys, g['preKeysRange'][0], g['preKeysRange'][0] + g['preKeysRange'][1])
    return {'update': update, 'preKeys': pre_keys}


async def get_next_pre_keys_node(state: dict, count: int):
    creds = state['creds']
    r = await get_next_pre_keys(state, count)
    pre_keys = r['preKeys']
    node = {
        'tag': 'iq',
        'attrs': {'xmlns': 'encrypt', 'type': 'set', 'to': S_WHATSAPP_NET},
        'content': [
            {'tag': 'registration', 'attrs': {}, 'content': encode_big_endian(creds['registrationId'])},
            {'tag': 'type', 'attrs': {}, 'content': KEY_BUNDLE_TYPE},
            {'tag': 'identity', 'attrs': {}, 'content': creds['signedIdentityKey']['public']},
            {'tag': 'list', 'attrs': {}, 'content': [xmpp_pre_key(pre_keys[k], int(k)) for k in pre_keys]},
            xmpp_signed_pre_key(creds['signedPreKey']),
        ],
    }
    return {'update': r['update'], 'node': node}
