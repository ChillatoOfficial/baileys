from __future__ import annotations
import hashlib
from ..Defaults import (KEY_BUNDLE_TYPE, WA_ADV_ACCOUNT_SIG_PREFIX, WA_ADV_DEVICE_SIG_PREFIX,
                        WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX)
from ..WABinary import Boom, get_binary_node_child, jid_decode, S_WHATSAPP_NET
from ..WAProto import WAProto_pb2 as proto
from .crypto import Curve, hmac_sign
from .generics import encode_big_endian
from .proto_utils import from_object, to_dict
from .signal import create_signal_identity

CP = proto.ClientPayload


def _get_user_agent(config: dict):
    return {
        'appVersion': {'primary': config['version'][0], 'secondary': config['version'][1],
                       'tertiary': config['version'][2]},
        'platform': CP.UserAgent.ANDROID if 'android' in config['browser'][1].lower() else CP.UserAgent.WEB,
        'releaseChannel': CP.UserAgent.RELEASE,
        'osVersion': '0.1',
        'device': 'Desktop',
        'osBuildNumber': '0.1',
        'localeLanguageIso6391': 'en',
        'mnc': '000',
        'mcc': '000',
        'localeCountryIso31661Alpha2': config['countryCode'],
    }


_PLATFORM_MAP = {'Mac OS': CP.WebInfo.DARWIN, 'Windows': CP.WebInfo.WIN_HYBRID}


def _get_web_info(config: dict):
    web_sub_platform = CP.WebInfo.WEB_BROWSER
    if config['syncFullHistory'] and _PLATFORM_MAP.get(config['browser'][0]) is not None \
            and config['browser'][1] == 'Desktop':
        web_sub_platform = _PLATFORM_MAP[config['browser'][0]]
    return {'webSubPlatform': web_sub_platform}


def _get_client_payload(config: dict) -> dict:
    payload = {
        'connectType': CP.WIFI_UNKNOWN,
        'connectReason': CP.USER_ACTIVATED,
        'userAgent': _get_user_agent(config),
    }
    if 'android' not in config['browser'][1].lower():
        payload['webInfo'] = _get_web_info(config)
    if config.get('pushName'):
        payload['pushName'] = config['pushName']
    return payload


def generate_login_node(user_jid: str, config: dict):
    d = jid_decode(user_jid)
    payload = {**_get_client_payload(config), 'passive': True, 'pull': True, 'username': int(d.user),
               'device': d.device, 'lidDbMigrated': False}
    return from_object(CP, payload)


def _get_platform_type(platform: str):
    t = platform.upper()
    if t == 'ANDROID':
        return proto.DeviceProps.ANDROID_PHONE
    try:
        return proto.DeviceProps.PlatformType.Value(t)
    except ValueError:
        return proto.DeviceProps.CHROME


def generate_registration_node(creds: dict, config: dict):
    registration_id = creds['registrationId']
    signed_pre_key = creds['signedPreKey']
    signed_identity_key = creds['signedIdentityKey']
    # the app version needs to be md5 hashed and passed in
    app_version_buf = hashlib.md5('.'.join(str(x) for x in config['version']).encode()).digest()
    companion = {
        'os': config['browser'][0],
        'platformType': _get_platform_type(config['browser'][1]),
        'requireFullSync': config['syncFullHistory'],
        'historySyncConfig': {
            'storageQuotaMb': 10240,
            'inlineInitialPayloadInE2EeMsg': True,
            'supportCallLogHistory': False,
            'supportBotUserAgentChatHistory': True,
            'supportCagReactionsAndPolls': True,
            'supportBizHostedMsg': True,
            'supportRecentSyncChunkMessageCountTuning': True,
            'supportHostedGroupMsg': True,
            'supportFbidBotChatHistory': True,
            'supportMessageAssociation': True,
            'supportGroupHistory': False,
        },
        'version': {'primary': 10, 'secondary': 15, 'tertiary': 7},
    }
    companion_proto = from_object(proto.DeviceProps, companion).SerializeToString()
    register_payload = {
        **_get_client_payload(config),
        'passive': False,
        'pull': False,
        'devicePairingData': {
            'buildHash': app_version_buf,
            'deviceProps': companion_proto,
            'eRegid': encode_big_endian(registration_id),
            'eKeytype': KEY_BUNDLE_TYPE,
            'eIdent': signed_identity_key['public'],
            'eSkeyId': encode_big_endian(signed_pre_key['keyId'], 3),
            'eSkeyVal': signed_pre_key['keyPair']['public'],
            'eSkeySig': signed_pre_key['signature'],
        },
    }
    return from_object(CP, register_payload)


def configure_successful_pairing(stanza: dict, adv_secret_key: str, signed_identity_key: dict,
                                 signal_identities=None) -> dict:
    import base64
    msg_id = stanza['attrs']['id']
    pair_success = get_binary_node_child(stanza, 'pair-success')
    device_identity_node = get_binary_node_child(pair_success, 'device-identity')
    platform_node = get_binary_node_child(pair_success, 'platform')
    device_node = get_binary_node_child(pair_success, 'device')
    business_node = get_binary_node_child(pair_success, 'biz')
    if not device_identity_node or not device_node:
        raise Boom('Missing device-identity or device in pair success node', data=stanza)

    biz_name = business_node['attrs'].get('name') if business_node else None
    jid = device_node['attrs'].get('jid')
    lid = device_node['attrs'].get('lid')

    hmac_msg = proto.ADVSignedDeviceIdentityHMAC()
    hmac_msg.ParseFromString(device_identity_node['content'])
    hmac_prefix = b''
    if hmac_msg.HasField('accountType') and hmac_msg.accountType == proto.HOSTED:
        hmac_prefix = WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX
    adv_sign = hmac_sign(hmac_prefix + hmac_msg.details, base64.b64decode(adv_secret_key))
    if hmac_msg.hmac != adv_sign:
        raise Boom('Invalid account signature')

    account_msg_pb = proto.ADVSignedDeviceIdentity()
    account_msg_pb.ParseFromString(hmac_msg.details)
    account = to_dict(account_msg_pb)
    device_identity = proto.ADVDeviceIdentity()
    device_identity.ParseFromString(account['details'])

    prefix = WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX if device_identity.deviceType == proto.HOSTED \
        else WA_ADV_ACCOUNT_SIG_PREFIX
    account_msg = prefix + account['details'] + signed_identity_key['public']
    if not Curve.verify(account['accountSignatureKey'], account_msg, account['accountSignature']):
        raise Boom('Failed to verify account signature')

    device_msg = WA_ADV_DEVICE_SIG_PREFIX + account['details'] + signed_identity_key['public'] + \
        account['accountSignatureKey']
    account['deviceSignature'] = Curve.sign(signed_identity_key['private'], device_msg)

    identity = create_signal_identity(lid, account['accountSignatureKey'])
    account_enc = encode_signed_device_identity(account, False)

    reply = {
        'tag': 'iq',
        'attrs': {'to': S_WHATSAPP_NET, 'type': 'result', 'id': msg_id},
        'content': [{
            'tag': 'pair-device-sign', 'attrs': {},
            'content': [{'tag': 'device-identity', 'attrs': {'key-index': str(device_identity.keyIndex)},
                         'content': account_enc}],
        }],
    }
    auth_update = {
        'account': account,
        'me': {'id': jid, 'name': biz_name, 'lid': lid},
        'signalIdentities': [*(signal_identities or []), identity],
        'platform': platform_node['attrs'].get('name') if platform_node else None,
    }
    return {'creds': auth_update, 'reply': reply}


def encode_signed_device_identity(account, include_signature_key: bool) -> bytes:
    account = dict(account)
    # drop the signature key if we are not to include it, or if it is empty
    if not include_signature_key or not len(account.get('accountSignatureKey') or b''):
        account['accountSignatureKey'] = None
    return from_object(proto.ADVSignedDeviceIdentity, account).SerializeToString()
