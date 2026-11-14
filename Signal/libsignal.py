from __future__ import annotations
from typing import Optional
from ..Utils.cache import TTLCache
from ..Utils.crypto import generate_signal_pub_key
from ..WABinary import (is_hosted_lid_user, is_hosted_pn_user, is_lid_user, is_pn_user, jid_decode, transfer_device,
                        WAJIDDomains)
from . import protobuf_mini as pb
from .group import (GroupCipher, GroupSessionBuilder, SenderKeyDistributionMessage, SenderKeyName, SenderKeyRecord)
from .lid_mapping import LIDMappingStore
from .session import ProtocolAddress, SessionBuilder, SessionCipher, SessionRecord


def extract_identity_from_pkmsg(ciphertext: bytes) -> Optional[bytes]:
    """Extract identity key from PreKeyWhisperMessage for identity change detection"""
    try:
        if not ciphertext or len(ciphertext) < 2:
            return None
        if (ciphertext[0] & 0xF) != 3:
            return None
        d = pb.decode(bytes(ciphertext)[1:], pb.PREKEY_WHISPER_MESSAGE)
        ik = d.get('identityKey')
        if ik and len(ik) == 33:
            return bytes(ik)
    except Exception:
        pass
    return None


def jid_to_signal_protocol_address(jid: str) -> ProtocolAddress:
    decoded = jid_decode(jid)
    user, device, server, domain_type = decoded.user, decoded.device, decoded.server, decoded.domainType
    if not user:
        raise ValueError(f'JID decoded but user is empty: "{jid}" -> user: "{user}", server: "{server}", '
                         f'device: {device}')
    signal_user = f'{user}_{domain_type}' if domain_type != WAJIDDomains.WHATSAPP else user
    final_device = device or 0
    if device == 99 and server not in ('hosted', 'hosted.lid'):
        raise ValueError('Unexpected non-hosted device JID with device 99. This ID seems invalid. ID:' + jid)
    return ProtocolAddress(signal_user, final_device)


def jid_to_signal_sender_key_name(group: str, user: str) -> SenderKeyName:
    return SenderKeyName(group, jid_to_signal_protocol_address(user))


class _SignalStorage:
    """libsignal storage backed by the auth key store (`signalStorage` in the JS source)."""

    def __init__(self, auth: dict, lid_mapping: LIDMappingStore):
        self.creds = auth['creds']
        self.keys = auth['keys']
        self.lid_mapping = lid_mapping

    async def resolve_lid_signal_address(self, id_: str) -> str:
        if '.' in id_:
            device_id, device = id_.split('.', 1)
            parts = device_id.split('_')
            user = parts[0]
            domain_type = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            if domain_type in (WAJIDDomains.LID, WAJIDDomains.HOSTED_LID):
                return id_
            pn_jid = f"{user}{f':{device}' if device != '0' else ''}@" \
                     f"{'hosted' if domain_type == WAJIDDomains.HOSTED else 's.whatsapp.net'}"
            lid_for_pn = await self.lid_mapping.get_lid_for_pn(pn_jid)
            if lid_for_pn:
                return str(jid_to_signal_protocol_address(lid_for_pn))
        return id_

    async def load_session(self, id_: str):
        try:
            wire = await self.resolve_lid_signal_address(id_)
            sess = (await self.keys.get('session', [wire])).get(wire)
            if sess:
                return SessionRecord.deserialize(sess)
        except Exception:
            return None
        return None

    async def store_session(self, id_: str, session: SessionRecord):
        wire = await self.resolve_lid_signal_address(id_)
        await self.keys.set({'session': {wire: session.serialize()}})

    async def is_trusted_identity(self, *_a) -> bool:
        return True  # TOFU - Trust on First Use (same as WhatsApp Web)

    async def load_identity_key(self, id_: str):
        wire = await self.resolve_lid_signal_address(id_)
        return (await self.keys.get('identity-key', [wire])).get(wire) or None

    async def save_identity(self, id_: str, identity_key: bytes) -> bool:
        wire = await self.resolve_lid_signal_address(id_)
        existing = (await self.keys.get('identity-key', [wire])).get(wire)
        if existing and bytes(existing) != bytes(identity_key):
            await self.keys.set({'session': {wire: None}, 'identity-key': {wire: bytes(identity_key)}})
            return True
        if not existing:
            await self.keys.set({'identity-key': {wire: bytes(identity_key)}})
            return True
        return False

    async def load_pre_key(self, id_):
        key_id = str(id_)
        key = (await self.keys.get('pre-key', [key_id])).get(key_id)
        if key:
            return {'privKey': bytes(key['private']), 'pubKey': generate_signal_pub_key(bytes(key['public']))}
        return None

    async def remove_pre_key(self, id_):
        await self.keys.set({'pre-key': {str(id_): None}})

    async def load_signed_pre_key(self, *_a):
        key = self.creds['signedPreKey']
        return {'privKey': bytes(key['keyPair']['private']),
                'pubKey': generate_signal_pub_key(bytes(key['keyPair']['public']))}

    async def load_sender_key(self, sender_key_name: SenderKeyName) -> SenderKeyRecord:
        key_id = str(sender_key_name)
        key = (await self.keys.get('sender-key', [key_id])).get(key_id)
        if key:
            return SenderKeyRecord.deserialize(key)
        return SenderKeyRecord()

    async def store_sender_key(self, sender_key_name: SenderKeyName, record: SenderKeyRecord):
        await self.keys.set({'sender-key': {str(sender_key_name): record.to_bytes()}})

    async def get_our_registration_id(self) -> int:
        return self.creds['registrationId']

    async def get_our_identity(self):
        k = self.creds['signedIdentityKey']
        return {'privKey': bytes(k['private']), 'pubKey': generate_signal_pub_key(bytes(k['public']))}


class SignalRepository:
    """LibSignal based repository with LID mapping store (SignalRepositoryWithLIDStore)."""

    def __init__(self, auth: dict, logger, pn_to_lid_func=None):
        self.auth = auth
        self.logger = logger
        self.keys = auth['keys']
        self.lid_mapping = LIDMappingStore(self.keys, logger, pn_to_lid_func)
        self.storage = _SignalStorage(auth, self.lid_mapping)
        self.migrated_session_cache = TTLCache(ttl=3 * 24 * 60 * 60, update_age_on_get=True)

    async def _ensure_sender_key_and_create_skdm(self, group: str, me_id: str):
        sender_name = jid_to_signal_sender_key_name(group, me_id)
        s = str(sender_name)
        sender_key = (await self.keys.get('sender-key', [s])).get(s)
        if not sender_key:
            await self.storage.store_sender_key(sender_name, SenderKeyRecord())
        skdm = await GroupSessionBuilder(self.storage).create(sender_name)
        return sender_name, skdm

    async def decrypt_group_message(self, group: str, author_jid: str, msg: bytes) -> bytes:
        sender_name = jid_to_signal_sender_key_name(group, author_jid)
        cipher = GroupCipher(self.storage, sender_name)
        return await self.keys.transaction(lambda: cipher.decrypt(msg), group)

    async def process_sender_key_distribution_message(self, item, author_jid: str):
        builder = GroupSessionBuilder(self.storage)
        group_id = item.get('groupId')
        if not group_id:
            raise ValueError('Group ID is required for sender key distribution message')
        sender_name = jid_to_signal_sender_key_name(group_id, author_jid)
        sender_msg = SenderKeyDistributionMessage(serialized=item.get('axolotlSenderKeyDistributionMessage'))
        s = str(sender_name)

        async def work():
            sender_key = (await self.keys.get('sender-key', [s])).get(s)
            if not sender_key:
                await self.storage.store_sender_key(sender_name, SenderKeyRecord())
            await builder.process(sender_name, sender_msg)
        await self.keys.transaction(work, group_id)

    async def decrypt_message(self, jid: str, type: str, ciphertext: bytes) -> bytes:
        addr = jid_to_signal_protocol_address(jid)
        session = SessionCipher(self.storage, addr)
        # Extract and save sender's identity key before decryption for identity change detection
        if type == 'pkmsg':
            identity_key = extract_identity_from_pkmsg(ciphertext)
            if identity_key:
                addr_str = str(addr)
                changed = await self.storage.save_identity(addr_str, identity_key)
                if changed:
                    self.logger.info({'jid': jid, 'addr': addr_str},
                                     'identity key changed or new contact, session will be re-established')

        async def do_decrypt():
            if type == 'pkmsg':
                return await session.decrypt_pre_key_whisper_message(ciphertext)
            return await session.decrypt_whisper_message(ciphertext)
        return await self.keys.transaction(do_decrypt, jid)

    async def encrypt_message(self, jid: str, data: bytes) -> dict:
        addr = jid_to_signal_protocol_address(jid)
        cipher = SessionCipher(self.storage, addr)

        async def work():
            r = await cipher.encrypt(data)
            return {'type': 'pkmsg' if r['type'] == 3 else 'msg', 'ciphertext': bytes(r['body'])}
        return await self.keys.transaction(work, jid)

    async def encrypt_group_message(self, group: str, me_id: str, data: bytes) -> dict:
        async def work():
            sender_name, skdm = await self._ensure_sender_key_and_create_skdm(group, me_id)
            ciphertext = await GroupCipher(self.storage, sender_name).encrypt(data)
            return {'ciphertext': ciphertext, 'senderKeyDistributionMessage': skdm.serialize()}
        return await self.keys.transaction(work, group)

    async def get_sender_key_distribution_message(self, group: str, me_id: str) -> bytes:
        async def work():
            _, skdm = await self._ensure_sender_key_and_create_skdm(group, me_id)
            return skdm.serialize()
        return await self.keys.transaction(work, group)

    async def has_sender_key(self, group: str, me_id: str) -> bool:
        name = str(jid_to_signal_sender_key_name(group, me_id))
        return bool((await self.keys.get('sender-key', [name])).get(name))

    async def get_session_info(self, jid: str):
        addr = str(jid_to_signal_protocol_address(jid))
        session = await self.storage.load_session(addr)
        if not session:
            return None
        open_ = session.get_open_session()
        if not open_:
            return None
        base_key = open_.indexInfo.get('baseKey')
        registration_id = open_.registrationId
        if not base_key or not isinstance(registration_id, int):
            return None
        return {'baseKey': bytes(base_key), 'registrationId': registration_id}

    async def inject_e2e_session(self, jid: str, session: dict):
        self.logger.trace({'jid': jid}, 'injecting E2EE session')
        builder = SessionBuilder(self.storage, jid_to_signal_protocol_address(jid))
        await self.keys.transaction(lambda: builder.init_outgoing(session), jid)

    def jid_to_signal_protocol_address(self, jid: str) -> str:
        return str(jid_to_signal_protocol_address(jid))

    async def validate_session(self, jid: str) -> dict:
        try:
            addr = jid_to_signal_protocol_address(jid)
            session = await self.storage.load_session(str(addr))
            if not session:
                return {'exists': False, 'reason': 'no session'}
            if not session.have_open_session():
                return {'exists': False, 'reason': 'no open session'}
            return {'exists': True}
        except Exception:
            return {'exists': False, 'reason': 'validation error'}

    async def delete_session(self, jids: list):
        if not jids:
            return
        updates = {str(jid_to_signal_protocol_address(j)): None for j in jids}

        async def work():
            await self.keys.set({'session': updates})
        return await self.keys.transaction(work, f'delete-{len(jids)}-sessions')

    def close(self):
        self.migrated_session_cache.clear()
        self.lid_mapping.close()

    async def migrate_session(self, from_jid: str, to_jid: str) -> dict:
        if not from_jid or (not is_lid_user(to_jid) and not is_hosted_lid_user(to_jid)):
            return {'migrated': 0, 'skipped': 0, 'total': 0}
        if not is_pn_user(from_jid) and not is_hosted_pn_user(from_jid):
            return {'migrated': 0, 'skipped': 0, 'total': 1}
        decoded = jid_decode(from_jid)
        user = decoded.user
        self.logger.debug({'fromJid': from_jid}, 'bulk device migration - loading all user devices')
        user_devices = (await self.keys.get('device-list', [user])).get(user)
        if not user_devices:
            return {'migrated': 0, 'skipped': 0, 'total': 0}
        from_device_str = str(decoded.device or 0)
        if from_device_str not in user_devices:
            user_devices.append(from_device_str)
        uncached = [d for d in user_devices if not self.migrated_session_cache.has(f'{user}.{d}')]
        existing = await self.keys.get('session', [f'{user}.{d}' for d in uncached])
        device_jids = []
        for session_key, data in existing.items():
            if data:
                device_str = session_key.split('.')[1] if '.' in session_key else None
                if not device_str:
                    continue
                n = int(device_str)
                jid = f'{user}@s.whatsapp.net' if n == 0 else f'{user}:{n}@s.whatsapp.net'
                if n == 99:
                    jid = f'{user}:99@hosted'
                device_jids.append(jid)
        self.logger.debug({'fromJid': from_jid, 'totalDevices': len(user_devices),
                           'devicesWithSessions': len(device_jids)}, 'bulk device migration complete')

        async def work():
            ops = []
            for jid in device_jids:
                lid_with_device = transfer_device(jid, to_jid)
                f, t = jid_decode(jid), jid_decode(lid_with_device)
                ops.append({'pnUser': f.user, 'deviceId': f.device or 0,
                            'fromAddr': jid_to_signal_protocol_address(jid),
                            'toAddr': jid_to_signal_protocol_address(lid_with_device)})
            migrated = 0
            pn_addrs = list(dict.fromkeys(str(o['fromAddr']) for o in ops))
            pn_sessions = await self.keys.get('session', pn_addrs)
            updates = {}
            for op in ops:
                pn_s, lid_s = str(op['fromAddr']), str(op['toAddr'])
                pn_session = pn_sessions.get(pn_s)
                if pn_session:
                    from_session = SessionRecord.deserialize(pn_session)
                    if from_session.have_open_session():
                        updates[lid_s] = from_session.serialize()
                        updates[pn_s] = None
                        migrated += 1
            if updates:
                await self.keys.set({'session': updates})
                self.logger.debug({'migratedSessions': migrated}, 'bulk session migration complete')
                for op in ops:
                    if updates.get(str(op['toAddr'])):
                        self.migrated_session_cache.set(f"{op['pnUser']}.{op['deviceId']}", True)
            return {'migrated': migrated, 'skipped': len(ops) - migrated, 'total': len(ops)}
        return await self.keys.transaction(work, f'migrate-{len(device_jids)}-sessions-{jid_decode(to_jid).user}')


def make_lib_signal_repository(auth: dict, logger, pn_to_lid_func=None) -> SignalRepository:
    return SignalRepository(auth, logger, pn_to_lid_func)
