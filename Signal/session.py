"""Port of libsignal-node's SessionRecord / SessionBuilder / SessionCipher (X3DH + Double Ratchet)."""
from __future__ import annotations
import base64
import json
import time
from . import curve
from .crypto import calculate_mac, decrypt, derive_secrets, encrypt, verify_mac
from . import protobuf_mini as pb

CHAIN_SENDING = 1
CHAIN_RECEIVING = 2
BASE_KEY_OURS = 1
BASE_KEY_THEIRS = 2
CLOSED_SESSIONS_MAX = 40
SESSION_RECORD_VERSION = 'v1'
MESSAGE_LOST_THRESHOLD = 2000


class SignalError(Exception):
    pass


class UntrustedIdentityKeyError(SignalError):
    pass


class SessionError(SignalError):
    pass


class MessageCounterError(SignalError):
    pass


class PreKeyError(SignalError):
    pass


def _b64(b) -> str:
    return base64.b64encode(bytes(b)).decode()


def _unb64(s) -> bytes:
    return base64.b64decode(s)


class ProtocolAddress:
    def __init__(self, id_: str, device_id: int):
        self.id = id_
        self.deviceId = device_id

    @staticmethod
    def from_string(s: str) -> 'ProtocolAddress':
        if not isinstance(s, str) or '.' not in s:
            raise TypeError('Invalid address encoding')
        id_, device = s.rsplit('.', 1)
        return ProtocolAddress(id_, int(device))

    def __str__(self):
        return f'{self.id}.{self.deviceId}'

    def toString(self):
        return str(self)

    def is_equal(self, other):
        return other and self.id == other.id and self.deviceId == other.deviceId


class SessionEntry:
    """A single double-ratchet session state (libsignal's SessionEntry)."""

    def __init__(self):
        self.registrationId = None
        self.currentRatchet = None
        self.indexInfo = None
        self._chains = {}
        self.pendingPreKey = None

    @staticmethod
    def create() -> 'SessionEntry':
        return SessionEntry()

    def add_chain(self, key: bytes, value: dict):
        k = _b64(key)
        if k in self._chains:
            raise SessionError('Overwriting a chain')
        self._chains[k] = value

    def get_chain(self, key: bytes):
        return self._chains.get(_b64(key))

    def delete_chain(self, key: bytes):
        k = _b64(key)
        if k not in self._chains:
            raise SessionError('Rolling back')
        del self._chains[k]

    def chains(self):
        return self._chains.items()

    def serialize(self) -> dict:
        chains = {}
        for k, c in self._chains.items():
            chains[k] = {
                'chainKey': {'counter': c['chainKey']['counter'],
                             'key': _b64(c['chainKey']['key']) if c['chainKey'].get('key') is not None else None},
                'chainType': c['chainType'],
                'messageKeys': {str(i): _b64(mk) for i, mk in c['messageKeys'].items()},
            }
        r = self.currentRatchet
        ratchet = {
            'ephemeralKeyPair': {'pubKey': _b64(r['ephemeralKeyPair']['pubKey']),
                                 'privKey': _b64(r['ephemeralKeyPair']['privKey'])},
            'lastRemoteEphemeralKey': _b64(r['lastRemoteEphemeralKey']),
            'previousCounter': r['previousCounter'],
            'rootKey': _b64(r['rootKey']),
        }
        ii = self.indexInfo
        index_info = {'baseKey': _b64(ii['baseKey']), 'baseKeyType': ii['baseKeyType'], 'closed': ii['closed'],
                      'used': ii['used'], 'created': ii['created'], 'remoteIdentityKey': _b64(ii['remoteIdentityKey'])}
        data = {'registrationId': self.registrationId, 'currentRatchet': ratchet, 'indexInfo': index_info,
                '_chains': chains}
        if self.pendingPreKey:
            pp = dict(self.pendingPreKey)
            pp['baseKey'] = _b64(pp['baseKey'])
            data['pendingPreKey'] = pp
        return data

    @staticmethod
    def deserialize(data: dict) -> 'SessionEntry':
        e = SessionEntry()
        e.registrationId = data.get('registrationId')
        r = data['currentRatchet']
        e.currentRatchet = {
            'ephemeralKeyPair': {'pubKey': _unb64(r['ephemeralKeyPair']['pubKey']),
                                 'privKey': _unb64(r['ephemeralKeyPair']['privKey'])},
            'lastRemoteEphemeralKey': _unb64(r['lastRemoteEphemeralKey']),
            'previousCounter': r['previousCounter'],
            'rootKey': _unb64(r['rootKey']),
        }
        ii = data['indexInfo']
        e.indexInfo = {'baseKey': _unb64(ii['baseKey']), 'baseKeyType': ii['baseKeyType'], 'closed': ii['closed'],
                       'used': ii['used'], 'created': ii['created'],
                       'remoteIdentityKey': _unb64(ii['remoteIdentityKey'])}
        for k, c in data['_chains'].items():
            e._chains[k] = {
                'chainKey': {'counter': c['chainKey']['counter'],
                             'key': _unb64(c['chainKey']['key']) if c['chainKey'].get('key') else None},
                'chainType': c['chainType'],
                'messageKeys': {int(i): _unb64(mk) for i, mk in c['messageKeys'].items()},
            }
        pp = data.get('pendingPreKey')
        if pp:
            pp = dict(pp)
            pp['baseKey'] = _unb64(pp['baseKey'])
            e.pendingPreKey = pp
        return e


class SessionRecord:
    def __init__(self):
        self.sessions: dict[str, SessionEntry] = {}
        self.version = SESSION_RECORD_VERSION

    @staticmethod
    def deserialize(data) -> 'SessionRecord':
        obj = json.loads(bytes(data).decode('utf-8') if not isinstance(data, str) else data)
        rec = SessionRecord()
        for k, v in obj.get('_sessions', {}).items():
            rec.sessions[k] = SessionEntry.deserialize(v)
        return rec

    def serialize(self) -> bytes:
        return json.dumps({'_sessions': {k: s.serialize() for k, s in self.sessions.items()},
                           'version': self.version}).encode('utf-8')

    def have_open_session(self) -> bool:
        o = self.get_open_session()
        return bool(o) and isinstance(o.registrationId, int)

    haveOpenSession = have_open_session

    def get_session(self, key: bytes):
        s = self.sessions.get(_b64(key))
        if s and s.indexInfo['baseKeyType'] == BASE_KEY_OURS:
            raise SessionError('Tried to lookup a session using our basekey')
        return s

    def get_open_session(self):
        for s in self.sessions.values():
            if not self._is_closed(s):
                return s
        return None

    getOpenSession = get_open_session

    def set_session(self, session: SessionEntry):
        self.sessions[_b64(session.indexInfo['baseKey'])] = session

    def get_sessions(self):
        # newest used first, open sessions first
        return sorted(self.sessions.values(),
                      key=lambda s: (0 if s.indexInfo['closed'] == -1 else 1, -s.indexInfo['used']))

    @staticmethod
    def _is_closed(s: SessionEntry) -> bool:
        return s.indexInfo['closed'] != -1

    def close_session(self, session: SessionEntry):
        if self._is_closed(session):
            return
        session.indexInfo['closed'] = int(time.time() * 1000)

    def open_session(self, session: SessionEntry):
        if not self._is_closed(session):
            return
        session.indexInfo['closed'] = -1

    def archive_current_state(self):
        o = self.get_open_session()
        if o:
            self.close_session(o)

    def promote_state(self, session):
        self.open_session(session)

    def remove_old_sessions(self):
        while len(self.sessions) > CLOSED_SESSIONS_MAX:
            oldest_key = None
            oldest = None
            for k, s in self.sessions.items():
                if s.indexInfo['closed'] != -1 and (oldest is None or s.indexInfo['closed'] < oldest):
                    oldest, oldest_key = s.indexInfo['closed'], k
            if oldest_key is None:
                break
            del self.sessions[oldest_key]

    def delete_all_sessions(self):
        self.sessions = {}


class SessionBuilder:
    def __init__(self, storage, protocol_address: ProtocolAddress):
        self.addr = protocol_address
        self.storage = storage

    async def init_outgoing(self, device: dict):
        fq = str(self.addr)
        if not await self.storage.is_trusted_identity(self.addr.id, device['identityKey']):
            raise UntrustedIdentityKeyError()
        curve.verify_signature(device['identityKey'], device['signedPreKey']['publicKey'],
                               device['signedPreKey']['signature'], True)
        base_key = curve.generate_key_pair()
        device_pre_key = device.get('preKey') and device['preKey']['publicKey']
        session = await self._init_session(True, base_key, None, device['identityKey'], device_pre_key,
                                           device['signedPreKey']['publicKey'], device['registrationId'])
        session.pendingPreKey = {'signedKeyId': device['signedPreKey']['keyId'], 'baseKey': base_key['pubKey']}
        if device.get('preKey'):
            session.pendingPreKey['preKeyId'] = device['preKey']['keyId']
        record = await self.storage.load_session(fq)
        if not record:
            record = SessionRecord()
        else:
            open_session = record.get_open_session()
            if open_session:
                record.close_session(open_session)
        record.set_session(session)
        await self.storage.store_session(fq, record)

    async def init_incoming(self, record: SessionRecord, message: dict):
        fq = str(self.addr)
        if not await self.storage.is_trusted_identity(self.addr.id, message['identityKey']):
            raise UntrustedIdentityKeyError()
        if record.get_session(message['baseKey']):
            return None
        pre_key_pair = await self.storage.load_pre_key(message['preKeyId']) if message.get('preKeyId') is not None else None
        if message.get('preKeyId') is not None and not pre_key_pair:
            raise PreKeyError('Invalid PreKey ID')
        signed = await self.storage.load_signed_pre_key(message['signedPreKeyId'])
        if not signed:
            raise PreKeyError('Missing SignedPreKey')
        session = record.get_open_session()
        if signed is None:
            if session and session.currentRatchet:
                return None
            raise SessionError('Missing Open Session for PreKeyWhisperMessage')
        record.archive_current_state()
        new_session = await self._init_session(False, pre_key_pair, signed, message['identityKey'],
                                               message['baseKey'], None, message['registrationId'])
        record.set_session(new_session)
        return message.get('preKeyId')

    async def _init_session(self, is_initiator, our_ephemeral, our_signed, their_identity, their_ephemeral,
                            their_signed, registration_id) -> SessionEntry:
        if is_initiator:
            if our_signed is not None:
                raise SessionError('Invalid call to initSession')
            our_signed = our_ephemeral
        else:
            if their_signed is not None:
                raise SessionError('Invalid call to initSession')
            their_signed = their_ephemeral
        our_identity = await self.storage.get_our_identity()
        a1 = curve.calculate_agreement(their_signed, our_identity['privKey']) if is_initiator else \
            curve.calculate_agreement(their_identity, our_signed['privKey'])
        a2 = curve.calculate_agreement(their_identity, our_signed['privKey']) if is_initiator else \
            curve.calculate_agreement(their_signed, our_identity['privKey'])
        a3 = curve.calculate_agreement(their_signed, our_signed['privKey'])
        # initiator: IK_A·SPK_B | EK_A·IK_B | EK_A·SPK_B ; responder computes the same values mirrored
        secret = b'\xff' * 32 + a1 + a2 + a3
        if our_ephemeral and their_ephemeral and is_initiator:
            secret += curve.calculate_agreement(their_ephemeral, our_ephemeral['privKey'])
        elif (not is_initiator) and our_ephemeral:
            # one-time prekey private key with the initiator's base key
            secret += curve.calculate_agreement(their_signed, our_ephemeral['privKey'])
        master = derive_secrets(secret, b'\x00' * 32, b'WhisperText', 2)
        session = SessionEntry.create()
        session.registrationId = registration_id
        session.currentRatchet = {
            'rootKey': master[0],
            'ephemeralKeyPair': curve.generate_key_pair() if is_initiator else our_signed,
            'lastRemoteEphemeralKey': their_signed,
            'previousCounter': 0,
        }
        now = int(time.time() * 1000)
        session.indexInfo = {
            'created': now, 'used': now, 'remoteIdentityKey': their_identity,
            'baseKey': our_ephemeral['pubKey'] if is_initiator else their_ephemeral,
            'baseKeyType': BASE_KEY_OURS if is_initiator else BASE_KEY_THEIRS,
            'closed': -1,
        }
        if is_initiator:
            self._calculate_sending_ratchet(session, their_signed)
        return session

    @staticmethod
    def _calculate_sending_ratchet(session: SessionEntry, remote_key: bytes):
        ratchet = session.currentRatchet
        shared = curve.calculate_agreement(remote_key, ratchet['ephemeralKeyPair']['privKey'])
        master = derive_secrets(shared, ratchet['rootKey'], b'WhisperRatchet', 2)
        session.add_chain(ratchet['ephemeralKeyPair']['pubKey'], {
            'messageKeys': {}, 'chainKey': {'counter': -1, 'key': master[1]}, 'chainType': CHAIN_SENDING})
        ratchet['rootKey'] = master[0]


class SessionCipher:
    def __init__(self, storage, protocol_address: ProtocolAddress):
        if not isinstance(protocol_address, ProtocolAddress):
            raise TypeError('protocol_address must be a ProtocolAddress')
        self.addr = protocol_address
        self.storage = storage

    def _addr_str(self):
        return str(self.addr)

    @staticmethod
    def _encode_tuple_byte(high, low):
        if high > 15 or low > 15:
            raise ValueError('Numbers must be 4 bits or less')
        return (high << 4) | low

    async def _get_record(self):
        return await self.storage.load_session(self._addr_str())

    async def _store_record(self, record: SessionRecord):
        record.remove_old_sessions()
        await self.storage.store_session(self._addr_str(), record)

    async def encrypt(self, data: bytes) -> dict:
        our_identity = await self.storage.get_our_identity()
        record = await self._get_record()
        if not record:
            raise SessionError('No sessions')
        session = record.get_open_session()
        if not session:
            raise SessionError('No open session')
        remote_identity = session.indexInfo['remoteIdentityKey']
        if not await self.storage.is_trusted_identity(self.addr.id, remote_identity):
            raise UntrustedIdentityKeyError()
        chain = session.get_chain(session.currentRatchet['ephemeralKeyPair']['pubKey'])
        if chain['chainType'] == CHAIN_RECEIVING:
            raise SessionError('Tried to encrypt on a receiving chain')
        self._fill_message_keys(chain, chain['chainKey']['counter'] + 1)
        counter = chain['chainKey']['counter']
        keys = derive_secrets(chain['messageKeys'][counter], b'\x00' * 32, b'WhisperMessageKeys')
        del chain['messageKeys'][counter]
        msg_buf = pb.encode({
            'ephemeralKey': session.currentRatchet['ephemeralKeyPair']['pubKey'],
            'counter': counter,
            'previousCounter': session.currentRatchet['previousCounter'],
            'ciphertext': encrypt(keys[0], data, keys[2][:16]),
        }, pb.WHISPER_MESSAGE)
        tuple_byte = bytes([self._encode_tuple_byte(3, 3)])
        mac_input = bytes(our_identity['pubKey']) + bytes(remote_identity) + tuple_byte + msg_buf
        mac = calculate_mac(keys[1], mac_input)
        result = tuple_byte + msg_buf + mac[:8]
        await self._store_record(record)
        pending = session.pendingPreKey
        if pending:
            fields = {
                'identityKey': our_identity['pubKey'],
                'registrationId': await self.storage.get_our_registration_id(),
                'baseKey': pending['baseKey'],
                'signedPreKeyId': pending['signedKeyId'],
                'message': result,
            }
            if pending.get('preKeyId') is not None:
                fields['preKeyId'] = pending['preKeyId']
            body = tuple_byte + pb.encode(fields, pb.PREKEY_WHISPER_MESSAGE)
            return {'type': 3, 'body': body, 'registrationId': session.registrationId}
        return {'type': 1, 'body': result, 'registrationId': session.registrationId}

    async def decrypt_whisper_message(self, data: bytes) -> bytes:
        data = bytes(data)
        record = await self._get_record()
        if not record:
            raise SessionError('No session record')
        errors = []
        for session in record.get_sessions():
            try:
                plaintext = await self._do_decrypt_whisper_message(data, session)
                session.indexInfo['used'] = int(time.time() * 1000)
                await self._store_record(record)
                return plaintext
            except MessageCounterError:
                raise
            except Exception as e:  # noqa
                errors.append(e)
        raise SessionError('No matching sessions found for message: ' + '; '.join(str(e) for e in errors))

    async def decrypt_pre_key_whisper_message(self, data: bytes) -> bytes:
        data = bytes(data)
        version = data[0]
        if (version & 0xF) > 3 or (version >> 4) < 3:
            raise SessionError('Incompatible version number on PreKeyWhisperMessage')
        pre = pb.decode(data[1:], pb.PREKEY_WHISPER_MESSAGE)
        pre.setdefault('registrationId', 0)
        record = await self._get_record() or SessionRecord()
        builder = SessionBuilder(self.storage, self.addr)
        pre_key_id = await builder.init_incoming(record, pre)
        session = record.get_session(pre['baseKey'])
        plaintext = await self._do_decrypt_whisper_message(pre['message'], session)
        session.indexInfo['used'] = int(time.time() * 1000)
        await self._store_record(record)
        if pre_key_id is not None:
            await self.storage.remove_pre_key(pre_key_id)
        return plaintext

    async def _do_decrypt_whisper_message(self, message_buffer: bytes, session: SessionEntry) -> bytes:
        message_buffer = bytes(message_buffer)
        if not session:
            raise SessionError('No session to decrypt with')
        version = message_buffer[0]
        if (version & 0xF) > 3 or (version >> 4) < 3:
            raise SessionError('Incompatible version number on WhisperMessage')
        proto_bytes = message_buffer[1:-8]
        message = pb.decode(proto_bytes, pb.WHISPER_MESSAGE)
        message.setdefault('counter', 0)
        message.setdefault('previousCounter', 0)
        self._maybe_step_ratchet(session, message['ephemeralKey'], message['previousCounter'])
        chain = session.get_chain(message['ephemeralKey'])
        if chain['chainType'] == CHAIN_SENDING:
            raise SessionError('Tried to decrypt on a sending chain')
        self._fill_message_keys(chain, message['counter'])
        if message['counter'] not in chain['messageKeys']:
            raise MessageCounterError('Key used already or never filled')
        message_key = chain['messageKeys'].pop(message['counter'])
        keys = derive_secrets(message_key, b'\x00' * 32, b'WhisperMessageKeys')
        our_identity = await self.storage.get_our_identity()
        mac_input = bytes(session.indexInfo['remoteIdentityKey']) + bytes(our_identity['pubKey']) + \
            bytes([self._encode_tuple_byte(3, 3)]) + proto_bytes
        verify_mac(mac_input, keys[1], message_buffer[-8:], 8)
        plaintext = decrypt(keys[0], message['ciphertext'], keys[2][:16])
        session.pendingPreKey = None
        return plaintext

    def _fill_message_keys(self, chain: dict, counter: int):
        if chain['chainKey']['counter'] >= counter:
            return
        if counter - chain['chainKey']['counter'] > MESSAGE_LOST_THRESHOLD:
            raise SessionError('Over 2000 messages into the future!')
        if chain['chainKey'].get('key') is None:
            raise SessionError('Chain closed')
        key = chain['chainKey']['key']
        chain['messageKeys'][chain['chainKey']['counter'] + 1] = calculate_mac(key, b'\x01')
        chain['chainKey']['key'] = calculate_mac(key, b'\x02')
        chain['chainKey']['counter'] += 1
        self._fill_message_keys(chain, counter)

    def _maybe_step_ratchet(self, session: SessionEntry, remote_key: bytes, previous_counter: int):
        if session.get_chain(remote_key):
            return
        ratchet = session.currentRatchet
        previous = session.get_chain(ratchet['lastRemoteEphemeralKey'])
        if previous:
            self._fill_message_keys(previous, previous_counter)
            previous['chainKey']['key'] = None
        self._calculate_ratchet(session, remote_key, False)
        prev_chain = session.get_chain(ratchet['ephemeralKeyPair']['pubKey'])
        if prev_chain:
            ratchet['previousCounter'] = prev_chain['chainKey']['counter']
            session.delete_chain(ratchet['ephemeralKeyPair']['pubKey'])
        ratchet['ephemeralKeyPair'] = curve.generate_key_pair()
        self._calculate_ratchet(session, remote_key, True)
        ratchet['lastRemoteEphemeralKey'] = remote_key

    @staticmethod
    def _calculate_ratchet(session: SessionEntry, remote_key: bytes, sending: bool):
        ratchet = session.currentRatchet
        shared = curve.calculate_agreement(remote_key, ratchet['ephemeralKeyPair']['privKey'])
        master = derive_secrets(shared, ratchet['rootKey'], b'WhisperRatchet', 2)
        chain_key = ratchet['ephemeralKeyPair']['pubKey'] if sending else remote_key
        session.add_chain(chain_key, {'messageKeys': {}, 'chainKey': {'counter': -1, 'key': master[1]},
                                      'chainType': CHAIN_SENDING if sending else CHAIN_RECEIVING})
        ratchet['rootKey'] = master[0]

    async def has_open_session(self) -> bool:
        record = await self._get_record()
        return bool(record and record.have_open_session())

    async def close_open_session(self):
        record = await self._get_record()
        if record:
            o = record.get_open_session()
            if o:
                record.close_session(o)
                await self._store_record(record)
