"""Port of Baileys' Signal/Group (sender-key / group cipher)."""
from __future__ import annotations
import base64
import json
import os
from typing import Optional
from . import curve
from .crypto import calculate_mac, decrypt, derive_secrets, encrypt
from ..WAProto import WAProto_pb2 as proto

CURRENT_VERSION = 3


def _bj_replacer(o):
    if isinstance(o, (bytes, bytearray)):
        return {'type': 'Buffer', 'data': base64.b64encode(bytes(o)).decode()}
    raise TypeError


def _bj_reviver(d):
    if d.get('type') == 'Buffer' and isinstance(d.get('data'), str):
        return base64.b64decode(d['data'])
    return d


class SenderKeyName:
    def __init__(self, group_id: str, sender):
        self.group_id = group_id
        self.sender = sender  # ProtocolAddress

    def get_group_id(self):
        return self.group_id

    def get_sender(self):
        return self.sender

    def serialize(self) -> str:
        return f'{self.group_id}::{self.sender.id}::{self.sender.deviceId}'

    def __str__(self):
        return self.serialize()

    toString = serialize

    def equals(self, other) -> bool:
        return other is not None and self.group_id == other.group_id and str(self.sender) == str(other.sender)


class SenderMessageKey:
    def __init__(self, iteration: int, seed: bytes):
        derivative = derive_secrets(seed, b'\x00' * 32, b'WhisperGroup')
        self.iv = derivative[0][:16]
        self.cipher_key = derivative[0][16:] + derivative[1][:16]
        self.iteration = iteration
        self.seed = bytes(seed)

    def get_iteration(self): return self.iteration
    def get_iv(self): return self.iv
    def get_cipher_key(self): return self.cipher_key
    def get_seed(self): return self.seed


class SenderChainKey:
    MESSAGE_KEY_SEED = b'\x01'
    CHAIN_KEY_SEED = b'\x02'

    def __init__(self, iteration: int, chain_key: bytes):
        self.iteration = iteration
        self.chain_key = bytes(chain_key)

    def get_iteration(self): return self.iteration

    def get_sender_message_key(self) -> SenderMessageKey:
        return SenderMessageKey(self.iteration, calculate_mac(self.chain_key, self.MESSAGE_KEY_SEED))

    def get_next(self) -> 'SenderChainKey':
        return SenderChainKey(self.iteration + 1, calculate_mac(self.chain_key, self.CHAIN_KEY_SEED))

    def get_seed(self) -> bytes: return self.chain_key


class SenderKeyState:
    MAX_MESSAGE_KEYS = 2000

    def __init__(self, id_=None, iteration=None, chain_key=None, signature_key_pair=None,
                 signature_key_public=None, signature_key_private=None, structure=None):
        if structure:
            self.structure = dict(structure)
            if not isinstance(self.structure.get('senderMessageKeys'), list):
                self.structure['senderMessageKeys'] = []
        else:
            if signature_key_pair:
                signature_key_public = signature_key_pair['public']
                signature_key_private = signature_key_pair['private']
            self.structure = {
                'senderKeyId': id_ or 0,
                'senderChainKey': {'iteration': iteration or 0, 'seed': bytes(chain_key or b'')},
                'senderSigningKey': {'public': bytes(signature_key_public or b''),
                                     'private': bytes(signature_key_private or b'')},
                'senderMessageKeys': [],
            }

    def get_key_id(self): return self.structure['senderKeyId']

    def get_sender_chain_key(self) -> SenderChainKey:
        c = self.structure['senderChainKey']
        return SenderChainKey(c['iteration'], c['seed'])

    def set_sender_chain_key(self, ck: SenderChainKey):
        self.structure['senderChainKey'] = {'iteration': ck.get_iteration(), 'seed': ck.get_seed()}

    def get_signing_key_public(self) -> bytes:
        pk = bytes(self.structure['senderSigningKey']['public'])
        if len(pk) == 32:
            return b'\x05' + pk
        return pk

    def get_signing_key_private(self) -> bytes:
        return bytes(self.structure['senderSigningKey'].get('private') or b'')

    def has_sender_message_key(self, iteration: int) -> bool:
        return any(k['iteration'] == iteration for k in self.structure['senderMessageKeys'])

    def add_sender_message_key(self, mk: SenderMessageKey):
        keys = self.structure['senderMessageKeys']
        keys.append({'iteration': mk.get_iteration(), 'seed': mk.get_seed()})
        if len(keys) > self.MAX_MESSAGE_KEYS:
            keys.pop(0)

    def remove_sender_message_key(self, iteration: int) -> Optional[SenderMessageKey]:
        keys = self.structure['senderMessageKeys']
        for i, k in enumerate(keys):
            if k['iteration'] == iteration:
                keys.pop(i)
                return SenderMessageKey(k['iteration'], k['seed'])
        return None

    def get_structure(self): return self.structure


class SenderKeyRecord:
    MAX_STATES = 5

    def __init__(self, serialized: Optional[list] = None):
        self.states: list[SenderKeyState] = []
        for s in serialized or []:
            self.states.append(SenderKeyState(structure=s))

    def is_empty(self) -> bool: return len(self.states) == 0

    def get_sender_key_state(self, key_id=None) -> Optional[SenderKeyState]:
        if key_id is None and self.states:
            return self.states[-1]
        for s in self.states:
            if s.get_key_id() == key_id:
                return s
        return None

    def add_sender_key_state(self, id_, iteration, chain_key, signature_key):
        self.states.append(SenderKeyState(id_, iteration, chain_key, None, signature_key))
        if len(self.states) > self.MAX_STATES:
            self.states.pop(0)

    def set_sender_key_state(self, id_, iteration, chain_key, key_pair):
        self.states = [SenderKeyState(id_, iteration, chain_key, key_pair)]

    def serialize(self) -> list:
        return [s.get_structure() for s in self.states]

    def to_bytes(self) -> bytes:
        return json.dumps(self.serialize(), default=_bj_replacer).encode('utf-8')

    @staticmethod
    def deserialize(data: bytes) -> 'SenderKeyRecord':
        parsed = json.loads(bytes(data).decode('utf-8'), object_hook=_bj_reviver)
        return SenderKeyRecord(parsed)


class SenderKeyDistributionMessage:
    def __init__(self, id_=None, iteration=None, chain_key=None, signature_key=None, serialized=None):
        if serialized:
            msg = proto.SenderKeyDistributionMessage()
            msg.ParseFromString(bytes(serialized)[1:])
            self.serialized = bytes(serialized)
            self.id = msg.id
            self.iteration = msg.iteration
            self.chain_key = bytes(msg.chainKey)
            self.signature_key = bytes(msg.signingKey)
        else:
            version = ((CURRENT_VERSION << 4) | CURRENT_VERSION) & 0xFF
            self.id, self.iteration = id_, iteration
            self.chain_key, self.signature_key = bytes(chain_key), bytes(signature_key)
            m = proto.SenderKeyDistributionMessage(id=id_, iteration=iteration, chainKey=self.chain_key,
                                                   signingKey=self.signature_key)
            self.serialized = bytes([version]) + m.SerializeToString()

    def serialize(self): return self.serialized
    def get_type(self): return 5
    def get_iteration(self): return self.iteration
    def get_chain_key(self): return self.chain_key
    def get_signature_key(self): return self.signature_key
    def get_id(self): return self.id


class SenderKeyMessage:
    SIGNATURE_LENGTH = 64

    def __init__(self, key_id=None, iteration=None, ciphertext=None, signature_key=None, serialized=None):
        if serialized:
            serialized = bytes(serialized)
            version = serialized[0]
            message = serialized[1:len(serialized) - self.SIGNATURE_LENGTH]
            signature = serialized[-self.SIGNATURE_LENGTH:]
            m = proto.SenderKeyMessage()
            m.ParseFromString(message)
            self.serialized = serialized
            self.message_version = (version & 0xFF) >> 4
            self.key_id, self.iteration, self.ciphertext = m.id, m.iteration, bytes(m.ciphertext)
            self.signature = signature
        else:
            version = ((CURRENT_VERSION << 4) | CURRENT_VERSION) & 0xFF
            ct = bytes(ciphertext)
            message = proto.SenderKeyMessage(id=key_id, iteration=iteration, ciphertext=ct).SerializeToString()
            signature = curve.calculate_signature(signature_key, bytes([version]) + message)
            self.serialized = bytes([version]) + message + signature
            self.message_version = CURRENT_VERSION
            self.key_id, self.iteration, self.ciphertext, self.signature = key_id, iteration, ct, signature

    def get_key_id(self): return self.key_id
    def get_iteration(self): return self.iteration
    def get_cipher_text(self): return self.ciphertext

    def verify_signature(self, signature_key: bytes):
        part1 = self.serialized[:-self.SIGNATURE_LENGTH]
        part2 = self.serialized[-self.SIGNATURE_LENGTH:]
        if not curve.verify(signature_key, part1, part2):
            raise ValueError('Invalid signature!')

    def serialize(self): return self.serialized
    def get_type(self): return 4


class GroupSessionBuilder:
    def __init__(self, sender_key_store):
        self.store = sender_key_store

    async def process(self, sender_key_name: SenderKeyName, msg: SenderKeyDistributionMessage):
        record = await self.store.load_sender_key(sender_key_name)
        record.add_sender_key_state(msg.get_id(), msg.get_iteration(), msg.get_chain_key(), msg.get_signature_key())
        await self.store.store_sender_key(sender_key_name, record)

    async def create(self, sender_key_name: SenderKeyName) -> SenderKeyDistributionMessage:
        record = await self.store.load_sender_key(sender_key_name)
        if record.is_empty():
            key_id = int.from_bytes(os.urandom(4), 'big') % 2147483647
            sender_key = os.urandom(32)
            kp = curve.generate_key_pair()
            record.set_sender_key_state(key_id, 0, sender_key, {'public': kp['pubKey'], 'private': kp['privKey']})
            await self.store.store_sender_key(sender_key_name, record)
        state = record.get_sender_key_state()
        if not state:
            raise ValueError('No session state available')
        return SenderKeyDistributionMessage(state.get_key_id(), state.get_sender_chain_key().get_iteration(),
                                            state.get_sender_chain_key().get_seed(), state.get_signing_key_public())


class GroupCipher:
    def __init__(self, sender_key_store, sender_key_name: SenderKeyName):
        self.store = sender_key_store
        self.name = sender_key_name

    async def encrypt(self, padded_plaintext: bytes) -> bytes:
        record = await self.store.load_sender_key(self.name)
        if not record:
            raise ValueError('No SenderKeyRecord found for encryption')
        state = record.get_sender_key_state()
        if not state:
            raise ValueError('No session to encrypt message')
        iteration = state.get_sender_chain_key().get_iteration()
        sender_key = self._get_sender_key(state, 0 if iteration == 0 else iteration + 1)
        ct = encrypt(sender_key.get_cipher_key(), padded_plaintext, sender_key.get_iv())
        msg = SenderKeyMessage(state.get_key_id(), sender_key.get_iteration(), ct, state.get_signing_key_private())
        await self.store.store_sender_key(self.name, record)
        return msg.serialize()

    async def decrypt(self, sender_key_message_bytes: bytes) -> bytes:
        record = await self.store.load_sender_key(self.name)
        if not record:
            raise ValueError('No SenderKeyRecord found for decryption')
        msg = SenderKeyMessage(serialized=sender_key_message_bytes)
        state = record.get_sender_key_state(msg.get_key_id())
        if not state:
            raise ValueError('No session found to decrypt message')
        msg.verify_signature(state.get_signing_key_public())
        sender_key = self._get_sender_key(state, msg.get_iteration())
        try:
            plaintext = decrypt(sender_key.get_cipher_key(), msg.get_cipher_text(), sender_key.get_iv())
        except Exception:
            raise ValueError('InvalidMessageException')
        await self.store.store_sender_key(self.name, record)
        return plaintext

    @staticmethod
    def _get_sender_key(state: SenderKeyState, iteration: int) -> SenderMessageKey:
        chain = state.get_sender_chain_key()
        if chain.get_iteration() > iteration:
            if state.has_sender_message_key(iteration):
                mk = state.remove_sender_message_key(iteration)
                if not mk:
                    raise ValueError('No sender message key found for iteration')
                return mk
            raise ValueError(f'Received message with old counter: {chain.get_iteration()}, {iteration}')
        if iteration - chain.get_iteration() > 2000:
            raise ValueError('Over 2000 messages into the future!')
        while chain.get_iteration() < iteration:
            state.add_sender_message_key(chain.get_sender_message_key())
            chain = chain.get_next()
        state.set_sender_chain_key(chain.get_next())
        return chain.get_sender_message_key()
