from __future__ import annotations
import struct
from typing import Callable, Optional
from ..Defaults import NOISE_MODE, WA_CERT_DETAILS
from ..WABinary import Boom, decode_binary_node
from ..WAProto import WAProto_pb2 as proto
from .crypto import Curve, aes_decrypt_gcm, aes_encrypt_gcm, hkdf, sha256

IV_LENGTH = 12
EMPTY = b''


def _generate_iv(counter: int) -> bytes:
    return b'\x00' * 8 + struct.pack('>I', counter & 0xFFFFFFFF)


class TransportState:
    def __init__(self, enc_key: bytes, dec_key: bytes):
        self.enc_key, self.dec_key = enc_key, dec_key
        self.read_counter = 0
        self.write_counter = 0

    def encrypt(self, plaintext: bytes) -> bytes:
        c = self.write_counter
        self.write_counter += 1
        return aes_encrypt_gcm(plaintext, self.enc_key, _generate_iv(c), EMPTY)

    def decrypt(self, ciphertext: bytes) -> bytes:
        c = self.read_counter
        self.read_counter += 1
        return aes_decrypt_gcm(ciphertext, self.dec_key, _generate_iv(c), EMPTY)


def make_noise_handler(key_pair: dict, NOISE_HEADER: bytes, logger, routing_info: Optional[bytes] = None):
    private_key, public_key = key_pair['private'], key_pair['public']
    logger = logger.child({'class': 'ns'})

    data = NOISE_MODE.encode()
    st = {
        'hash': data if len(data) == 32 else sha256(data),
        'counter': 0,
        'sent_intro': False,
        'in_bytes': b'',
        'transport': None,
    }
    st['salt'] = st['enc_key'] = st['dec_key'] = st['hash']

    if routing_info:
        ri = bytes(routing_info)
        intro_header = (b'ED' + bytes([0, 1, len(ri) >> 16]) + struct.pack('>H', len(ri) & 65535) + ri +
                        bytes(NOISE_HEADER))
    else:
        intro_header = bytes(NOISE_HEADER)

    def authenticate(d: bytes):
        if not st['transport']:
            st['hash'] = sha256(st['hash'] + bytes(d))

    def encrypt(plaintext: bytes) -> bytes:
        if st['transport']:
            return st['transport'].encrypt(plaintext)
        c = st['counter']
        st['counter'] += 1
        result = aes_encrypt_gcm(plaintext, st['enc_key'], _generate_iv(c), st['hash'])
        authenticate(result)
        return result

    def decrypt(ciphertext: bytes) -> bytes:
        if st['transport']:
            return st['transport'].decrypt(ciphertext)
        c = st['counter']
        st['counter'] += 1
        result = aes_decrypt_gcm(ciphertext, st['dec_key'], _generate_iv(c), st['hash'])
        authenticate(ciphertext)
        return result

    def local_hkdf(d: bytes):
        key = hkdf(bytes(d), 64, salt=st['salt'], info='')
        return key[:32], key[32:]

    def mix_into_key(d: bytes):
        write, read = local_hkdf(d)
        st['salt'] = write
        st['enc_key'] = read
        st['dec_key'] = read
        st['counter'] = 0

    async def finish_init():
        write, read = local_hkdf(b'')
        st['transport'] = TransportState(write, read)
        logger.trace('Noise handler transitioned to Transport state')

    def process_handshake(server_hello, noise_key: dict) -> bytes:
        authenticate(server_hello.ephemeral)
        mix_into_key(Curve.shared_key(private_key, server_hello.ephemeral))
        dec_static = decrypt(server_hello.static)
        mix_into_key(Curve.shared_key(private_key, dec_static))
        cert_decoded = decrypt(server_hello.payload)
        chain = proto.CertChain()
        chain.ParseFromString(cert_decoded)
        leaf, intermediate = chain.leaf, chain.intermediate
        if not leaf.details or not leaf.signature:
            raise Boom('invalid noise leaf certificate', status_code=400)
        if not intermediate.details or not intermediate.signature:
            raise Boom('invalid noise intermediate certificate', status_code=400)
        details = proto.CertChain.NoiseCertificate.Details()
        details.ParseFromString(intermediate.details)
        verify = Curve.verify(details.key, leaf.details, leaf.signature)
        verify_intermediate = Curve.verify(WA_CERT_DETAILS['PUBLIC_KEY'], intermediate.details, intermediate.signature)
        if not verify:
            raise Boom('noise certificate signature invalid', status_code=400)
        if not verify_intermediate:
            raise Boom('noise intermediate certificate signature invalid', status_code=400)
        if details.issuerSerial != WA_CERT_DETAILS['SERIAL']:
            raise Boom('certification match failed', status_code=400)
        key_enc = encrypt(noise_key['public'])
        mix_into_key(Curve.shared_key(noise_key['private'], server_hello.ephemeral))
        return key_enc

    def encode_frame(d: bytes) -> bytes:
        d = bytes(d)
        if st['transport']:
            d = st['transport'].encrypt(d)
        n = len(d)
        intro = b'' if st['sent_intro'] else intro_header
        st['sent_intro'] = True
        return intro + bytes([(n >> 16) & 0xFF, (n >> 8) & 0xFF, n & 0xFF]) + d

    async def decode_frame(new_data: bytes, on_frame: Callable):
        st['in_bytes'] = st['in_bytes'] + bytes(new_data) if st['in_bytes'] else bytes(new_data)
        await process_data(on_frame)

    async def process_data(on_frame: Callable):
        while True:
            buf = st['in_bytes']
            if len(buf) < 3:
                return
            size = (buf[0] << 16) | (buf[1] << 8) | buf[2]
            if len(buf) < size + 3:
                return
            frame = buf[3:size + 3]
            st['in_bytes'] = buf[size + 3:]
            if st['transport']:
                result = st['transport'].decrypt(frame)
                frame = decode_binary_node(result)
            r = on_frame(frame)
            if hasattr(r, '__await__'):
                await r

    authenticate(NOISE_HEADER)
    authenticate(public_key)

    class Handler:
        pass
    h = Handler()
    h.encrypt, h.decrypt, h.authenticate, h.mix_into_key = encrypt, decrypt, authenticate, mix_into_key
    h.finish_init, h.process_handshake = finish_init, process_handshake
    h.encode_frame, h.decode_frame = encode_frame, decode_frame
    return h
