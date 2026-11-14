from __future__ import annotations
import hashlib
import hmac as _hmac
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand, HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import padding

from ..Defaults import KEY_BUNDLE_TYPE
from ..Signal import curve


def md5(data: bytes) -> bytes:
    return hashlib.md5(bytes(data)).digest()


def hkdf(buffer: bytes, expanded_length: int, salt: bytes | None = None, info: str | bytes = b'') -> bytes:
    """HKDF-SHA256 (extract + expand). Mirrors `hkdf(buffer, length, {salt, info})`."""
    if isinstance(info, str):
        info = info.encode()
    return HKDF(algorithm=hashes.SHA256(), length=expanded_length, salt=bytes(salt) if salt else None,
                info=info).derive(bytes(buffer))


def generate_signal_pub_key(pub_key: bytes) -> bytes:
    """prefix version byte to the pub keys, required for some curve crypto functions"""
    pub_key = bytes(pub_key)
    return pub_key if len(pub_key) == 33 else KEY_BUNDLE_TYPE + pub_key


class Curve:
    @staticmethod
    def generate_key_pair() -> dict:
        kp = curve.generate_key_pair()
        return {'private': kp['privKey'], 'public': kp['pubKey'][1:]}

    @staticmethod
    def shared_key(private_key: bytes, public_key: bytes) -> bytes:
        return curve.calculate_agreement(generate_signal_pub_key(public_key), bytes(private_key))

    @staticmethod
    def sign(private_key: bytes, buf: bytes) -> bytes:
        return curve.calculate_signature(bytes(private_key), bytes(buf))

    @staticmethod
    def verify(pub_key: bytes, message: bytes, signature: bytes) -> bool:
        try:
            return curve.verify(generate_signal_pub_key(pub_key), bytes(message), bytes(signature))
        except Exception:
            return False


def signed_key_pair(identity_key_pair: dict, key_id: int) -> dict:
    pre_key = Curve.generate_key_pair()
    pub_key = generate_signal_pub_key(pre_key['public'])
    signature = Curve.sign(identity_key_pair['private'], pub_key)
    return {'keyPair': pre_key, 'signature': signature, 'keyId': key_id}


GCM_TAG_LENGTH = 16


def aes_encrypt_gcm(plaintext: bytes, key: bytes, iv: bytes, additional_data: bytes) -> bytes:
    """AES-256-GCM; the tag is suffixed to the ciphertext."""
    return AESGCM(bytes(key)).encrypt(bytes(iv), bytes(plaintext), bytes(additional_data) or None)


def aes_decrypt_gcm(ciphertext: bytes, key: bytes, iv: bytes, additional_data: bytes) -> bytes:
    return AESGCM(bytes(key)).decrypt(bytes(iv), bytes(ciphertext), bytes(additional_data) or None)


def aes_encrypt_ctr(plaintext: bytes, key: bytes, iv: bytes) -> bytes:
    c = Cipher(algorithms.AES(bytes(key)), modes.CTR(bytes(iv))).encryptor()
    return c.update(bytes(plaintext)) + c.finalize()


def aes_decrypt_ctr(ciphertext: bytes, key: bytes, iv: bytes) -> bytes:
    c = Cipher(algorithms.AES(bytes(key)), modes.CTR(bytes(iv))).decryptor()
    return c.update(bytes(ciphertext)) + c.finalize()


def aes_decrypt_with_iv(buffer: bytes, key: bytes, iv: bytes) -> bytes:
    d = Cipher(algorithms.AES(bytes(key)), modes.CBC(bytes(iv))).decryptor()
    padded = d.update(bytes(buffer)) + d.finalize()
    u = padding.PKCS7(128).unpadder()
    return u.update(padded) + u.finalize()


def aes_decrypt(buffer: bytes, key: bytes) -> bytes:
    """decrypt AES 256 CBC; where the IV is prefixed to the buffer"""
    buffer = bytes(buffer)
    return aes_decrypt_with_iv(buffer[16:], key, buffer[:16])


def aes_encrypt_with_iv(buffer: bytes, key: bytes, iv: bytes) -> bytes:
    p = padding.PKCS7(128).padder()
    padded = p.update(bytes(buffer)) + p.finalize()
    e = Cipher(algorithms.AES(bytes(key)), modes.CBC(bytes(iv))).encryptor()
    return e.update(padded) + e.finalize()


def aes_encrypt(buffer: bytes, key: bytes) -> bytes:
    iv = os.urandom(16)
    return iv + aes_encrypt_with_iv(buffer, key, iv)


def hmac_sign(buffer: bytes, key: bytes, variant: str = 'sha256') -> bytes:
    return _hmac.new(bytes(key), bytes(buffer), getattr(hashlib, variant)).digest()


def sha256(buffer: bytes) -> bytes:
    return hashlib.sha256(bytes(buffer)).digest()


async def derive_pairing_code_key(pairing_code: str, salt: bytes) -> bytes:
    import asyncio
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=bytes(salt), iterations=2 << 16)
    return await asyncio.get_running_loop().run_in_executor(None, kdf.derive, pairing_code.encode())
