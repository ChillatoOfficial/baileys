"""libsignal `crypto` helpers."""
from __future__ import annotations
import hashlib
import hmac
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding


def encrypt(key: bytes, data: bytes, iv: bytes) -> bytes:
    padder = padding.PKCS7(128).padder()
    padded = padder.update(bytes(data)) + padder.finalize()
    enc = Cipher(algorithms.AES(bytes(key)), modes.CBC(bytes(iv))).encryptor()
    return enc.update(padded) + enc.finalize()


def decrypt(key: bytes, data: bytes, iv: bytes) -> bytes:
    dec = Cipher(algorithms.AES(bytes(key)), modes.CBC(bytes(iv))).decryptor()
    padded = dec.update(bytes(data)) + dec.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def calculate_mac(key: bytes, data: bytes) -> bytes:
    return hmac.new(bytes(key), bytes(data), hashlib.sha256).digest()


def hash_(data: bytes) -> bytes:
    return hashlib.sha512(bytes(data)).digest()


def derive_secrets(input_: bytes, salt: bytes, info: bytes, chunks: int = 3) -> list:
    if len(salt) != 32:
        raise ValueError('Got salt of incorrect length')
    prk = hmac.new(bytes(salt), bytes(input_), hashlib.sha256).digest()
    out = []
    prev = b''
    for i in range(1, chunks + 1):
        prev = hmac.new(prk, prev + bytes(info) + bytes([i]), hashlib.sha256).digest()
        out.append(prev)
    return out


def verify_mac(data: bytes, key: bytes, mac: bytes, length: int):
    calculated = calculate_mac(key, data)[:length]
    if len(mac) != length or len(calculated) != length:
        raise ValueError('Bad MAC length')
    if not hmac.compare_digest(calculated, bytes(mac)):
        raise ValueError('Bad MAC')
