"""Curve25519 / XEdDSA primitives compatible with libsignal's `curve` module.

Public keys exposed here are 33 bytes (0x05 prefix) just like libsignal; helpers
accept 32 byte keys as well.
"""
from __future__ import annotations
import hashlib
import os
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature

_P = 2 ** 255 - 19
_Q = 2 ** 252 + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _inv(x):
    return pow(x, _P - 2, _P)


def _recover_x(y, sign):
    xx = (y * y - 1) * _inv(_D * y * y + 1) % _P
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = x * _I % _P
    if (x * x - xx) % _P != 0:
        raise ValueError('invalid point')
    if (x & 1) != sign:
        x = _P - x
    return x


_By = 4 * _inv(5) % _P
_Bx = _recover_x(_By, 0)
_B = (_Bx, _By, 1, _Bx * _By % _P)


def _add(p1, p2):
    x1, y1, z1, t1 = p1
    x2, y2, z2, t2 = p2
    a = (y1 - x1) * (y2 - x2) % _P
    b = (y1 + x1) * (y2 + x2) % _P
    c = 2 * t1 * t2 * _D % _P
    d = 2 * z1 * z2 % _P
    e, f, g, h = b - a, d - c, d + c, b + a
    return (e * f % _P, g * h % _P, f * g % _P, e * h % _P)


def _mul(s, p):
    q = (0, 1, 1, 0)
    while s > 0:
        if s & 1:
            q = _add(q, p)
        p = _add(p, p)
        s >>= 1
    return q


def _compress(p):
    x, y, z, _ = p
    zi = _inv(z)
    x, y = x * zi % _P, y * zi % _P
    return (y | ((x & 1) << 255)).to_bytes(32, 'little')


def _sha512(*parts) -> bytes:
    h = hashlib.sha512()
    for p in parts:
        h.update(p)
    return h.digest()


def _strip(pub: bytes) -> bytes:
    pub = bytes(pub)
    if len(pub) == 33:
        if pub[0] != 5:
            raise ValueError('Invalid public key')
        return pub[1:]
    if len(pub) != 32:
        raise ValueError('Invalid public key')
    return pub


def generate_key_pair() -> dict:
    """Returns {'privKey': 32 bytes, 'pubKey': 33 bytes (0x05 prefixed)}."""
    priv = bytearray(os.urandom(32))
    priv[0] &= 248
    priv[31] &= 127
    priv[31] |= 64
    priv = bytes(priv)
    return {'privKey': priv, 'pubKey': create_key_pair(priv)['pubKey']}


def create_key_pair(priv: bytes) -> dict:
    pub = X25519PrivateKey.from_private_bytes(bytes(priv)).public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return {'privKey': bytes(priv), 'pubKey': b'\x05' + pub}


def calculate_agreement(pub_key: bytes, priv_key: bytes) -> bytes:
    pub = _strip(pub_key)
    return X25519PrivateKey.from_private_bytes(bytes(priv_key)).exchange(X25519PublicKey.from_public_bytes(pub))


def calculate_signature(priv_key: bytes, message: bytes) -> bytes:
    """XEdDSA signature (64 bytes)."""
    k = int.from_bytes(bytes(priv_key), 'little')
    # clamp
    k &= ~7
    k &= (1 << 255) - 1
    k |= 1 << 254
    e = _mul(k, _B)
    pub = _compress(e)
    a = k
    if pub[31] & 0x80:
        a = (-k) % _Q
        pub = bytes(pub[:31]) + bytes([pub[31] & 0x7F])
    a_bytes = a.to_bytes(32, 'little')
    z = os.urandom(64)
    r = int.from_bytes(_sha512(b'\xfe' + b'\xff' * 31, a_bytes, bytes(message), z), 'little') % _Q
    big_r = _compress(_mul(r, _B))
    h = int.from_bytes(_sha512(big_r, pub, bytes(message)), 'little') % _Q
    s = (r + h * a) % _Q
    return big_r + s.to_bytes(32, 'little')


def verify_signature(pub_key: bytes, message: bytes, signature: bytes, is_init: bool = False) -> bool:
    """Returns True when valid, otherwise raises (`verify` returns bool for convenience)."""
    ok = verify(pub_key, message, signature)
    if not ok:
        raise ValueError('Invalid signature')
    return True


def verify(pub_key: bytes, message: bytes, signature: bytes) -> bool:
    try:
        u = int.from_bytes(_strip(pub_key), 'little') & ((1 << 255) - 1)
        signature = bytes(signature)
        if len(signature) != 64:
            return False
        sign = signature[63] >> 7
        sig = signature[:63] + bytes([signature[63] & 0x7F])
        y = (u - 1) * _inv(u + 1) % _P
        ed_pub = (y | (sign << 255)).to_bytes(32, 'little')
        Ed25519PublicKey.from_public_bytes(ed_pub).verify(sig, bytes(message))
        return True
    except (InvalidSignature, ValueError, Exception):
        return False
