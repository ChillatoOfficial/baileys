"""LT Hash: summation based hash maintaining the integrity of a piece of data over a series of mutations."""
from __future__ import annotations
import struct
from .crypto import hkdf

_INFO = 'WhatsApp Patch Integrity'


class LTHashAntiTampering:
    salt = _INFO

    def _perform(self, base: bytes, items: list, subtract: bool) -> bytes:
        buf = bytearray(base)
        n = len(buf) // 2
        lanes = list(struct.unpack('<%dH' % n, bytes(buf)))
        for item in items:
            expanded = hkdf(bytes(item), 128, info=_INFO)
            other = struct.unpack('<%dH' % (len(expanded) // 2), expanded)
            if subtract:
                lanes = [(a - b) & 0xFFFF for a, b in zip(lanes, other)]
            else:
                lanes = [(a + b) & 0xFFFF for a, b in zip(lanes, other)]
        return struct.pack('<%dH' % n, *lanes)

    def subtract_then_add(self, base: bytes, subtract: list, add: list) -> bytes:
        return self._perform(self._perform(bytes(base), subtract, True), add, False)

    # JS name
    subtractThenAdd = subtract_then_add


LT_HASH_ANTI_TAMPERING = LTHashAntiTampering()
