from __future__ import annotations


class BinaryInfo:
    def __init__(self, **options):
        self.protocolVersion = 5
        self.sequence = 0
        self.events: list = []  # [{eventName: {'props': {...}, 'globals': {...}}}]
        self.buffer: list = []
        for k, v in options.items():
            setattr(self, k, v)
