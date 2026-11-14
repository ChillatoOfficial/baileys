"""Composable message filters for the high level `Client` API.

    from baileys import filters as f

    @client.message(f.text & ~f.from_me & f.private)
    async def handler(m): ...
"""
from __future__ import annotations

import re
from typing import Callable


class Filter:
    def __init__(self, fn: Callable, name: str = 'filter'):
        self.fn, self.name = fn, name

    def __call__(self, m) -> bool:
        return bool(self.fn(m))

    def __and__(self, other: 'Filter') -> 'Filter':
        return Filter(lambda m: self(m) and other(m), f'({self.name} & {other.name})')

    def __or__(self, other: 'Filter') -> 'Filter':
        return Filter(lambda m: self(m) or other(m), f'({self.name} | {other.name})')

    def __invert__(self) -> 'Filter':
        return Filter(lambda m: not self(m), f'~{self.name}')

    def __repr__(self):
        return f'<Filter {self.name}>'


text = Filter(lambda m: bool(m.text), 'text')
from_me = Filter(lambda m: m.from_me, 'from_me')
group = Filter(lambda m: m.is_group, 'group')
private = Filter(lambda m: not m.is_group, 'private')
media = Filter(lambda m: m.media_type is not None, 'media')
quoted = Filter(lambda m: m.quoted is not None, 'quoted')
everything = Filter(lambda m: True, 'all')


def media_type(*types: str) -> Filter:
    """e.g. media_type('image', 'video')"""
    return Filter(lambda m: m.media_type in types, f'media_type{types}')


def regex(pattern: str, flags: int = re.IGNORECASE) -> Filter:
    rx = re.compile(pattern, flags)
    return Filter(lambda m: bool(m.text and rx.search(m.text)), f'regex({pattern!r})')


def equals(*values: str, ignore_case: bool = True) -> Filter:
    vals = {v.lower() if ignore_case else v for v in values}
    return Filter(lambda m: bool(m.text) and ((m.text.strip().lower() if ignore_case else m.text.strip()) in vals),
                  f'equals{values}')


def chat(*jids: str) -> Filter:
    return Filter(lambda m: m.chat in jids, f'chat{jids}')


def sender(*jids: str) -> Filter:
    return Filter(lambda m: m.sender in jids, f'sender{jids}')
