from __future__ import annotations
from typing import Optional


def build_ack_stanza(node: dict, error_code: Optional[int] = None, me_id: Optional[str] = None) -> dict:
    """Builds an ACK stanza for a received node (mirrors WhatsApp Web's ACK construction)."""
    tag, attrs = node['tag'], node['attrs']
    stanza = {'tag': 'ack', 'attrs': {'id': attrs.get('id'), 'to': attrs.get('from'), 'class': tag}}
    if error_code:
        stanza['attrs']['error'] = str(error_code)
    if attrs.get('participant'):
        stanza['attrs']['participant'] = attrs['participant']
    if attrs.get('recipient'):
        stanza['attrs']['recipient'] = attrs['recipient']
    if attrs.get('type'):  # WA Web always includes type when present
        stanza['attrs']['type'] = attrs['type']
    if tag == 'message' and me_id:  # WA Web always includes `from` for message-class ACKs
        stanza['attrs']['from'] = me_id
    return stanza
