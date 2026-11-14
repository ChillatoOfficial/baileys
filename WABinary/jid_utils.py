from __future__ import annotations
import re
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

S_WHATSAPP_NET = '@s.whatsapp.net'
OFFICIAL_BIZ_JID = '16505361212@c.us'
SERVER_JID = 'server@c.us'
PSA_WID = '0@c.us'
STORIES_JID = 'status@broadcast'
META_AI_JID = '13135550002@c.us'


class WAJIDDomains(IntEnum):
    WHATSAPP = 0
    LID = 1
    HOSTED = 128
    HOSTED_LID = 129


@dataclass
class FullJid:
    user: str
    server: str
    device: Optional[int] = None
    domainType: Optional[int] = None

    # dict-style access so it behaves like the JS object
    def __getitem__(self, k):
        return getattr(self, k)

    def get(self, k, default=None):
        return getattr(self, k, default)

    def to_dict(self) -> dict:
        return {'user': self.user, 'server': self.server, 'device': self.device, 'domainType': self.domainType}


def get_server_from_domain_type(initial_server: str, domain_type=None) -> str:
    if domain_type == WAJIDDomains.LID:
        return 'lid'
    if domain_type == WAJIDDomains.HOSTED:
        return 'hosted'
    if domain_type == WAJIDDomains.HOSTED_LID:
        return 'hosted.lid'
    return initial_server


def jid_encode(user, server: str, device=None, agent=None) -> str:
    return f"{user or ''}{f'_{agent}' if agent else ''}{f':{device}' if device else ''}@{server}"


def jid_decode(jid) -> Optional[FullJid]:
    if not isinstance(jid, str):
        return None
    sep = jid.find('@')
    if sep < 0:
        return None
    server = jid[sep + 1:]
    user_combined = jid[:sep]
    parts = user_combined.split(':')
    user_agent = parts[0]
    device = parts[1] if len(parts) > 1 else None
    ua = user_agent.split('_')
    user = ua[0]
    agent = ua[1] if len(ua) > 1 else None
    domain_type = WAJIDDomains.WHATSAPP
    if server == 'lid':
        domain_type = WAJIDDomains.LID
    elif server == 'hosted':
        domain_type = WAJIDDomains.HOSTED
    elif server == 'hosted.lid':
        domain_type = WAJIDDomains.HOSTED_LID
    elif agent:
        try:
            domain_type = int(agent)
        except ValueError:
            domain_type = 0
    return FullJid(user=user, server=server, domainType=int(domain_type),
                   device=int(device) if device else None)


def are_jids_same_user(jid1, jid2) -> bool:
    a, b = jid_decode(jid1), jid_decode(jid2)
    return (a.user if a else None) == (b.user if b else None)


def _ends(jid, suffix) -> bool:
    return isinstance(jid, str) and jid.endswith(suffix)


def is_jid_meta_ai(jid): return _ends(jid, '@bot')
def is_pn_user(jid): return _ends(jid, '@s.whatsapp.net')
def is_lid_user(jid): return _ends(jid, '@lid')
def is_jid_broadcast(jid): return _ends(jid, '@broadcast')
def is_jid_group(jid): return _ends(jid, '@g.us')
def is_jid_status_broadcast(jid): return jid == 'status@broadcast'
def is_jid_newsletter(jid): return _ends(jid, '@newsletter')
def is_hosted_pn_user(jid): return _ends(jid, '@hosted')
def is_hosted_lid_user(jid): return _ends(jid, '@hosted.lid')


_BOT_RE = re.compile(r'^1313555\d{4}$|^131655500\d{2}$')


def is_jid_bot(jid) -> bool:
    return bool(jid) and bool(_BOT_RE.match(jid.split('@')[0])) and jid.endswith('@c.us')


def jid_normalized_user(jid) -> str:
    r = jid_decode(jid)
    if not r:
        return ''
    return jid_encode(r.user, 's.whatsapp.net' if r.server == 'c.us' else r.server)


def transfer_device(from_jid: str, to_jid: str) -> str:
    f = jid_decode(from_jid)
    device_id = (f.device if f else 0) or 0
    t = jid_decode(to_jid)
    return jid_encode(t.user, t.server, device_id)
