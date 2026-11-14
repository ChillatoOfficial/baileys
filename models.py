"""Rich objects built from raw WhatsApp dicts: `Group`, `Contact`."""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional, Union

from .WABinary.jid_utils import jid_normalized_user

if TYPE_CHECKING:  # pragma: no cover
    from .client import Client
    from .schemas import ContactInfo, GroupMetadata, Participant


def _norm(jid: Optional[str]) -> str:
    return jid_normalized_user(jid) if jid else ''


class Group:
    """A group with cached metadata. Get it with `await client.group(jid)` or `await m.group()`."""

    def __init__(self, client: 'Client', meta: 'GroupMetadata'):
        self.client, self.meta = client, meta

    # --- data ----------------------------------------------------------------------------------------------
    @property
    def id(self) -> str:
        return self.meta['id']

    @property
    def subject(self) -> str:
        return self.meta.get('subject') or ''

    name = subject

    @property
    def description(self) -> Optional[str]:
        return self.meta.get('desc')

    @property
    def owner(self) -> Optional[str]:
        return self.meta.get('owner')

    @property
    def participants(self) -> 'list[Participant]':
        return self.meta.get('participants') or []

    @property
    def size(self) -> int:
        return self.meta.get('size') or len(self.participants)

    @property
    def only_admins_can_send(self) -> bool:
        return bool(self.meta.get('announce'))

    @property
    def only_admins_can_edit(self) -> bool:
        return bool(self.meta.get('restrict'))

    @staticmethod
    def _ids(p: dict) -> set[str]:
        """Every identity a participant is known by (LID and phone-number JIDs)."""
        return {_norm(x) for x in (p.get('id'), p.get('lid'), p.get('phoneNumber')) if x}

    def find(self, jid: str) -> Optional[dict]:
        j = _norm(jid)
        return next((p for p in self.participants if j in self._ids(p)), None)

    def has(self, jid: str) -> bool:
        return self.find(jid) is not None

    @property
    def admins(self) -> list[dict]:
        return [p for p in self.participants if p.get('admin')]

    def is_admin(self, jid: str) -> bool:
        p = self.find(jid)
        return bool(p and p.get('admin'))

    def is_superadmin(self, jid: str) -> bool:
        p = self.find(jid)
        return bool(p and p.get('admin') == 'superadmin')

    @property
    def me(self) -> Optional[dict]:
        creds = self.client.sock.auth_state['creds'].get('me') or {}
        return self.find(creds.get('id', '')) or self.find(creds.get('lid', ''))

    @property
    def bot_is_admin(self) -> bool:
        me = self.me
        return bool(me and me.get('admin'))

    # --- actions -------------------------------------------------------------------------------------------
    async def refresh(self) -> 'Group':
        return await self.client.group(self.id, refresh=True)

    async def _update(self, jids: Union[str, Iterable[str]], action: str):
        jids = [jids] if isinstance(jids, str) else list(jids)
        res = await self.client.sock.group_participants_update(self.id, jids, action)
        self.client._groups.pop(self.id, None)
        return res

    async def add(self, *jids: str):
        return await self._update(jids, 'add')

    async def remove(self, *jids: str):
        return await self._update(jids, 'remove')

    async def promote(self, *jids: str):
        return await self._update(jids, 'promote')

    async def demote(self, *jids: str):
        return await self._update(jids, 'demote')

    async def rename(self, subject: str):
        await self.client.sock.group_update_subject(self.id, subject)
        self.client._groups.pop(self.id, None)

    async def set_description(self, text: Optional[str]):
        await self.client.sock.group_update_description(self.id, text)
        self.client._groups.pop(self.id, None)

    async def lock(self, only_admins_send: bool = True):
        await self.client.sock.group_setting_update(self.id, 'announcement' if only_admins_send else 'not_announcement')
        self.client._groups.pop(self.id, None)

    async def invite_link(self) -> str:
        return 'https://chat.whatsapp.com/' + await self.client.sock.group_invite_code(self.id)

    async def revoke_invite(self) -> str:
        return 'https://chat.whatsapp.com/' + await self.client.sock.group_revoke_invite(self.id)

    async def leave(self):
        await self.client.sock.group_leave(self.id)

    async def send(self, content, **options):
        return await self.client.send(self.id, content, **options)

    async def mention_all(self, text: str = '', **options):
        """Send `text` mentioning every member (appends @user for each member)."""
        jids = [p['id'] for p in self.participants]
        tags = ' '.join('@' + j.split('@')[0].split(':')[0] for j in jids)
        return await self.client.send(self.id, {'text': f'{text}\n{tags}'.strip(), 'mentions': jids}, **options)

    def __repr__(self):
        return f'<Group {self.id} {self.subject!r} size={self.size}>'


class Contact:
    """Name/identity info from the contacts the server synced."""

    def __init__(self, client: 'Client', data: 'ContactInfo'):
        self.client, self.data = client, data

    @property
    def id(self) -> str:
        return self.data.get('id')

    @property
    def lid(self) -> Optional[str]:
        return self.data.get('lid')

    @property
    def name(self) -> Optional[str]:
        return self.data.get('name') or self.data.get('verifiedName') or self.data.get('notify')

    @property
    def push_name(self) -> Optional[str]:
        return self.data.get('notify')

    @property
    def number(self) -> Optional[str]:
        return self.id.split('@')[0].split(':')[0] if self.id and self.id.endswith('@s.whatsapp.net') else None

    async def picture_url(self, high_res: bool = False) -> Optional[str]:
        try:
            return await self.client.sock.profile_picture_url(self.id, 'image' if high_res else 'preview')
        except Exception:
            return None

    async def send(self, content, **options):
        return await self.client.send(self.id, content, **options)

    def __repr__(self):
        return f'<Contact {self.id} {self.name!r}>'
