from __future__ import annotations
from typing import Optional


class USyncUser:
    def __init__(self):
        self.id: Optional[str] = None
        self.lid: Optional[str] = None
        self.phone: Optional[str] = None
        self.username: Optional[str] = None
        self.usernameKey: Optional[str] = None
        self.type: Optional[str] = None
        self.personaId: Optional[str] = None

    def with_id(self, id_: str): self.id = id_; return self
    def with_lid(self, lid: str): self.lid = lid; return self
    def with_phone(self, phone: str): self.phone = phone; return self
    def with_username(self, username: str): self.username = username; return self
    def with_username_key(self, key: str): self.usernameKey = key; return self
    def with_type(self, type_: str): self.type = type_; return self
    def with_persona_id(self, persona_id: str): self.personaId = persona_id; return self
