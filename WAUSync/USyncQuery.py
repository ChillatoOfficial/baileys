from __future__ import annotations
from typing import Optional
from ..WABinary import get_binary_node_child
from .Protocols import (USyncBotProfileProtocol, USyncContactProtocol, USyncDeviceProtocol,
                        USyncDisappearingModeProtocol, USyncLIDProtocol, USyncQueryProtocol, USyncStatusProtocol,
                        USyncUsernameProtocol)
from .USyncUser import USyncUser


class USyncQuery:
    def __init__(self):
        self.protocols: list[USyncQueryProtocol] = []
        self.users: list[USyncUser] = []
        self.context = 'interactive'
        self.mode = 'query'

    def with_mode(self, mode: str):
        self.mode = mode
        return self

    def with_context(self, context: str):
        self.context = context
        return self

    def with_user(self, user: USyncUser):
        self.users.append(user)
        return self

    def parse_usync_query_result(self, result: Optional[dict]) -> Optional[dict]:
        if not result or result['attrs'].get('type') != 'result':
            return None
        protocol_map = {p.name: p.parser for p in self.protocols}
        query_result = {'list': [], 'sideList': []}
        usync_node = get_binary_node_child(result, 'usync')
        list_node = get_binary_node_child(usync_node, 'list') if usync_node else None
        if list_node and isinstance(list_node.get('content'), list):
            for node in list_node['content']:
                id_ = (node.get('attrs') or {}).get('jid')
                if id_:
                    data = {}
                    if isinstance(node.get('content'), list):
                        for content in node['content']:
                            parser = protocol_map.get(content['tag'])
                            if parser:
                                v = parser(content)
                                if v is not None:
                                    data[content['tag']] = v
                    query_result['list'].append({**data, 'id': id_})
        return query_result

    def with_device_protocol(self): self.protocols.append(USyncDeviceProtocol()); return self
    def with_contact_protocol(self): self.protocols.append(USyncContactProtocol()); return self
    def with_status_protocol(self): self.protocols.append(USyncStatusProtocol()); return self
    def with_disappearing_mode_protocol(self): self.protocols.append(USyncDisappearingModeProtocol()); return self
    def with_bot_profile_protocol(self): self.protocols.append(USyncBotProfileProtocol()); return self
    def with_lid_protocol(self): self.protocols.append(USyncLIDProtocol()); return self
    def with_username_protocol(self): self.protocols.append(USyncUsernameProtocol()); return self
