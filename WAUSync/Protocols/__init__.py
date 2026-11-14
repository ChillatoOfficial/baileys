from __future__ import annotations
from datetime import datetime
from typing import Optional
from ...WABinary import (assert_node_error_free, get_binary_node_child, get_binary_node_child_string,
                         get_binary_node_children)


class USyncQueryProtocol:
    """Defines the interface for a USyncQuery protocol"""
    name: str = ''

    def get_query_element(self) -> dict:
        """Defines what goes inside the query part of a USyncQuery"""
        raise NotImplementedError

    def get_user_element(self, user) -> Optional[dict]:
        """Defines what goes inside the user part of a USyncQuery"""
        raise NotImplementedError

    def parser(self, node: dict):
        """Parse the result of the query"""
        raise NotImplementedError


class USyncBotProfileProtocol(USyncQueryProtocol):
    name = 'bot'

    def get_query_element(self):
        return {'tag': 'bot', 'attrs': {}, 'content': [{'tag': 'profile', 'attrs': {'v': '1'}}]}

    def get_user_element(self, user):
        return {'tag': 'bot', 'attrs': {}, 'content': [{'tag': 'profile', 'attrs': {'persona_id': user.personaId}}]}

    def parser(self, node):
        bot_node = get_binary_node_child(node, 'bot')
        profile = get_binary_node_child(bot_node, 'profile')
        commands_node = get_binary_node_child(profile, 'commands')
        prompts_node = get_binary_node_child(profile, 'prompts')
        commands = [{'name': get_binary_node_child_string(c, 'name'),
                     'description': get_binary_node_child_string(c, 'description')}
                    for c in get_binary_node_children(commands_node, 'command')]
        prompts = [f"{get_binary_node_child_string(p, 'emoji')} {get_binary_node_child_string(p, 'text')}"
                   for p in get_binary_node_children(prompts_node, 'prompt')]
        return {
            'isDefault': bool(get_binary_node_child(profile, 'default')),
            'jid': node['attrs'].get('jid'),
            'name': get_binary_node_child_string(profile, 'name'),
            'attributes': get_binary_node_child_string(profile, 'attributes'),
            'description': get_binary_node_child_string(profile, 'description'),
            'category': get_binary_node_child_string(profile, 'category'),
            'personaId': profile['attrs'].get('persona_id'),
            'commandsDescription': get_binary_node_child_string(commands_node, 'description'),
            'commands': commands,
            'prompts': prompts,
        }


class USyncContactProtocol(USyncQueryProtocol):
    name = 'contact'

    def get_query_element(self):
        return {'tag': 'contact', 'attrs': {}}

    def get_user_element(self, user):
        if user.phone:
            return {'tag': 'contact', 'attrs': {}, 'content': user.phone}
        if user.username:
            attrs = {'username': user.username}
            if user.usernameKey:
                attrs['pin'] = user.usernameKey
            if user.lid:
                attrs['lid'] = user.lid
            return {'tag': 'contact', 'attrs': attrs}
        if user.type:
            return {'tag': 'contact', 'attrs': {'type': user.type}}
        return {'tag': 'contact', 'attrs': {}}

    def parser(self, node):
        if node['tag'] == 'contact':
            assert_node_error_free(node)
            return (node.get('attrs') or {}).get('type') == 'in'
        return False


class USyncDeviceProtocol(USyncQueryProtocol):
    name = 'devices'

    def get_query_element(self):
        return {'tag': 'devices', 'attrs': {'version': '2'}}

    def get_user_element(self, user=None):
        return None

    def parser(self, node):
        device_list = []
        key_index = None
        if node['tag'] == 'devices':
            assert_node_error_free(node)
            device_list_node = get_binary_node_child(node, 'device-list')
            key_index_node = get_binary_node_child(node, 'key-index-list')
            if device_list_node and isinstance(device_list_node.get('content'), list):
                for child in device_list_node['content']:
                    attrs = child['attrs']
                    if child['tag'] == 'device':
                        device_list.append({
                            'id': int(attrs['id']),
                            'keyIndex': int(attrs['key-index']) if attrs.get('key-index') is not None else None,
                            'isHosted': bool(attrs.get('is_hosted') and attrs['is_hosted'] == 'true')})
            if key_index_node and key_index_node['tag'] == 'key-index-list':
                key_index = {'timestamp': int(key_index_node['attrs']['ts']),
                             'signedKeyIndex': key_index_node.get('content'),
                             'expectedTimestamp': int(key_index_node['attrs']['expected_ts'])
                             if key_index_node['attrs'].get('expected_ts') else None}
        return {'deviceList': device_list, 'keyIndex': key_index}


class USyncDisappearingModeProtocol(USyncQueryProtocol):
    name = 'disappearing_mode'

    def get_query_element(self):
        return {'tag': 'disappearing_mode', 'attrs': {}}

    def get_user_element(self, user=None):
        return None

    def parser(self, node):
        if node['tag'] == 'disappearing_mode':
            assert_node_error_free(node)
            duration = int(node['attrs'].get('duration'))
            set_at = datetime.fromtimestamp(int(node['attrs'].get('t') or 0))
            return {'duration': duration, 'setAt': set_at}
        return None


class USyncLIDProtocol(USyncQueryProtocol):
    name = 'lid'

    def get_query_element(self):
        return {'tag': 'lid', 'attrs': {}}

    def get_user_element(self, user):
        if user.lid:
            return {'tag': 'lid', 'attrs': {'jid': user.lid}}
        return None

    def parser(self, node):
        if node['tag'] == 'lid':
            return node['attrs'].get('val')
        return None


class USyncStatusProtocol(USyncQueryProtocol):
    name = 'status'

    def get_query_element(self):
        return {'tag': 'status', 'attrs': {}}

    def get_user_element(self, user=None):
        return None

    def parser(self, node):
        if node['tag'] == 'status':
            assert_node_error_free(node)
            content = node.get('content')
            status = content.decode() if isinstance(content, (bytes, bytearray)) else (
                content if isinstance(content, str) else None)
            set_at = datetime.fromtimestamp(int(node['attrs'].get('t') or 0))
            if not status:
                if node['attrs'].get('code') and int(node['attrs']['code']) == 401:
                    status = ''
                else:
                    status = None
            return {'status': status, 'setAt': set_at}
        return None


class USyncUsernameProtocol(USyncQueryProtocol):
    name = 'username'

    def get_query_element(self):
        return {'tag': 'username', 'attrs': {}}

    def get_user_element(self, user):
        return None

    def parser(self, node):
        if node['tag'] == 'username':
            assert_node_error_free(node)
            return node['content'] if isinstance(node.get('content'), str) else None
        return None
