from __future__ import annotations
import base64
from typing import Optional

from ..Types import QueryIds, XWAPaths
from ..Utils.messages_media import generate_profile_picture
from ..WABinary import get_binary_node_child
from .groups import make_groups_socket
from .mex import execute_w_mex_query as generic_execute_w_mex_query


def _parse_newsletter_create_response(response: dict) -> dict:
    thread, viewer = response['thread_metadata'], response['viewer_metadata']
    return {
        'id': response['id'],
        'owner': None,
        'name': thread['name']['text'],
        'creation_time': int(thread['creation_time']),
        'description': thread['description']['text'],
        'invite': thread['invite'],
        'subscribers': int(thread['subscribers_count']),
        'verification': thread['verification'],
        'picture': {'id': thread['picture']['id'], 'directPath': thread['picture']['direct_path']},
        'mute_state': viewer['mute'],
    }


def _parse_newsletter_metadata(result) -> Optional[dict]:
    if not isinstance(result, dict):
        return None
    if isinstance(result.get('id'), str):
        return result
    if isinstance(result.get('result'), dict) and 'id' in result['result']:
        return result['result']
    return None


def make_newsletter_socket(config: dict):
    sock = make_groups_socket(config)
    query, generate_message_tag = sock.query, sock.generate_message_tag

    def execute_w_mex_query(variables: dict, query_id, data_path):
        return generic_execute_w_mex_query(variables, getattr(query_id, 'value', query_id),
                                           getattr(data_path, 'value', data_path), query, generate_message_tag)

    async def newsletter_update(jid: str, updates: dict):
        variables = {'newsletter_id': jid, 'updates': {**updates, 'settings': None}}
        return await execute_w_mex_query(variables, QueryIds.UPDATE_METADATA, 'xwa2_newsletter_update')

    async def newsletter_create(name: str, description: Optional[str] = None) -> dict:
        variables = {'input': {'name': name, 'description': description}}
        raw = await execute_w_mex_query(variables, QueryIds.CREATE, XWAPaths.xwa2_newsletter_create)
        return _parse_newsletter_create_response(raw)

    async def newsletter_subscribers(jid: str):
        return await execute_w_mex_query({'newsletter_id': jid}, QueryIds.SUBSCRIBERS,
                                         XWAPaths.xwa2_newsletter_subscribers)

    async def newsletter_metadata(type_: str, key: str):
        variables = {'fetch_creation_time': True, 'fetch_full_image': True, 'fetch_viewer_metadata': True,
                     'input': {'key': key, 'type': type_.upper()}}
        result = await execute_w_mex_query(variables, QueryIds.METADATA, XWAPaths.xwa2_newsletter_metadata)
        return _parse_newsletter_metadata(result)

    def newsletter_follow(jid: str):
        return execute_w_mex_query({'newsletter_id': jid}, QueryIds.FOLLOW, XWAPaths.xwa2_newsletter_join_v2)

    def newsletter_unfollow(jid: str):
        return execute_w_mex_query({'newsletter_id': jid}, QueryIds.UNFOLLOW, XWAPaths.xwa2_newsletter_leave_v2)

    def newsletter_mute(jid: str):
        return execute_w_mex_query({'newsletter_id': jid}, QueryIds.MUTE, XWAPaths.xwa2_newsletter_mute_v2)

    def newsletter_unmute(jid: str):
        return execute_w_mex_query({'newsletter_id': jid}, QueryIds.UNMUTE, XWAPaths.xwa2_newsletter_unmute_v2)

    async def newsletter_update_name(jid: str, name: str):
        return await newsletter_update(jid, {'name': name})

    async def newsletter_update_description(jid: str, description: str):
        return await newsletter_update(jid, {'description': description})

    async def newsletter_update_picture(jid: str, content):
        img = (await generate_profile_picture(content))['img']
        return await newsletter_update(jid, {'picture': base64.b64encode(img).decode()})

    async def newsletter_remove_picture(jid: str):
        return await newsletter_update(jid, {'picture': ''})

    async def newsletter_react_message(jid: str, server_id: str, reaction: Optional[str] = None):
        await query({'tag': 'message',
                     'attrs': {'to': jid, **({} if reaction else {'edit': '7'}), 'type': 'reaction',
                               'server_id': server_id, 'id': generate_message_tag()},
                     'content': [{'tag': 'reaction', 'attrs': {'code': reaction} if reaction else {}}]})

    async def newsletter_fetch_messages(jid: str, count: int, since: Optional[int] = None,
                                        after: Optional[int] = None):
        message_update_attrs = {'count': str(count)}
        if isinstance(since, (int, float)):
            message_update_attrs['since'] = str(since)
        if after:
            message_update_attrs['after'] = str(after)
        return await query({'tag': 'iq', 'attrs': {'id': generate_message_tag(), 'type': 'get',
                                                    'xmlns': 'newsletter', 'to': jid},
                            'content': [{'tag': 'message_updates', 'attrs': message_update_attrs}]})

    async def subscribe_newsletter_updates(jid: str) -> Optional[dict]:
        result = await query({'tag': 'iq', 'attrs': {'id': generate_message_tag(), 'type': 'set',
                                                      'xmlns': 'newsletter', 'to': jid},
                              'content': [{'tag': 'live_updates', 'attrs': {}, 'content': []}]})
        live_updates_node = get_binary_node_child(result, 'live_updates')
        duration = ((live_updates_node or {}).get('attrs') or {}).get('duration')
        return {'duration': duration} if duration else None

    async def newsletter_admin_count(jid: str) -> int:
        response = await execute_w_mex_query({'newsletter_id': jid}, QueryIds.ADMIN_COUNT,
                                             XWAPaths.xwa2_newsletter_admin_count)
        return response['admin_count']

    async def newsletter_change_owner(jid: str, new_owner_jid: str):
        await execute_w_mex_query({'newsletter_id': jid, 'user_id': new_owner_jid}, QueryIds.CHANGE_OWNER,
                                  XWAPaths.xwa2_newsletter_change_owner)

    async def newsletter_demote(jid: str, user_jid: str):
        await execute_w_mex_query({'newsletter_id': jid, 'user_id': user_jid}, QueryIds.DEMOTE,
                                  XWAPaths.xwa2_newsletter_demote)

    async def newsletter_delete(jid: str):
        await execute_w_mex_query({'newsletter_id': jid}, QueryIds.DELETE, XWAPaths.xwa2_newsletter_delete_v2)

    return sock.extend(
        newsletter_create=newsletter_create, newsletter_update=newsletter_update,
        newsletter_subscribers=newsletter_subscribers, newsletter_metadata=newsletter_metadata,
        newsletter_follow=newsletter_follow, newsletter_unfollow=newsletter_unfollow,
        newsletter_mute=newsletter_mute, newsletter_unmute=newsletter_unmute,
        newsletter_update_name=newsletter_update_name, newsletter_update_description=newsletter_update_description,
        newsletter_update_picture=newsletter_update_picture, newsletter_remove_picture=newsletter_remove_picture,
        newsletter_react_message=newsletter_react_message, newsletter_fetch_messages=newsletter_fetch_messages,
        subscribe_newsletter_updates=subscribe_newsletter_updates, newsletter_admin_count=newsletter_admin_count,
        newsletter_change_owner=newsletter_change_owner, newsletter_demote=newsletter_demote,
        newsletter_delete=newsletter_delete)
