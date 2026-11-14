from __future__ import annotations
import json
from typing import Optional

from ..Types import WAMessageStubType
from ..Utils.generics import generate_message_id, generate_message_id_v2, unix_timestamp_seconds
from ..Utils.logger import logger
from ..Utils.proto_utils import normalize
from ..WABinary import (get_binary_node_child, get_binary_node_child_string, get_binary_node_children, jid_encode,
                        jid_normalized_user)
from ..WAProto import WAProto_pb2 as proto
from .business import make_business_socket


def make_communities_socket(config: dict):
    sock = make_business_socket(config)
    auth_state, ev, query, upsert_message = sock.auth_state, sock.ev, sock.query, sock.upsert_message

    async def community_query(jid: str, type_: str, content: list):
        return await query({'tag': 'iq', 'attrs': {'type': type_, 'xmlns': 'w:g2', 'to': jid}, 'content': content})

    async def community_metadata(jid: str) -> dict:
        result = await community_query(jid, 'get', [{'tag': 'query', 'attrs': {'request': 'interactive'}}])
        return extract_community_metadata(result)

    async def community_fetch_all_participating() -> dict:
        result = await query({'tag': 'iq', 'attrs': {'to': '@g.us', 'xmlns': 'w:g2', 'type': 'get'},
                              'content': [{'tag': 'participating', 'attrs': {}, 'content': [
                                  {'tag': 'participants', 'attrs': {}}, {'tag': 'description', 'attrs': {}}]}]})
        data = {}
        communities_child = get_binary_node_child(result, 'communities')
        if communities_child:
            for community_node in get_binary_node_children(communities_child, 'community'):
                meta = extract_community_metadata({'tag': 'result', 'attrs': {}, 'content': [community_node]})
                data[meta['id']] = meta
        sock.ev.emit('groups.update', list(data.values()))
        return data

    async def parse_group_result(node: dict):
        logger.info({'node': node}, 'parseGroupResult')
        group_node = get_binary_node_child(node, 'group')
        if group_node:
            try:
                logger.info({'groupNode': group_node}, 'groupNode')
                metadata = await sock.group_metadata(f"{group_node['attrs'].get('id')}@g.us")
                return metadata if metadata else None
            except Exception as error:
                print('Error parsing group metadata:', error)
                return None
        return None

    async def on_dirty(node: dict):
        attrs = get_binary_node_child(node, 'dirty')['attrs']
        if attrs.get('type') != 'communities':
            return
        await community_fetch_all_participating()
        await sock.clean_dirty_bits('groups')
    sock.ws.on('CB:ib,,dirty', on_dirty)

    async def community_create(subject: str, body: str):
        description_id = generate_message_id()[:12]
        result = await community_query('@g.us', 'set', [{
            'tag': 'create', 'attrs': {'subject': subject},
            'content': [
                {'tag': 'description', 'attrs': {'id': description_id},
                 'content': [{'tag': 'body', 'attrs': {}, 'content': (body or '').encode('utf-8')}]},
                {'tag': 'parent', 'attrs': {'default_membership_approval_mode': 'request_required'}},
                {'tag': 'allow_non_admin_sub_group_creation', 'attrs': {}},
                {'tag': 'create_general_chat', 'attrs': {}},
            ]}])
        return await parse_group_result(result)

    async def community_create_group(subject: str, participants: list, parent_community_jid: str):
        key = generate_message_id_v2()
        result = await community_query('@g.us', 'set', [{
            'tag': 'create', 'attrs': {'subject': subject, 'key': key},
            'content': [*[{'tag': 'participant', 'attrs': {'jid': jid}} for jid in participants],
                        {'tag': 'linked_parent', 'attrs': {'jid': parent_community_jid}}]}])
        return await parse_group_result(result)

    async def community_leave(id_: str):
        await community_query('@g.us', 'set', [{'tag': 'leave', 'attrs': {},
                                               'content': [{'tag': 'community', 'attrs': {'id': id_}}]}])

    async def community_update_subject(jid: str, subject: str):
        await community_query(jid, 'set', [{'tag': 'subject', 'attrs': {}, 'content': subject.encode('utf-8')}])

    async def community_link_group(group_jid: str, parent_community_jid: str):
        await community_query(parent_community_jid, 'set', [{
            'tag': 'links', 'attrs': {},
            'content': [{'tag': 'link', 'attrs': {'link_type': 'sub_group'},
                         'content': [{'tag': 'group', 'attrs': {'jid': group_jid}}]}]}])

    async def community_unlink_group(group_jid: str, parent_community_jid: str):
        await community_query(parent_community_jid, 'set', [{
            'tag': 'unlink', 'attrs': {'unlink_type': 'sub_group'},
            'content': [{'tag': 'group', 'attrs': {'jid': group_jid}}]}])

    async def community_fetch_linked_groups(jid: str):
        community_jid = jid
        is_community = False
        # Try to determine if it is a subgroup or a community
        metadata = await sock.group_metadata(jid)
        if metadata.get('linkedParent'):
            community_jid = metadata['linkedParent']  # It is a subgroup, get the community jid
        else:
            is_community = True  # It is a community
        # Fetch all subgroups of the community
        result = await community_query(community_jid, 'get', [{'tag': 'sub_groups', 'attrs': {}}])
        linked_groups_data = []
        sub_groups_node = get_binary_node_child(result, 'sub_groups')
        if sub_groups_node:
            for group_node in get_binary_node_children(sub_groups_node, 'group'):
                a = group_node['attrs']
                linked_groups_data.append({
                    'id': jid_encode(a['id'], 'g.us') if a.get('id') else None,
                    'subject': a.get('subject') or '',
                    'creation': float(a['creation']) if a.get('creation') else None,
                    'owner': jid_normalized_user(a['creator']) if a.get('creator') else None,
                    'size': float(a['size']) if a.get('size') else None})
        return {'communityJid': community_jid, 'isCommunity': is_community, 'linkedGroups': linked_groups_data}

    async def community_request_participants_list(jid: str):
        result = await community_query(jid, 'get', [{'tag': 'membership_approval_requests', 'attrs': {}}])
        node = get_binary_node_child(result, 'membership_approval_requests')
        return [v['attrs'] for v in get_binary_node_children(node, 'membership_approval_request')]

    async def community_request_participants_update(jid: str, participants: list, action: str):
        result = await community_query(jid, 'set', [{
            'tag': 'membership_requests_action', 'attrs': {},
            'content': [{'tag': action, 'attrs': {},
                         'content': [{'tag': 'participant', 'attrs': {'jid': j}} for j in participants]}]}])
        node = get_binary_node_child(result, 'membership_requests_action')
        node_action = get_binary_node_child(node, action)
        return [{'status': p['attrs'].get('error') or '200', 'jid': p['attrs'].get('jid')}
                for p in get_binary_node_children(node_action, 'participant')]

    async def community_participants_update(jid: str, participants: list, action: str):
        result = await community_query(jid, 'set', [{
            'tag': action, 'attrs': {'linked_groups': 'true'} if action == 'remove' else {},
            'content': [{'tag': 'participant', 'attrs': {'jid': j}} for j in participants]}])
        node = get_binary_node_child(result, action)
        return [{'status': p['attrs'].get('error') or '200', 'jid': p['attrs'].get('jid'), 'content': p}
                for p in get_binary_node_children(node, 'participant')]

    async def community_update_description(jid: str, description: Optional[str] = None):
        metadata = await community_metadata(jid)
        prev = metadata.get('descId')
        await community_query(jid, 'set', [{
            'tag': 'description',
            'attrs': {**({'id': generate_message_id()} if description else {'delete': 'true'}),
                      **({'prev': prev} if prev else {})},
            'content': [{'tag': 'body', 'attrs': {}, 'content': description.encode('utf-8')}] if description else None}])

    async def community_invite_code(jid: str):
        result = await community_query(jid, 'get', [{'tag': 'invite', 'attrs': {}}])
        return ((get_binary_node_child(result, 'invite') or {}).get('attrs') or {}).get('code')

    async def community_revoke_invite(jid: str):
        result = await community_query(jid, 'set', [{'tag': 'invite', 'attrs': {}}])
        return ((get_binary_node_child(result, 'invite') or {}).get('attrs') or {}).get('code')

    async def community_accept_invite(code: str):
        results = await community_query('@g.us', 'set', [{'tag': 'invite', 'attrs': {'code': code}}])
        return ((get_binary_node_child(results, 'community') or {}).get('attrs') or {}).get('jid')

    async def community_revoke_invite_v4(community_jid: str, invited_jid: str) -> bool:
        result = await community_query(community_jid, 'set', [{'tag': 'revoke', 'attrs': {}, 'content': [
            {'tag': 'participant', 'attrs': {'jid': invited_jid}}]}])
        return bool(result)

    async def _community_accept_invite_v4(key, invite_message: dict):
        key = {'remoteJid': key} if isinstance(key, str) else key
        results = await community_query(invite_message['groupJid'], 'set', [{
            'tag': 'accept', 'attrs': {'code': invite_message['inviteCode'],
                                       'expiration': str(invite_message['inviteExpiration']),
                                       'admin': key['remoteJid']}}])
        # if we have the full message key, update the invite message to be expired
        if key.get('id'):
            invite_message = normalize(proto.Message.GroupInviteMessage, invite_message)
            invite_message['inviteExpiration'] = 0
            invite_message['inviteCode'] = ''
            ev.emit('messages.update', [{'key': key, 'update': {'message': {'groupInviteMessage': invite_message}}}])
        # generate the community add message
        await upsert_message({
            'key': {'remoteJid': invite_message['groupJid'], 'id': generate_message_id_v2((sock.user or {}).get('id')),
                    'fromMe': False, 'participant': key['remoteJid']},
            'messageStubType': WAMessageStubType.GROUP_PARTICIPANT_ADD,
            'messageStubParameters': [json.dumps(auth_state['creds'].get('me'), default=str)],
            'participant': key['remoteJid'], 'messageTimestamp': unix_timestamp_seconds()}, 'notify')
        return results['attrs'].get('from')
    community_accept_invite_v4 = ev.create_buffered_function(_community_accept_invite_v4)

    async def community_get_invite_info(code: str):
        results = await community_query('@g.us', 'get', [{'tag': 'invite', 'attrs': {'code': code}}])
        return extract_community_metadata(results)

    async def community_toggle_ephemeral(jid: str, ephemeral_expiration: int):
        content = {'tag': 'ephemeral', 'attrs': {'expiration': str(ephemeral_expiration)}} if ephemeral_expiration \
            else {'tag': 'not_ephemeral', 'attrs': {}}
        await community_query(jid, 'set', [content])

    async def community_setting_update(jid: str, setting: str):
        await community_query(jid, 'set', [{'tag': setting, 'attrs': {}}])

    async def community_member_add_mode(jid: str, mode: str):
        await community_query(jid, 'set', [{'tag': 'member_add_mode', 'attrs': {}, 'content': mode}])

    async def community_join_approval_mode(jid: str, mode: str):
        await community_query(jid, 'set', [{'tag': 'membership_approval_mode', 'attrs': {},
                                            'content': [{'tag': 'community_join', 'attrs': {'state': mode}}]}])

    return sock.extend(
        community_metadata=community_metadata, community_create=community_create,
        community_create_group=community_create_group, community_leave=community_leave,
        community_update_subject=community_update_subject, community_link_group=community_link_group,
        community_unlink_group=community_unlink_group, community_fetch_linked_groups=community_fetch_linked_groups,
        community_request_participants_list=community_request_participants_list,
        community_request_participants_update=community_request_participants_update,
        community_participants_update=community_participants_update,
        community_update_description=community_update_description, community_invite_code=community_invite_code,
        community_revoke_invite=community_revoke_invite, community_accept_invite=community_accept_invite,
        community_revoke_invite_v4=community_revoke_invite_v4,
        community_accept_invite_v4=community_accept_invite_v4, community_get_invite_info=community_get_invite_info,
        community_toggle_ephemeral=community_toggle_ephemeral, community_setting_update=community_setting_update,
        community_member_add_mode=community_member_add_mode,
        community_join_approval_mode=community_join_approval_mode,
        community_fetch_all_participating=community_fetch_all_participating)


def extract_community_metadata(result: dict) -> dict:
    community = get_binary_node_child(result, 'community')
    attrs = community['attrs']
    desc_child = get_binary_node_child(community, 'description')
    desc = desc_id = None
    if desc_child:
        desc = get_binary_node_child_string(desc_child, 'body')
        desc_id = desc_child['attrs'].get('id')
    community_id = attrs['id'] if attrs.get('id') and '@' in attrs['id'] else jid_encode(attrs.get('id') or '', 'g.us')
    eph = ((get_binary_node_child(community, 'ephemeral') or {}).get('attrs') or {}).get('expiration')
    member_add_mode = get_binary_node_child_string(community, 'member_add_mode') == 'all_member_add'
    linked_parent = get_binary_node_child(community, 'linked_parent')
    return {
        'id': community_id,
        'subject': attrs.get('subject') or '',
        'subjectOwner': attrs.get('s_o'),
        'subjectTime': float(attrs.get('s_t') or 0),
        'size': len(get_binary_node_children(community, 'participant')),
        'creation': float(attrs.get('creation') or 0),
        'owner': jid_normalized_user(attrs['creator']) if attrs.get('creator') else None,
        'desc': desc, 'descId': desc_id,
        'linkedParent': ((linked_parent or {}).get('attrs') or {}).get('jid') or None,
        'restrict': bool(get_binary_node_child(community, 'locked')),
        'announce': bool(get_binary_node_child(community, 'announcement')),
        'isCommunity': bool(get_binary_node_child(community, 'parent')),
        'isCommunityAnnounce': bool(get_binary_node_child(community, 'default_sub_community')),
        'joinApprovalMode': bool(get_binary_node_child(community, 'membership_approval_mode')),
        'memberAddMode': member_add_mode,
        # TODO: IMPLEMENT THE PN/LID FIELDS HERE!!
        'participants': [{'id': p['attrs'].get('jid'), 'admin': p['attrs'].get('type') or None}
                         for p in get_binary_node_children(community, 'participant')],
        'ephemeralDuration': int(eph) if eph else None,
        'addressingMode': get_binary_node_child_string(community, 'addressing_mode'),
    }
