from __future__ import annotations
import json
from typing import Optional

from ..Types import WAMessageAddressingMode, WAMessageStubType
from ..Utils.generics import generate_message_id_v2, unix_timestamp_seconds
from ..Utils.proto_utils import normalize
from ..WABinary import (Boom, get_binary_node_child, get_binary_node_child_string, get_binary_node_children, is_lid_user,
                        is_pn_user, jid_encode, jid_normalized_user)
from ..WAProto import WAProto_pb2 as proto
from .chats import make_chats_socket


def _num(v):
    """JS unary plus on an attribute: number or NaN(None)"""
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


def make_groups_socket(config: dict):
    sock = make_chats_socket(config)
    auth_state, ev, query, upsert_message = sock.auth_state, sock.ev, sock.query, sock.upsert_message

    async def group_query(jid: str, type_: str, content: list):
        return await query({'tag': 'iq', 'attrs': {'type': type_, 'xmlns': 'w:g2', 'to': jid}, 'content': content})

    async def group_metadata(jid: str) -> dict:
        result = await group_query(jid, 'get', [{'tag': 'query', 'attrs': {'request': 'interactive'}}])
        return extract_group_metadata(result)

    async def group_fetch_all_participating() -> dict:
        result = await query({'tag': 'iq', 'attrs': {'to': '@g.us', 'xmlns': 'w:g2', 'type': 'get'},
                              'content': [{'tag': 'participating', 'attrs': {}, 'content': [
                                  {'tag': 'participants', 'attrs': {}}, {'tag': 'description', 'attrs': {}}]}]})
        data = {}
        groups_child = get_binary_node_child(result, 'groups')
        if groups_child:
            for group_node in get_binary_node_children(groups_child, 'group'):
                meta = extract_group_metadata({'tag': 'result', 'attrs': {}, 'content': [group_node]})
                data[meta['id']] = meta
        # TODO: properly parse LID / PN DATA
        sock.ev.emit('groups.update', list(data.values()))
        return data

    async def on_dirty(node: dict):
        attrs = get_binary_node_child(node, 'dirty')['attrs']
        if attrs.get('type') != 'groups':
            return
        await group_fetch_all_participating()
        await sock.clean_dirty_bits('groups')
    sock.ws.on('CB:ib,,dirty', on_dirty)

    async def group_create(subject: str, participants: list):
        key = generate_message_id_v2()
        result = await group_query('@g.us', 'set', [{
            'tag': 'create', 'attrs': {'subject': subject, 'key': key},
            'content': [{'tag': 'participant', 'attrs': {'jid': jid}} for jid in participants]}])
        return extract_group_metadata(result)

    async def group_leave(id_: str):
        await group_query('@g.us', 'set', [{'tag': 'leave', 'attrs': {},
                                           'content': [{'tag': 'group', 'attrs': {'id': id_}}]}])

    async def group_update_subject(jid: str, subject: str):
        await group_query(jid, 'set', [{'tag': 'subject', 'attrs': {}, 'content': subject.encode('utf-8')}])

    async def group_request_participants_list(jid: str):
        result = await group_query(jid, 'get', [{'tag': 'membership_approval_requests', 'attrs': {}}])
        node = get_binary_node_child(result, 'membership_approval_requests')
        return [v['attrs'] for v in get_binary_node_children(node, 'membership_approval_request')]

    async def group_request_participants_update(jid: str, participants: list, action: str):
        result = await group_query(jid, 'set', [{
            'tag': 'membership_requests_action', 'attrs': {},
            'content': [{'tag': action, 'attrs': {},
                         'content': [{'tag': 'participant', 'attrs': {'jid': j}} for j in participants]}]}])
        node = get_binary_node_child(result, 'membership_requests_action')
        node_action = get_binary_node_child(node, action)
        return [{'status': p['attrs'].get('error') or '200', 'jid': p['attrs'].get('jid')}
                for p in get_binary_node_children(node_action, 'participant')]

    async def group_participants_update(jid: str, participants: list, action: str):
        result = await group_query(jid, 'set', [{
            'tag': action, 'attrs': {},
            'content': [{'tag': 'participant', 'attrs': {'jid': j}} for j in participants]}])
        node = get_binary_node_child(result, action)
        return [{'status': p['attrs'].get('error') or '200', 'jid': p['attrs'].get('jid'), 'content': p}
                for p in get_binary_node_children(node, 'participant')]

    async def group_update_description(jid: str, description: Optional[str] = None):
        metadata = await group_metadata(jid)
        prev = metadata.get('descId')
        await group_query(jid, 'set', [{
            'tag': 'description',
            'attrs': {**({'id': generate_message_id_v2()} if description else {'delete': 'true'}),
                      **({'prev': prev} if prev else {})},
            'content': [{'tag': 'body', 'attrs': {}, 'content': description.encode('utf-8')}] if description else None}])

    async def group_invite_code(jid: str):
        result = await group_query(jid, 'get', [{'tag': 'invite', 'attrs': {}}])
        invite_node = get_binary_node_child(result, 'invite')
        return (invite_node or {}).get('attrs', {}).get('code')

    async def group_revoke_invite(jid: str):
        result = await group_query(jid, 'set', [{'tag': 'invite', 'attrs': {}}])
        invite_node = get_binary_node_child(result, 'invite')
        return (invite_node or {}).get('attrs', {}).get('code')

    async def group_accept_invite(code: str):
        results = await group_query('@g.us', 'set', [{'tag': 'invite', 'attrs': {'code': code}}])
        result = get_binary_node_child(results, 'group')
        return (result or {}).get('attrs', {}).get('jid')

    async def group_revoke_invite_v4(group_jid: str, invited_jid: str) -> bool:
        """revoke a v4 invite for someone; returns true if successful"""
        result = await group_query(group_jid, 'set', [{'tag': 'revoke', 'attrs': {}, 'content': [
            {'tag': 'participant', 'attrs': {'jid': invited_jid}}]}])
        return bool(result)

    async def _group_accept_invite_v4(key, invite_message: dict):
        """accept a GroupInviteMessage; key is the key of the invite message, or only the jid of the inviter"""
        key = {'remoteJid': key} if isinstance(key, str) else key
        results = await group_query(invite_message['groupJid'], 'set', [{
            'tag': 'accept', 'attrs': {'code': invite_message['inviteCode'],
                                       'expiration': str(invite_message['inviteExpiration']),
                                       'admin': key['remoteJid']}}])
        # if we have the full message key, update the invite message to be expired
        if key.get('id'):
            invite_message = normalize(proto.Message.GroupInviteMessage, invite_message)  # new expired invite message
            invite_message['inviteExpiration'] = 0
            invite_message['inviteCode'] = ''
            ev.emit('messages.update', [{'key': key, 'update': {'message': {'groupInviteMessage': invite_message}}}])
        # generate the group add message
        await upsert_message({
            'key': {'remoteJid': invite_message['groupJid'], 'id': generate_message_id_v2((sock.user or {}).get('id')),
                    'fromMe': False, 'participant': key['remoteJid']},
            'messageStubType': WAMessageStubType.GROUP_PARTICIPANT_ADD,
            'messageStubParameters': [json.dumps(auth_state['creds'].get('me'), default=str)],
            'participant': key['remoteJid'],
            'messageTimestamp': unix_timestamp_seconds()}, 'notify')
        return results['attrs'].get('from')
    group_accept_invite_v4 = ev.create_buffered_function(_group_accept_invite_v4)

    async def group_get_invite_info(code: str):
        results = await group_query('@g.us', 'get', [{'tag': 'invite', 'attrs': {'code': code}}])
        return extract_group_metadata(results)

    async def group_toggle_ephemeral(jid: str, ephemeral_expiration: int):
        content = {'tag': 'ephemeral', 'attrs': {'expiration': str(ephemeral_expiration)}} if ephemeral_expiration \
            else {'tag': 'not_ephemeral', 'attrs': {}}
        await group_query(jid, 'set', [content])

    async def group_setting_update(jid: str, setting: str):
        """setting: announcement | not_announcement | locked | unlocked"""
        await group_query(jid, 'set', [{'tag': setting, 'attrs': {}}])

    async def group_member_add_mode(jid: str, mode: str):
        """mode: admin_add | all_member_add"""
        await group_query(jid, 'set', [{'tag': 'member_add_mode', 'attrs': {}, 'content': mode}])

    async def group_join_approval_mode(jid: str, mode: str):
        """mode: on | off"""
        await group_query(jid, 'set', [{'tag': 'membership_approval_mode', 'attrs': {},
                                        'content': [{'tag': 'group_join', 'attrs': {'state': mode}}]}])

    return sock.extend(
        group_query=group_query, group_metadata=group_metadata, group_create=group_create, group_leave=group_leave,
        group_update_subject=group_update_subject, group_request_participants_list=group_request_participants_list,
        group_request_participants_update=group_request_participants_update,
        group_participants_update=group_participants_update, group_update_description=group_update_description,
        group_invite_code=group_invite_code, group_revoke_invite=group_revoke_invite,
        group_accept_invite=group_accept_invite, group_revoke_invite_v4=group_revoke_invite_v4,
        group_accept_invite_v4=group_accept_invite_v4, group_get_invite_info=group_get_invite_info,
        group_toggle_ephemeral=group_toggle_ephemeral, group_setting_update=group_setting_update,
        group_member_add_mode=group_member_add_mode, group_join_approval_mode=group_join_approval_mode,
        group_fetch_all_participating=group_fetch_all_participating)


def extract_group_metadata(result: dict) -> dict:
    group = get_binary_node_child(result, 'group')
    if not group:
        # Mirror WAWeb: surface server/client errors with their code+text instead of crashing.
        error_node = get_binary_node_child(result, 'error')
        if error_node:
            code = int(error_node['attrs']['code']) if error_node['attrs'].get('code') else 500
            text = error_node['attrs'].get('text') or 'group metadata query failed'
            raise Boom(text, status_code=code, data=error_node)
        raise Boom('Invalid group metadata response: missing <group> node', data=result)
    attrs = group['attrs']
    if not attrs.get('id'):
        raise Boom('Invalid group metadata response: missing group id', data=group)

    desc_child = get_binary_node_child(group, 'description')
    desc = desc_id = desc_owner = desc_owner_pn = desc_owner_username = desc_time = None
    if desc_child:
        da = desc_child['attrs']
        desc = get_binary_node_child_string(desc_child, 'body')
        desc_owner = jid_normalized_user(da['participant']) if da.get('participant') else None
        desc_owner_pn = jid_normalized_user(da['participant_pn']) if da.get('participant_pn') else None
        desc_owner_username = da.get('participant_username') or None
        desc_time = _num(da.get('t'))
        desc_id = da.get('id')

    group_id = attrs['id'] if '@' in attrs['id'] else jid_encode(attrs['id'], 'g.us')
    eph = ((get_binary_node_child(group, 'ephemeral') or {}).get('attrs') or {}).get('expiration')
    member_add_mode = get_binary_node_child_string(group, 'member_add_mode') == 'all_member_add'
    participants = []
    for p in get_binary_node_children(group, 'participant'):
        pa = p['attrs']
        participants.append({
            'id': pa.get('jid'),
            'phoneNumber': pa.get('phone_number') if is_lid_user(pa.get('jid')) and is_pn_user(pa.get('phone_number'))
            else None,
            'lid': pa.get('lid') if is_pn_user(pa.get('jid')) and is_lid_user(pa.get('lid')) else None,
            'username': pa.get('participant_username') or pa.get('username') or None,
            'admin': pa.get('type') or None,
        })
    linked_parent = get_binary_node_child(group, 'linked_parent')
    return {
        'id': group_id,
        'notify': attrs.get('notify'),
        'addressingMode': WAMessageAddressingMode.LID if attrs.get('addressing_mode') == 'lid'
        else WAMessageAddressingMode.PN,
        'subject': attrs.get('subject'),
        'subjectOwner': attrs.get('s_o'),
        'subjectOwnerPn': attrs.get('s_o_pn'),
        'subjectOwnerUsername': attrs.get('s_o_username'),
        'subjectTime': _num(attrs.get('s_t')),
        'size': int(attrs['size']) if attrs.get('size') else len(get_binary_node_children(group, 'participant')),
        'creation': _num(attrs.get('creation')),
        'owner': jid_normalized_user(attrs['creator']) if attrs.get('creator') else None,
        'ownerPn': jid_normalized_user(attrs['creator_pn']) if attrs.get('creator_pn') else None,
        'ownerUsername': attrs.get('creator_username') or None,
        'owner_country_code': attrs.get('creator_country_code'),
        'desc': desc, 'descId': desc_id, 'descOwner': desc_owner, 'descOwnerPn': desc_owner_pn,
        'descOwnerUsername': desc_owner_username, 'descTime': desc_time,
        'linkedParent': ((linked_parent or {}).get('attrs') or {}).get('jid') or None,
        'restrict': bool(get_binary_node_child(group, 'locked')),
        'announce': bool(get_binary_node_child(group, 'announcement')),
        'isCommunity': bool(get_binary_node_child(group, 'parent')),
        'isCommunityAnnounce': bool(get_binary_node_child(group, 'default_sub_group')),
        'joinApprovalMode': bool(get_binary_node_child(group, 'membership_approval_mode')),
        'memberAddMode': member_add_mode,
        'participants': participants,
        'ephemeralDuration': int(eph) if eph else None,
    }
