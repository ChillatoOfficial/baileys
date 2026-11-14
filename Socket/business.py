from __future__ import annotations
import base64
from typing import Optional

from ..Utils.business import (parse_catalog_node, parse_collections_node, parse_order_details_node,
                              parse_product_node, to_product_node, uploading_necessary_images_of_product)
from ..Utils.messages_media import get_raw_media_upload_data
from ..WABinary import get_binary_node_child, jid_normalized_user, S_WHATSAPP_NET
from .messages_recv import make_messages_recv_socket


def make_business_socket(config: dict):
    sock = make_messages_recv_socket(config)
    auth_state, query, wa_upload_to_server = sock.auth_state, sock.query, sock.wa_upload_to_server

    async def update_bussines_profile(args: dict):
        node = []
        for key in ('address', 'email', 'description'):
            if args.get(key) is not None:
                node.append({'tag': key, 'attrs': {}, 'content': args[key]})
        if args.get('websites') is not None:
            node.extend({'tag': 'website', 'attrs': {}, 'content': w} for w in args['websites'])
        if args.get('hours') is not None:
            days = []
            for day_config in args['hours']['days']:
                base = {'tag': 'business_hours_config', 'attrs': {'day_of_week': day_config['day'],
                                                                   'mode': day_config['mode']}}
                if day_config['mode'] == 'specific_hours':
                    base = {**base, 'attrs': {**base['attrs'], 'open_time': day_config.get('openTimeInMinutes'),
                                              'close_time': day_config.get('closeTimeInMinutes')}}
                days.append(base)
            node.append({'tag': 'business_hours', 'attrs': {'timezone': args['hours'].get('timezone')},
                         'content': days})
        return await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz'},
                            'content': [{'tag': 'business_profile', 'attrs': {'v': '3', 'mutation_type': 'delta'},
                                         'content': node}]})

    async def update_cover_photo(photo) -> str:
        raw = await get_raw_media_upload_data(photo, 'biz-cover-photo')
        file_sha256_b64 = base64.b64encode(raw['fileSha256']).decode()
        up = await wa_upload_to_server(raw['filePath'], fileEncSha256B64=file_sha256_b64, mediaType='biz-cover-photo')
        await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz'},
                     'content': [{'tag': 'business_profile', 'attrs': {'v': '3', 'mutation_type': 'delta'},
                                  'content': [{'tag': 'cover_photo',
                                               'attrs': {'id': str(up.get('fbid')), 'op': 'update',
                                                         'token': up.get('meta_hmac'), 'ts': str(up.get('ts'))}}]}]})
        return up.get('fbid')

    async def remove_cover_photo(id_: str):
        return await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz'},
                            'content': [{'tag': 'business_profile', 'attrs': {'v': '3', 'mutation_type': 'delta'},
                                         'content': [{'tag': 'cover_photo', 'attrs': {'op': 'delete', 'id': id_}}]}]})

    async def get_catalog(jid: Optional[str] = None, limit: Optional[int] = None, cursor: Optional[str] = None):
        jid = jid_normalized_user(jid or (auth_state['creds'].get('me') or {}).get('id'))
        query_param_nodes = [
            {'tag': 'limit', 'attrs': {}, 'content': str(limit or 10).encode()},
            {'tag': 'width', 'attrs': {}, 'content': b'100'},
            {'tag': 'height', 'attrs': {}, 'content': b'100'},
        ]
        if cursor:
            query_param_nodes.append({'tag': 'after', 'attrs': {}, 'content': cursor})
        result = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'get', 'xmlns': 'w:biz:catalog'},
                              'content': [{'tag': 'product_catalog', 'attrs': {'jid': jid, 'allow_shop_source': 'true'},
                                           'content': query_param_nodes}]})
        return parse_catalog_node(result)

    async def get_collections(jid: Optional[str] = None, limit: int = 51):
        jid = jid_normalized_user(jid or (auth_state['creds'].get('me') or {}).get('id'))
        result = await query({
            'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'get', 'xmlns': 'w:biz:catalog', 'smax_id': '35'},
            'content': [{'tag': 'collections', 'attrs': {'biz_jid': jid}, 'content': [
                {'tag': 'collection_limit', 'attrs': {}, 'content': str(limit).encode()},
                {'tag': 'item_limit', 'attrs': {}, 'content': str(limit).encode()},
                {'tag': 'width', 'attrs': {}, 'content': b'100'},
                {'tag': 'height', 'attrs': {}, 'content': b'100'}]}]})
        return parse_collections_node(result)

    async def get_order_details(order_id: str, token_base64: str):
        result = await query({
            'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'get', 'xmlns': 'fb:thrift_iq', 'smax_id': '5'},
            'content': [{'tag': 'order', 'attrs': {'op': 'get', 'id': order_id}, 'content': [
                {'tag': 'image_dimensions', 'attrs': {}, 'content': [
                    {'tag': 'width', 'attrs': {}, 'content': b'100'},
                    {'tag': 'height', 'attrs': {}, 'content': b'100'}]},
                {'tag': 'token', 'attrs': {}, 'content': token_base64.encode()}]}]})
        return parse_order_details_node(result)

    async def product_update(product_id: str, update: dict):
        update = await uploading_necessary_images_of_product(update, wa_upload_to_server)
        edit_node = to_product_node(product_id, update)
        result = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz:catalog'},
                              'content': [{'tag': 'product_catalog_edit', 'attrs': {'v': '1'}, 'content': [
                                  edit_node, {'tag': 'width', 'attrs': {}, 'content': '100'},
                                  {'tag': 'height', 'attrs': {}, 'content': '100'}]}]})
        edit = get_binary_node_child(result, 'product_catalog_edit')
        return parse_product_node(get_binary_node_child(edit, 'product'))

    async def product_create(create: dict):
        create['isHidden'] = bool(create.get('isHidden'))  # ensure isHidden is defined
        create = await uploading_necessary_images_of_product(create, wa_upload_to_server)
        create_node = to_product_node(None, create)
        result = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz:catalog'},
                              'content': [{'tag': 'product_catalog_add', 'attrs': {'v': '1'}, 'content': [
                                  create_node, {'tag': 'width', 'attrs': {}, 'content': '100'},
                                  {'tag': 'height', 'attrs': {}, 'content': '100'}]}]})
        add = get_binary_node_child(result, 'product_catalog_add')
        return parse_product_node(get_binary_node_child(add, 'product'))

    async def product_delete(product_ids: list):
        result = await query({'tag': 'iq', 'attrs': {'to': S_WHATSAPP_NET, 'type': 'set', 'xmlns': 'w:biz:catalog'},
                              'content': [{'tag': 'product_catalog_delete', 'attrs': {'v': '1'}, 'content': [
                                  {'tag': 'product', 'attrs': {}, 'content': [
                                      {'tag': 'id', 'attrs': {}, 'content': i.encode()}]} for i in product_ids]}]})
        node = get_binary_node_child(result, 'product_catalog_delete')
        return {'deleted': int(((node or {}).get('attrs') or {}).get('deleted_count') or 0)}

    return sock.extend(
        logger=config['logger'], get_order_details=get_order_details, get_catalog=get_catalog,
        get_collections=get_collections, product_create=product_create, product_delete=product_delete,
        product_update=product_update, update_bussines_profile=update_bussines_profile,
        update_cover_photo=update_cover_photo, remove_cover_photo=remove_cover_photo)
