from __future__ import annotations
import json
from typing import Any, Callable
from ..WABinary import Boom, get_binary_node_child, S_WHATSAPP_NET


async def _w_mex_query(variables: dict, query_id: str, query: Callable, generate_message_tag: Callable):
    return await query({
        'tag': 'iq',
        'attrs': {'id': generate_message_tag(), 'type': 'get', 'to': S_WHATSAPP_NET, 'xmlns': 'w:mex'},
        'content': [{'tag': 'query', 'attrs': {'query_id': query_id},
                     'content': json.dumps({'variables': variables}, separators=(',', ':')).encode('utf-8')}],
    })


async def execute_w_mex_query(variables: dict, query_id: str, data_path: str, query: Callable,
                              generate_message_tag: Callable) -> Any:
    result = await _w_mex_query(variables, query_id, query, generate_message_tag)
    child = get_binary_node_child(result, 'result')
    if child and child.get('content'):
        content = child['content']
        data = json.loads(content.decode() if isinstance(content, (bytes, bytearray)) else content)
        if data.get('errors'):
            error_messages = ', '.join(e.get('message') or 'Unknown error' for e in data['errors'])
            first = data['errors'][0]
            error_code = (first.get('extensions') or {}).get('error_code') or 400
            raise Boom(f'GraphQL server error: {error_messages}', status_code=error_code, data=first)
        response = (data.get('data') or {}).get(data_path) if data_path else data.get('data')
        if response is not None:
            return response
    action = (data_path or '')
    action = action[5:].replace('_', ' ') if action.startswith('xwa2_') else action.replace('_', ' ')
    raise Boom(f'Failed to {action}, unexpected response structure.', status_code=400, data=result)
