from __future__ import annotations
import asyncio
from collections import deque
from typing import Callable


def make_offline_node_processor(node_processor_map: dict, deps: dict, batch_size: int = 10) -> dict:
    """Queues offline stanza nodes for sequential processing, yielding to the event loop periodically.
    deps: isWsOpen(), onUnexpectedError(error, msg), yieldToEventLoop() (async)"""
    nodes: deque = deque()
    state = {'isProcessing': False}

    def enqueue(type_: str, node: dict):
        nodes.append((type_, node))
        if state['isProcessing']:
            return
        state['isProcessing'] = True

        async def run():
            processed_in_batch = 0
            try:
                while nodes and deps['isWsOpen']():
                    t, n = nodes.popleft()
                    processor = node_processor_map.get(t)
                    if not processor:
                        deps['onUnexpectedError'](Exception(f'unknown offline node type: {t}'), 'processing offline node')
                        continue
                    try:
                        await processor(n)
                    except Exception as err:
                        deps['onUnexpectedError'](err, f'processing offline {t}')
                    processed_in_batch += 1
                    # Yield to event loop after processing a batch
                    if processed_in_batch >= batch_size:
                        processed_in_batch = 0
                        await deps['yieldToEventLoop']()
            finally:
                state['isProcessing'] = False

        async def guarded():
            try:
                await run()
            except Exception as error:
                deps['onUnexpectedError'](error, 'processing offline nodes')
        asyncio.ensure_future(guarded())
    return {'enqueue': enqueue}
