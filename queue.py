"""Outgoing message queue: per-chat ordering, spacing with jitter and retry with backoff.

WhatsApp is sensitive to bursts. This does not make spam safe; it just keeps normal bots from looking like a
flood and from losing messages on a transient failure.
"""
from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, Optional

_RETRY_CODES = {408, 428, 440, 503, 515}


def _retryable(err: BaseException) -> bool:
    if isinstance(err, (ConnectionError, asyncio.TimeoutError, TimeoutError)):
        return True
    code = getattr(err, 'status_code', None) or (getattr(err, 'output', None) or {}).get('statusCode')
    return code in _RETRY_CODES


class SendQueue:
    def __init__(self, *, per_chat: float = 0.6, global_: float = 0.12, jitter: float = 0.25, retries: int = 2,
                 backoff: float = 1.5, logger=None):
        """per_chat/global_: minimum seconds between sends to the same chat / overall; jitter: random extra
        seconds; retries: extra attempts on transient errors (connection closed, timeout, 503...)."""
        self.per_chat, self.global_, self.jitter = per_chat, global_, jitter
        self.retries, self.backoff, self.logger = retries, backoff, logger
        self._chat_next: dict[str, float] = {}
        self._global_next = 0.0
        self._locks: dict[str, tuple[asyncio.Lock, int]] = {}

    async def submit(self, chat: str, fn: Callable[[], Awaitable], priority_bypass: bool = False):
        lock, n = self._locks.get(chat) or (asyncio.Lock(), 0)
        self._locks[chat] = (lock, n + 1)
        try:
            async with lock:  # keeps the order of messages inside a chat
                if not priority_bypass:
                    await self._wait_turn(chat)
                for attempt in range(self.retries + 1):
                    try:
                        return await fn()
                    except Exception as e:
                        if attempt >= self.retries or not _retryable(e):
                            raise
                        delay = self.backoff ** (attempt + 1) + random.uniform(0, self.jitter)
                        if self.logger:
                            self.logger.warn({'err': repr(e), 'retryIn': round(delay, 2)}, 'send failed, retrying')
                        await asyncio.sleep(delay)
        finally:
            lock, n = self._locks[chat]
            if n <= 1:
                self._locks.pop(chat, None)
            else:
                self._locks[chat] = (lock, n - 1)

    async def _wait_turn(self, chat: str):
        loop = asyncio.get_running_loop()
        now = loop.time()
        at = max(now, self._chat_next.get(chat, 0.0), self._global_next) + random.uniform(0, self.jitter)
        # reserve both slots *before* sleeping so concurrent senders queue up behind us
        self._chat_next[chat] = at + self.per_chat
        self._global_next = at + self.global_
        if len(self._chat_next) > 5000:
            self._chat_next = {k: v for k, v in self._chat_next.items() if v > now}
        if at > now:
            await asyncio.sleep(at - now)


def make_queue(rate_limit, logger=None) -> Optional[SendQueue]:
    """rate_limit: None/True -> defaults, False -> disabled, dict -> SendQueue kwargs, SendQueue -> as is"""
    if rate_limit is False:
        return None
    if isinstance(rate_limit, SendQueue):
        return rate_limit
    kwargs = dict(rate_limit) if isinstance(rate_limit, dict) else {}
    return SendQueue(logger=logger, **kwargs)
