from __future__ import annotations
import asyncio
import time
from enum import IntEnum
from typing import Callable, Optional
from .cache import TTLCache

RECENT_MESSAGES_SIZE = 512  # Number of sent messages to cache in memory for handling retry receipts
MESSAGE_KEY_SEPARATOR = '\u0000'
RECREATE_SESSION_TIMEOUT = 60 * 60 * 1000  # 1 hour in milliseconds
PHONE_REQUEST_DELAY = 3000


class RetryReason(IntEnum):
    """Retry reason codes matching WhatsApp Web's Signal error codes."""
    UnknownError = 0
    SignalErrorNoSession = 1
    SignalErrorInvalidKey = 2
    SignalErrorInvalidKeyId = 3
    SignalErrorInvalidMessage = 4  # MAC verification failed - most common cause of decryption failures
    SignalErrorInvalidSignature = 5
    SignalErrorFutureMessage = 6
    SignalErrorBadMac = 7  # Explicit MAC failure - session is definitely out of sync
    SignalErrorInvalidSession = 8
    SignalErrorInvalidMsgKey = 9
    BadBroadcastEphemeralSetting = 10
    UnknownCompanionNoPrekey = 11
    AdvFailure = 12
    StatusRevokeDelay = 13


MAC_ERROR_CODES = {RetryReason.SignalErrorInvalidMessage, RetryReason.SignalErrorBadMac}


class MessageRetryManager:
    def __init__(self, logger, max_msg_retry_count: int):
        self.logger = logger
        self.max_msg_retry_count = max_msg_retry_count
        self.message_key_index: dict[str, str] = {}
        self.recent_messages_map = TTLCache(ttl=5 * 60, max_size=RECENT_MESSAGES_SIZE)
        self.session_recreate_history = TTLCache(ttl=RECREATE_SESSION_TIMEOUT * 2 / 1000)
        self.retry_counters = TTLCache(ttl=15 * 60, update_age_on_get=True)
        self.base_keys = TTLCache(ttl=15 * 60, max_size=1024)
        self.pending_phone_requests: dict[str, asyncio.TimerHandle] = {}
        self.statistics = self._new_stats()

    @staticmethod
    def _new_stats() -> dict:
        return {'totalRetries': 0, 'successfulRetries': 0, 'failedRetries': 0, 'mediaRetries': 0,
                'sessionRecreations': 0, 'phoneRequests': 0}

    @staticmethod
    def _key_to_string(to: str, id_: str) -> str:
        return f'{to}{MESSAGE_KEY_SEPARATOR}{id_}'

    def add_recent_message(self, to: str, id_: str, message: dict):
        """Add a recent message to the cache for retry handling"""
        key_str = self._key_to_string(to, id_)
        self.recent_messages_map.set(key_str, {'message': message, 'timestamp': int(time.time() * 1000)})
        self.message_key_index[id_] = key_str
        self.logger.debug(f'Added message to retry cache: {to}/{id_}')

    def get_recent_message(self, to: str, id_: str) -> Optional[dict]:
        return self.recent_messages_map.get(self._key_to_string(to, id_))

    def should_recreate_session(self, jid: str, has_session: bool, error_code: Optional[int] = None) -> dict:
        """MAC errors (codes 4 and 7) trigger immediate session recreation regardless of timeout."""
        now = int(time.time() * 1000)
        if not has_session:
            self.session_recreate_history.set(jid, now)
            self.statistics['sessionRecreations'] += 1
            return {'reason': "we don't have a Signal session with them", 'recreate': True}
        if error_code is not None and error_code in MAC_ERROR_CODES:
            self.session_recreate_history.set(jid, now)
            self.statistics['sessionRecreations'] += 1
            self.logger.warn({'jid': jid, 'errorCode': RetryReason(error_code).name},
                             'MAC error detected, forcing immediate session recreation')
            return {'reason': f'MAC error (code {error_code}: {RetryReason(error_code).name}), '
                              'immediate session recreation', 'recreate': True}
        prev_time = self.session_recreate_history.get(jid)
        if not prev_time or now - prev_time > RECREATE_SESSION_TIMEOUT:
            self.session_recreate_history.set(jid, now)
            self.statistics['sessionRecreations'] += 1
            return {'reason': 'retry count > 1 and over an hour since last recreation', 'recreate': True}
        return {'reason': '', 'recreate': False}

    def parse_retry_error_code(self, error_attr: Optional[str]) -> Optional[int]:
        if error_attr is None or error_attr == '':
            return None
        try:
            code = int(error_attr)
        except ValueError:
            return None
        if RetryReason.UnknownError <= code <= RetryReason.StatusRevokeDelay:
            return code
        return RetryReason.UnknownError

    def is_mac_error(self, error_code: Optional[int]) -> bool:
        return error_code is not None and error_code in MAC_ERROR_CODES

    def increment_retry_count(self, message_id: str) -> int:
        self.retry_counters.set(message_id, (self.retry_counters.get(message_id) or 0) + 1)
        self.statistics['totalRetries'] += 1
        return self.retry_counters.get(message_id)

    def get_retry_count(self, message_id: str) -> int:
        return self.retry_counters.get(message_id) or 0

    def has_exceeded_max_retries(self, message_id: str) -> bool:
        return self.get_retry_count(message_id) >= self.max_msg_retry_count

    def mark_retry_success(self, message_id: str):
        self.statistics['successfulRetries'] += 1
        self.retry_counters.delete(message_id)
        self.cancel_pending_phone_request(message_id)
        self._remove_recent_message(message_id)

    def mark_retry_failed(self, message_id: str):
        self.statistics['failedRetries'] += 1
        self.retry_counters.delete(message_id)
        self.cancel_pending_phone_request(message_id)
        self._remove_recent_message(message_id)

    def schedule_phone_request(self, message_id: str, callback: Callable, delay: float = PHONE_REQUEST_DELAY):
        self.cancel_pending_phone_request(message_id)

        def fire():
            self.pending_phone_requests.pop(message_id, None)
            self.statistics['phoneRequests'] += 1
            r = callback()
            if asyncio.iscoroutine(r):
                asyncio.ensure_future(r)
        self.pending_phone_requests[message_id] = asyncio.get_running_loop().call_later(delay / 1000, fire)
        self.logger.debug(f'Scheduled phone request for message {message_id} with {delay}ms delay')

    def cancel_pending_phone_request(self, message_id: str):
        t = self.pending_phone_requests.pop(message_id, None)
        if t:
            t.cancel()
            self.logger.debug(f'Cancelled pending phone request for message {message_id}')

    def clear(self):
        self.recent_messages_map.clear()
        self.message_key_index.clear()
        self.session_recreate_history.clear()
        self.retry_counters.clear()
        self.base_keys.clear()
        for mid in list(self.pending_phone_requests):
            self.cancel_pending_phone_request(mid)
        self.statistics = self._new_stats()

    def save_base_key(self, addr: str, msg_id: str, base_key: bytes):
        self.base_keys.set(f'{addr}:{msg_id}', bytes(base_key))

    def has_same_base_key(self, addr: str, msg_id: str, base_key: bytes) -> bool:
        stored = self.base_keys.get(f'{addr}:{msg_id}')
        return bool(stored) and bytes(stored) == bytes(base_key)

    def delete_base_key(self, addr: str, msg_id: str):
        self.base_keys.delete(f'{addr}:{msg_id}')

    def _remove_recent_message(self, message_id: str):
        key_str = self.message_key_index.pop(message_id, None)
        if key_str:
            self.recent_messages_map.delete(key_str)
