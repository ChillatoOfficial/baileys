"""baileys: Python port of WhiskeySockets/Baileys (WhatsApp Web multi-device API)."""
from __future__ import annotations

from .Defaults import *  # noqa: F401,F403
from .Defaults import DEFAULT_CONNECTION_CONFIG as DEFAULT_CONNECTION_CONFIG
from .Types import *  # noqa: F401,F403
from .Utils.auth_utils import (add_transaction_capability as add_transaction_capability,
                               init_auth_creds as init_auth_creds,
                               make_cacheable_signal_key_store as make_cacheable_signal_key_store)
from .Utils.browser_utils import Browsers as Browsers
from .Utils.chat_utils import *  # noqa: F401,F403
from .Utils.crypto import *  # noqa: F401,F403
from .Utils.event_buffer import make_event_buffer as make_event_buffer
from .Utils.generics import *  # noqa: F401,F403
from .Utils.history import *  # noqa: F401,F403
from .Utils.link_preview import get_url_info as get_url_info
from .Utils.logger import ILogger as ILogger, logger as logger
from .Utils.messages import *  # noqa: F401,F403
from .Utils.messages_media import *  # noqa: F401,F403
from .Utils.process_message import process_message as process_message
from .Utils.proto_utils import from_object as from_object, normalize as normalize, to_dict as to_dict
from .Utils.use_multi_file_auth_state import use_multi_file_auth_state as use_multi_file_auth_state
from .WABinary import *  # noqa: F401,F403
from .WAProto import WAProto_pb2 as WAProto  # noqa: F401
from .WAProto import WAProto_pb2 as proto  # noqa: F401
from .WAUSync import *  # noqa: F401,F403
from .Socket import make_wa_socket as make_wa_socket, makeWASocket as makeWASocket

from . import filters as filters, schemas as schemas  # noqa: E402,F401
from .client import Client as Client
from .commands import Command as Command
from .events import (
    Event as Event,
    ConnectionEvent as ConnectionEvent,
    HistoryEvent as HistoryEvent,
    MessageUpdateEvent as MessageUpdateEvent,
    MessageDeleteEvent as MessageDeleteEvent,
    ReactionEvent as ReactionEvent,
    ReceiptEvent as ReceiptEvent,
    PresenceEvent as PresenceEvent,
    ParticipantsEvent as ParticipantsEvent,
    ChatEvent as ChatEvent,
    ChatDeleteEvent as ChatDeleteEvent,
    ContactEvent as ContactEvent,
    GroupEvent as GroupEvent,
    JoinRequestEvent as JoinRequestEvent,
    CallEvent as CallEvent,
    BlocklistEvent as BlocklistEvent,
    LabelEvent as LabelEvent,
    LabelAssociationEvent as LabelAssociationEvent,
    SettingsEvent as SettingsEvent,
    LidMappingEvent as LidMappingEvent,
    NewsletterEvent as NewsletterEvent,
)
from .message import Message as Message
from .models import Contact as Contact, Group as Group
from .plugins import ClientPool as ClientPool, Plugin as Plugin
from .queue import SendQueue as SendQueue
from .storage import (AuthBackend as AuthBackend, use_custom_auth_state as use_custom_auth_state,  # noqa: E402
                      use_redis_auth_state as use_redis_auth_state,
                      use_sqlite_auth_state as use_sqlite_auth_state)  # noqa: E402

# JS-style aliases
useMultiFileAuthState = use_multi_file_auth_state
default = make_wa_socket

__version__ = '7.0.0rc14'
