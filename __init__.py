"""baileys: Python port of WhiskeySockets/Baileys (WhatsApp Web multi-device API)."""
from __future__ import annotations

from .Defaults import *  # noqa: F401,F403
from .Defaults import DEFAULT_CONNECTION_CONFIG
from .Types import *  # noqa: F401,F403
from .Utils.auth_utils import (add_transaction_capability, init_auth_creds, make_cacheable_signal_key_store)
from .Utils.browser_utils import Browsers
from .Utils.chat_utils import *  # noqa: F401,F403
from .Utils.crypto import *  # noqa: F401,F403
from .Utils.event_buffer import make_event_buffer
from .Utils.generics import *  # noqa: F401,F403
from .Utils.history import *  # noqa: F401,F403
from .Utils.link_preview import get_url_info
from .Utils.logger import ILogger, logger
from .Utils.messages import *  # noqa: F401,F403
from .Utils.messages_media import *  # noqa: F401,F403
from .Utils.process_message import process_message
from .Utils.proto_utils import from_object, normalize, to_dict
from .Utils.use_multi_file_auth_state import use_multi_file_auth_state
from .WABinary import *  # noqa: F401,F403
from .WAProto import WAProto_pb2 as WAProto  # noqa: F401
from .WAProto import WAProto_pb2 as proto  # noqa: F401
from .WAUSync import *  # noqa: F401,F403
from .Socket import make_wa_socket, makeWASocket

from . import filters, schemas  # noqa: E402,F401
from .client import Client
from .commands import Command
from .events import (Event, ConnectionEvent, HistoryEvent, MessageUpdateEvent, MessageDeleteEvent, ReactionEvent, ReceiptEvent, PresenceEvent, ParticipantsEvent, ChatEvent, ChatDeleteEvent, ContactEvent, GroupEvent, JoinRequestEvent, CallEvent, BlocklistEvent, LabelEvent, LabelAssociationEvent, SettingsEvent, LidMappingEvent, NewsletterEvent)
from .message import Message
from .models import Contact, Group
from .plugins import ClientPool, Plugin
from .queue import SendQueue
from .storage import (AuthBackend, use_custom_auth_state, use_redis_auth_state,  # noqa: E402
                      use_sqlite_auth_state)  # noqa: E402

# JS-style aliases
useMultiFileAuthState = use_multi_file_auth_state
default = make_wa_socket

__version__ = '7.0.0rc14'
