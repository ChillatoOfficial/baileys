from __future__ import annotations
from enum import Enum, IntEnum
from ..WAProto import WAProto_pb2 as WAProto

# Python port note: the JS type definitions are TypeScript-only. Runtime values (enums / constants)
# live here; structural types (Chat, Contact, GroupMetadata ...) are plain dicts.

WAMessageStubType = WAProto.WebMessageInfo.StubType
WAMessageStatus = WAProto.WebMessageInfo.Status


class DisconnectReason(IntEnum):
    connectionClosed = 428
    connectionLost = 408
    connectionReplaced = 440
    timedOut = 408
    loggedOut = 401
    badSession = 500
    restartRequired = 515
    multideviceMismatch = 411
    forbidden = 403
    unavailableService = 503


class WAMessageAddressingMode(str, Enum):
    PN = 'pn'
    LID = 'lid'


class SyncState(IntEnum):
    Connecting = 0
    AwaitingInitialSync = 1
    Syncing = 2
    Online = 3


class ReachoutTimelockEnforcementType(str, Enum):
    BIZ_COMMERCE_VIOLATION_ALCOHOL = 'BIZ_COMMERCE_VIOLATION_ALCOHOL'
    BIZ_COMMERCE_VIOLATION_ADULT = 'BIZ_COMMERCE_VIOLATION_ADULT'
    BIZ_COMMERCE_VIOLATION_ANIMALS = 'BIZ_COMMERCE_VIOLATION_ANIMALS'
    BIZ_COMMERCE_VIOLATION_BODY_PARTS_FLUIDS = 'BIZ_COMMERCE_VIOLATION_BODY_PARTS_FLUIDS'
    BIZ_COMMERCE_VIOLATION_DATING = 'BIZ_COMMERCE_VIOLATION_DATING'
    BIZ_COMMERCE_VIOLATION_DIGITAL_SERVICES_PRODUCTS = 'BIZ_COMMERCE_VIOLATION_DIGITAL_SERVICES_PRODUCTS'
    BIZ_COMMERCE_VIOLATION_DRUGS = 'BIZ_COMMERCE_VIOLATION_DRUGS'
    BIZ_COMMERCE_VIOLATION_DRUGS_ONLY_OTC = 'BIZ_COMMERCE_VIOLATION_DRUGS_ONLY_OTC'
    BIZ_COMMERCE_VIOLATION_GAMBLING = 'BIZ_COMMERCE_VIOLATION_GAMBLING'
    BIZ_COMMERCE_VIOLATION_HEALTHCARE = 'BIZ_COMMERCE_VIOLATION_HEALTHCARE'
    BIZ_COMMERCE_VIOLATION_REAL_FAKE_CURRENCY = 'BIZ_COMMERCE_VIOLATION_REAL_FAKE_CURRENCY'
    BIZ_COMMERCE_VIOLATION_SUPPLEMENTS = 'BIZ_COMMERCE_VIOLATION_SUPPLEMENTS'
    BIZ_COMMERCE_VIOLATION_TOBACCO = 'BIZ_COMMERCE_VIOLATION_TOBACCO'
    BIZ_COMMERCE_VIOLATION_VIOLENT_CONTENT = 'BIZ_COMMERCE_VIOLATION_VIOLENT_CONTENT'
    BIZ_COMMERCE_VIOLATION_WEAPONS = 'BIZ_COMMERCE_VIOLATION_WEAPONS'
    BIZ_QUALITY = 'BIZ_QUALITY'
    DEFAULT = 'DEFAULT'
    WEB_COMPANION_ONLY = 'WEB_COMPANION_ONLY'


class NewChatMessageCappingStatusType(str, Enum):
    NONE = 'NONE'
    FIRST_WARNING = 'FIRST_WARNING'
    SECOND_WARNING = 'SECOND_WARNING'
    CAPPED = 'CAPPED'


class NewChatMessageCappingMVStatusType(str, Enum):
    NOT_ELIGIBLE = 'NOT_ELIGIBLE'
    NOT_ACTIVE = 'NOT_ACTIVE'
    ACTIVE = 'ACTIVE'
    ACTIVE_UPGRADE_AVAILABLE = 'ACTIVE_UPGRADE_AVAILABLE'


class NewChatMessageCappingOTEStatusType(str, Enum):
    NOT_ELIGIBLE = 'NOT_ELIGIBLE'
    ELIGIBLE = 'ELIGIBLE'
    ACTIVE_IN_CURRENT_CYCLE = 'ACTIVE_IN_CURRENT_CYCLE'
    EXHAUSTED = 'EXHAUSTED'


class LabelColor(IntEnum):
    Color1 = 0
    Color2 = 1
    Color3 = 2
    Color4 = 3
    Color5 = 4
    Color6 = 5
    Color7 = 6
    Color8 = 7
    Color9 = 8
    Color10 = 9
    Color11 = 10
    Color12 = 11
    Color13 = 12
    Color14 = 13
    Color15 = 14
    Color16 = 15
    Color17 = 16
    Color18 = 17
    Color19 = 18
    Color20 = 19


class LabelAssociationType(str, Enum):
    Chat = 'label_jid'
    Message = 'label_message'


ALL_WA_PATCH_NAMES = ['critical_block', 'critical_unblock_low', 'regular_high', 'regular_low', 'regular']

WAMessageKey = dict
WAMessage = WAProto.WebMessageInfo


class XWAPaths(str, Enum):
    xwa2_newsletter_create = 'xwa2_newsletter_create'
    xwa2_newsletter_subscribers = 'xwa2_newsletter_subscribers'
    xwa2_newsletter_view = 'xwa2_newsletter_view'
    xwa2_newsletter_metadata = 'xwa2_newsletter'
    xwa2_newsletter_admin_count = 'xwa2_newsletter_admin'
    xwa2_newsletter_mute_v2 = 'xwa2_newsletter_mute_v2'
    xwa2_newsletter_unmute_v2 = 'xwa2_newsletter_unmute_v2'
    xwa2_newsletter_follow = 'xwa2_newsletter_follow'
    xwa2_newsletter_unfollow = 'xwa2_newsletter_unfollow'
    xwa2_newsletter_join_v2 = 'xwa2_newsletter_join_v2'
    xwa2_newsletter_leave_v2 = 'xwa2_newsletter_leave_v2'
    xwa2_newsletter_change_owner = 'xwa2_newsletter_change_owner'
    xwa2_newsletter_demote = 'xwa2_newsletter_demote'
    xwa2_newsletter_delete_v2 = 'xwa2_newsletter_delete_v2'
    xwa2_fetch_account_reachout_timelock = 'xwa2_fetch_account_reachout_timelock'
    xwa2_message_capping_info = 'xwa2_message_capping_info'


class QueryIds(str, Enum):
    CREATE = '8823471724422422'
    UPDATE_METADATA = '24250201037901610'
    METADATA = '6563316087068696'
    SUBSCRIBERS = '9783111038412085'
    FOLLOW = '24404358912487870'
    UNFOLLOW = '9767147403369991'
    MUTE = '29766401636284406'
    UNMUTE = '9864994326891137'
    ADMIN_COUNT = '7130823597031706'
    CHANGE_OWNER = '7341777602580933'
    DEMOTE = '6551828931592903'
    DELETE = '30062808666639665'
    REACHOUT_TIMELOCK = '23983697327930364'
    MESSAGE_CAPPING_INFO = '24503548349331633'
