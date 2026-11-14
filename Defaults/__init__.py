from __future__ import annotations
import re
from ..WAProto import WAProto_pb2 as proto
from ..Utils.logger import logger
from ..Utils.browser_utils import Browsers

version = [2, 3000, 1043857760]

UNAUTHORIZED_CODES = [401, 403, 419]

DEFAULT_ORIGIN = 'https://web.whatsapp.com'
CALL_VIDEO_PREFIX = 'https://call.whatsapp.com/video/'
CALL_AUDIO_PREFIX = 'https://call.whatsapp.com/voice/'
DEF_CALLBACK_PREFIX = 'CB:'
DEF_TAG_PREFIX = 'TAG:'
PHONE_CONNECTION_CB = 'CB:Pong'

WA_ADV_ACCOUNT_SIG_PREFIX = bytes([6, 0])
WA_ADV_DEVICE_SIG_PREFIX = bytes([6, 1])
WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX = bytes([6, 5])
WA_ADV_HOSTED_DEVICE_SIG_PREFIX = bytes([6, 6])

WA_DEFAULT_EPHEMERAL = 7 * 24 * 60 * 60

# Status messages older than 24 hours are considered expired
STATUS_EXPIRY_SECONDS = 24 * 60 * 60

# WA Web enforces a 14-day maximum age for placeholder resend requests
PLACEHOLDER_MAX_AGE_SECONDS = 14 * 24 * 60 * 60

NOISE_MODE = 'Noise_XX_25519_AESGCM_SHA256\0\0\0\0'
DICT_VERSION = 3
KEY_BUNDLE_TYPE = bytes([5])
NOISE_WA_HEADER = bytes([87, 65, 6, DICT_VERSION])  # last is "DICT_VERSION"
URL_REGEX = re.compile(r'https://(?![^:@/\s]+:[^:@/\s]+@)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(:\d+)?(/[^\s]*)?')

WA_CERT_DETAILS = {
    'SERIAL': 0,
    'ISSUER': 'WhatsAppLongTerm1',
    'PUBLIC_KEY': bytes.fromhex('142375574d0a587166aae71ebe516437c4a28b73e3695c6ce1f7f9545da8ee6b'),
}

_HST = proto.HistorySync
PROCESSABLE_HISTORY_TYPES = [
    _HST.INITIAL_BOOTSTRAP, _HST.PUSH_NAME, _HST.RECENT, _HST.FULL, _HST.ON_DEMAND,
    _HST.NON_BLOCKING_DATA, _HST.INITIAL_STATUS_V3,
]

DEFAULT_CACHE_TTLS = {
    'SIGNAL_STORE': 5 * 60,  # 5 minutes
    'MSG_RETRY': 60 * 60,  # 1 hour
    'CALL_OFFER': 5 * 60,  # 5 minutes
    'USER_DEVICES': 5 * 60,  # 5 minutes
}


def _make_signal_repository(*args, **kwargs):
    from ..Signal.libsignal import make_lib_signal_repository
    return make_lib_signal_repository(*args, **kwargs)


def _should_sync_history_message(msg) -> bool:
    sync_type = msg.get('syncType') if isinstance(msg, dict) else getattr(msg, 'syncType', None)
    return sync_type != proto.HistorySync.FULL


DEFAULT_CONNECTION_CONFIG: dict = {
    'version': version,
    'browser': Browsers.macOS('Chrome'),
    'waWebSocketUrl': 'wss://web.whatsapp.com/ws/chat',
    'connectTimeoutMs': 20_000,
    'keepAliveIntervalMs': 30_000,
    'logger': logger.child({'class': 'baileys'}),
    'emitOwnEvents': True,
    'defaultQueryTimeoutMs': 60_000,
    'customUploadHosts': [],
    'retryRequestDelayMs': 250,
    'maxMsgRetryCount': 5,
    'fireInitQueries': True,
    'auth': None,
    'markOnlineOnConnect': True,
    'syncFullHistory': True,
    'patchMessageBeforeSending': lambda msg, recipient_jids=None: msg,
    'shouldSyncHistoryMessage': _should_sync_history_message,
    'shouldIgnoreJid': lambda jid: False,
    'linkPreviewImageThumbnailWidth': 192,
    'transactionOpts': {'maxCommitRetries': 10, 'delayBetweenTriesMs': 3000},
    'generateHighQualityLinkPreview': False,
    'enableAutoSessionRecreation': True,
    'enableRecentMessageCache': True,
    'options': {},
    'appStateMacVerification': {'patch': False, 'snapshot': False},
    'countryCode': 'US',
    'getMessage': None,  # async (key) -> Message | None
    'cachedGroupMetadata': None,  # async (jid) -> GroupMetadata | None
    'makeSignalRepository': _make_signal_repository,
}

MEDIA_PATH_MAP = {
    'image': '/mms/image',
    'video': '/mms/video',
    'document': '/mms/document',
    'audio': '/mms/audio',
    'sticker': '/mms/image',
    'thumbnail-link': '/mms/image',
    'product-catalog-image': '/product/image',
    'md-app-state': '',
    'md-msg-hist': '/mms/md-app-state',
    'biz-cover-photo': '/pps/biz-cover-photo',
}

MEDIA_HKDF_KEY_MAPPING = {
    'audio': 'Audio',
    'document': 'Document',
    'gif': 'Video',
    'image': 'Image',
    'ppic': '',
    'product': 'Image',
    'ptt': 'Audio',
    'sticker': 'Image',
    'video': 'Video',
    'thumbnail-document': 'Document Thumbnail',
    'thumbnail-image': 'Image Thumbnail',
    'thumbnail-video': 'Video Thumbnail',
    'thumbnail-link': 'Link Thumbnail',
    'md-msg-hist': 'History',
    'md-app-state': 'App State',
    'product-catalog-image': '',
    'payment-bg-image': 'Payment Background',
    'ptv': 'Video',
    'biz-cover-photo': 'Image',
}

MEDIA_KEYS = list(MEDIA_PATH_MAP.keys())

# 120s timeout for history sync stall detection
HISTORY_SYNC_PAUSED_TIMEOUT_MS = 120_000

MIN_PREKEY_COUNT = 5
INITIAL_PREKEY_COUNT = 812
UPLOAD_TIMEOUT = 30000  # 30 seconds


class TimeMs:
    Minute = 60 * 1000
    Hour = 60 * 60 * 1000
    Day = 24 * 60 * 60 * 1000
    Week = 7 * 24 * 60 * 60 * 1000
