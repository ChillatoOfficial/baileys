from __future__ import annotations
import asyncio
import os
import re
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from ..Defaults import (CALL_AUDIO_PREFIX, CALL_VIDEO_PREFIX, MEDIA_KEYS, URL_REGEX, WA_DEFAULT_EPHEMERAL)
from ..Types import WAMessageStatus
from ..WABinary import Boom, is_jid_group, is_jid_newsletter, is_jid_status_broadcast, jid_normalized_user
from ..WAProto import WAProto_pb2 as proto
from .crypto import sha256
from .generics import generate_message_id_v2, get_key_author, unix_timestamp_seconds
from .messages_media import (download_content_from_message, encrypted_stream, generate_thumbnail, get_audio_duration,
                             get_audio_waveform, get_raw_media_upload_data, to_buffer)
from .proto_utils import from_object, normalize, to_dict
from .reporting_utils import should_include_reporting_token

MIMETYPE_MAP = {
    'image': 'image/jpeg',
    'video': 'video/mp4',
    'document': 'application/pdf',
    'audio': 'audio/ogg; codecs=opus',
    'sticker': 'image/webp',
    'product-catalog-image': 'image/jpeg',
}

_MESSAGE_TYPE_PROTO = {
    'image': proto.Message.ImageMessage,
    'video': proto.Message.VideoMessage,
    'audio': proto.Message.AudioMessage,
    'sticker': proto.Message.StickerMessage,
    'document': proto.Message.DocumentMessage,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def extract_url_from_text(text: str) -> Optional[str]:
    """Uses a regex to test whether the string contains a URL, and returns the URL if it does."""
    m = URL_REGEX.search(text)
    return m.group(0) if m else None


async def generate_link_preview_if_required(text: str, get_url_info, logger=None):
    url = extract_url_from_text(text)
    if get_url_info and url:
        try:
            return await get_url_info(url)
        except Exception as error:  # ignore if fails
            if logger:
                logger.warn({'trace': str(error)}, 'url generation failed')
    return None


async def _assert_color(color):
    if isinstance(color, (int, float)):
        return int(color) if color > 0 else 0xFFFFFFFF + int(color) + 1
    hex_ = color.strip().replace('#', '')
    if len(hex_) <= 6:
        hex_ = 'FF' + hex_.rjust(6, '0')
    return int(hex_, 16)


async def prepare_wa_message_media(message: dict, options: dict) -> dict:
    logger = options.get('logger')
    media_type = None
    for key in MEDIA_KEYS:
        if key in message:
            media_type = key
    if not media_type:
        raise Boom('Invalid media type', status_code=400)

    upload_data = {**message, 'media': message[media_type]}
    del upload_data[media_type]

    media = upload_data['media']
    cacheable_key = None
    if isinstance(media, dict) and media.get('url') and options.get('mediaCache'):
        cacheable_key = media_type + ':' + str(media['url'])

    if media_type == 'document' and not upload_data.get('fileName'):
        upload_data['fileName'] = 'file'
    if not upload_data.get('mimetype'):
        upload_data['mimetype'] = MIMETYPE_MAP.get(media_type)

    if cacheable_key:
        media_buff = options['mediaCache'].get(cacheable_key)
        if asyncio.iscoroutine(media_buff):
            media_buff = await media_buff
        if media_buff:
            if logger:
                logger.debug({'cacheableKey': cacheable_key}, 'got media cache hit')
            obj = to_dict(_decode_message(media_buff))
            key = f'{media_type}Message'
            extra = {k: v for k, v in upload_data.items() if k != 'media'}
            obj[key].update(extra)
            return obj

    is_newsletter = bool(options.get('jid')) and is_jid_newsletter(options['jid'])
    if is_newsletter:
        if logger:
            logger.info({'key': cacheable_key}, 'Preparing raw media for newsletter')
        raw = await get_raw_media_upload_data(upload_data['media'], options.get('mediaTypeOverride') or media_type,
                                              logger)
        import base64
        file_sha256_b64 = base64.b64encode(raw['fileSha256']).decode()
        up = await options['upload'](raw['filePath'], mediaType=media_type, fileEncSha256B64=file_sha256_b64,
                                     timeoutMs=options.get('mediaUploadTimeoutMs'))
        os.unlink(raw['filePath'])
        inner = {'url': up['mediaUrl'], 'directPath': up['directPath'], 'fileSha256': raw['fileSha256'],
                 'fileLength': raw['fileLength'], **{k: v for k, v in upload_data.items() if k != 'media'}}
        obj = normalize(proto.Message, {f'{media_type}Message': inner})
        if upload_data.get('ptv'):
            obj['ptvMessage'] = obj.pop('videoMessage')
        if obj.get('stickerMessage'):
            obj['stickerMessage']['stickerSentTs'] = _now_ms()
        if cacheable_key:
            if logger:
                logger.debug({'cacheableKey': cacheable_key}, 'set cache')
            r = options['mediaCache'].set(cacheable_key, from_object(proto.Message, obj).SerializeToString())
            if asyncio.iscoroutine(r):
                await r
        return obj

    requires_duration = media_type == 'audio' and upload_data.get('seconds') is None
    requires_thumbnail = media_type in ('image', 'video') and upload_data.get('jpegThumbnail') is None
    requires_waveform = media_type == 'audio' and upload_data.get('ptt') is True and upload_data.get('waveform') is None
    requires_audio_background = options.get('backgroundColor') and media_type == 'audio' and \
        upload_data.get('ptt') is True
    requires_original = requires_duration or requires_thumbnail
    enc = await encrypted_stream(upload_data['media'], options.get('mediaTypeOverride') or media_type, logger=logger,
                                 save_original_file_if_required=requires_original, opts=options.get('options'))
    import base64
    file_enc_sha256_b64 = base64.b64encode(enc['fileEncSha256']).decode()

    async def do_upload():
        result = await options['upload'](enc['encFilePath'], mediaType=media_type,
                                         fileEncSha256B64=file_enc_sha256_b64,
                                         timeoutMs=options.get('mediaUploadTimeoutMs'))
        if logger:
            logger.debug({'mediaType': media_type, 'cacheableKey': cacheable_key}, 'uploaded media')
        return result

    async def do_extra():
        try:
            if requires_thumbnail:
                r = await generate_thumbnail(enc['originalFilePath'], media_type, options)
                upload_data['jpegThumbnail'] = r['thumbnail']
                dims = r['originalImageDimensions']
                if not upload_data.get('width') and dims:
                    upload_data['width'] = dims['width']
                    upload_data['height'] = dims['height']
                    if logger:
                        logger.debug('set dimensions')
                if logger:
                    logger.debug('generated thumbnail')
            if requires_duration:
                upload_data['seconds'] = await get_audio_duration(enc['originalFilePath'])
                if logger:
                    logger.debug('computed audio duration')
            if requires_waveform:
                upload_data['waveform'] = await get_audio_waveform(enc['originalFilePath'], logger)
                if logger:
                    logger.debug('processed waveform')
            if requires_audio_background:
                upload_data['backgroundArgb'] = await _assert_color(options['backgroundColor'])
                if logger:
                    logger.debug('computed backgroundColor audio status')
        except Exception as error:
            if logger:
                logger.warn({'trace': str(error)}, 'failed to obtain extra info')

    try:
        up, _ = await asyncio.gather(do_upload(), do_extra())
    finally:
        try:
            os.unlink(enc['encFilePath'])
            if enc.get('originalFilePath'):
                os.unlink(enc['originalFilePath'])
            if logger:
                logger.debug('removed tmp files')
        except OSError:
            if logger:
                logger.warn('failed to remove tmp file')

    inner = {'url': up['mediaUrl'], 'directPath': up['directPath'], 'mediaKey': enc['mediaKey'],
             'fileEncSha256': enc['fileEncSha256'], 'fileSha256': enc['fileSha256'],
             'fileLength': enc['fileLength'], 'mediaKeyTimestamp': unix_timestamp_seconds(),
             **{k: v for k, v in upload_data.items() if k != 'media'}}
    obj = normalize(proto.Message, {f'{media_type}Message': inner})
    if upload_data.get('ptv'):
        obj['ptvMessage'] = obj.pop('videoMessage')
    if cacheable_key:
        if logger:
            logger.debug({'cacheableKey': cacheable_key}, 'set cache')
        r = options['mediaCache'].set(cacheable_key, from_object(proto.Message, obj).SerializeToString())
        if asyncio.iscoroutine(r):
            await r
    return obj


def _decode_message(buf: bytes):
    m = proto.Message()
    m.ParseFromString(bytes(buf))
    return m


def prepare_disappearing_message_setting_content(ephemeral_expiration: Optional[int] = None) -> dict:
    return normalize(proto.Message, {'ephemeralMessage': {'message': {'protocolMessage': {
        'type': proto.Message.ProtocolMessage.EPHEMERAL_SETTING,
        'ephemeralExpiration': ephemeral_expiration or 0}}}})


def generate_forward_message_content(message: dict, force_forward: bool = False) -> dict:
    """Generate forwarded message content like WA does"""
    content = message.get('message')
    if not content:
        raise Boom('no content in message', status_code=400)
    content = normalize(proto.Message, normalize_message_content(content))  # hacky copy
    key = next(iter(content))
    score = ((content.get(key) or {}).get('contextInfo') or {}).get('forwardingScore', 0) \
        if isinstance(content.get(key), dict) else 0
    score += 0 if (message['key'].get('fromMe') and not force_forward) else 1
    if key == 'conversation':
        content['extendedTextMessage'] = {'text': content['conversation']}
        del content['conversation']
        key = 'extendedTextMessage'
    inner = content[key]
    if score > 0:
        inner['contextInfo'] = {'forwardingScore': score, 'isForwarded': True}
    else:
        inner['contextInfo'] = {}
    return content


def has_non_nullish_property(message: dict, key: str) -> bool:
    return isinstance(message, dict) and key in message and message[key] is not None


def _has_optional(obj, key) -> bool:
    return isinstance(obj, dict) and key in obj and obj[key] is not None


async def generate_wa_message_content(message: dict, options: dict) -> dict:
    m: dict = {}
    if has_non_nullish_property(message, 'text'):
        ext = {'text': message['text']}
        url_info = message.get('linkPreview')
        if 'linkPreview' not in message:
            url_info = await generate_link_preview_if_required(message['text'], options.get('getUrlInfo'),
                                                               options.get('logger'))
        if url_info:
            ext['matchedText'] = url_info.get('matched-text')
            ext['jpegThumbnail'] = url_info.get('jpegThumbnail')
            ext['description'] = url_info.get('description')
            ext['title'] = url_info.get('title')
            ext['previewType'] = 0
            img = url_info.get('highQualityThumbnail')
            if img:
                ext['thumbnailDirectPath'] = img.get('directPath')
                ext['mediaKey'] = img.get('mediaKey')
                ext['mediaKeyTimestamp'] = img.get('mediaKeyTimestamp')
                ext['thumbnailWidth'] = img.get('width')
                ext['thumbnailHeight'] = img.get('height')
                ext['thumbnailSha256'] = img.get('fileSha256')
                ext['thumbnailEncSha256'] = img.get('fileEncSha256')
        if options.get('backgroundColor'):
            ext['backgroundArgb'] = await _assert_color(options['backgroundColor'])
        if options.get('font'):
            ext['font'] = options['font']
        m['extendedTextMessage'] = ext
    elif has_non_nullish_property(message, 'contacts'):
        contacts = message['contacts']['contacts']
        if not len(contacts):
            raise Boom('require atleast 1 contact', status_code=400)
        if len(contacts) == 1:
            m['contactMessage'] = contacts[0]
        else:
            m['contactsArrayMessage'] = message['contacts']
    elif has_non_nullish_property(message, 'location'):
        m['locationMessage'] = message['location']
    elif has_non_nullish_property(message, 'react'):
        if not message['react'].get('senderTimestampMs'):
            message['react']['senderTimestampMs'] = _now_ms()
        m['reactionMessage'] = message['react']
    elif has_non_nullish_property(message, 'delete'):
        m['protocolMessage'] = {'key': message['delete'], 'type': proto.Message.ProtocolMessage.REVOKE}
    elif has_non_nullish_property(message, 'forward'):
        m = generate_forward_message_content(message['forward'], message.get('force'))
    elif has_non_nullish_property(message, 'disappearingMessagesInChat'):
        v = message['disappearingMessagesInChat']
        exp = (WA_DEFAULT_EPHEMERAL if v else 0) if isinstance(v, bool) else v
        m = prepare_disappearing_message_setting_content(exp)
    elif has_non_nullish_property(message, 'groupInvite'):
        gi = message['groupInvite']
        m['groupInviteMessage'] = {'inviteCode': gi['inviteCode'], 'inviteExpiration': gi['inviteExpiration'],
                                   'caption': gi.get('text'), 'groupJid': gi['jid'], 'groupName': gi['subject']}
        if options.get('getProfilePicUrl'):
            pfp_url = await options['getProfilePicUrl'](gi['jid'], 'preview')
            if pfp_url:
                async with httpx.AsyncClient(follow_redirects=True) as c:
                    resp = await c.get(pfp_url)
                if resp.status_code < 400:
                    m['groupInviteMessage']['jpegThumbnail'] = resp.content
    elif has_non_nullish_property(message, 'pin'):
        m['pinInChatMessage'] = {'key': message['pin'], 'type': message.get('type'), 'senderTimestampMs': _now_ms()}
        m['messageContextInfo'] = {
            'messageAddOnDurationInSecs': (message.get('time') or 86400) if message.get('type') == 1 else 0}
    elif has_non_nullish_property(message, 'buttonReply'):
        br = message['buttonReply']
        if message.get('type') == 'template':
            m['templateButtonReplyMessage'] = {'selectedDisplayText': br.get('displayText'),
                                               'selectedId': br.get('id'), 'selectedIndex': br.get('index')}
        elif message.get('type') == 'plain':
            m['buttonsResponseMessage'] = {'selectedButtonId': br.get('id'),
                                           'selectedDisplayText': br.get('displayText'),
                                           'type': proto.Message.ButtonsResponseMessage.DISPLAY_TEXT}
    elif _has_optional(message, 'ptv') and message['ptv']:
        r = await prepare_wa_message_media({'video': message['video']}, options)
        m['ptvMessage'] = r.get('videoMessage')
    elif has_non_nullish_property(message, 'product'):
        r = await prepare_wa_message_media({'image': message['product']['productImage']}, options)
        m['productMessage'] = {**{k: v for k, v in message.items() if k != 'product'},
                               'product': {**message['product'], 'productImage': r.get('imageMessage')}}
    elif has_non_nullish_property(message, 'listReply'):
        m['listResponseMessage'] = {**message['listReply']}
    elif has_non_nullish_property(message, 'event'):
        ev = message['event']
        start_time = int(ev['startDate'].timestamp())
        m['eventMessage'] = {}
        if ev.get('call') and options.get('getCallLink'):
            token = await options['getCallLink'](ev['call'], {'startTime': start_time})
            m['eventMessage']['joinLink'] = (CALL_AUDIO_PREFIX if ev['call'] == 'audio' else CALL_VIDEO_PREFIX) + token
        m['messageContextInfo'] = {'messageSecret': ev.get('messageSecret') or os.urandom(32)}
        m['eventMessage'].update({
            'name': ev['name'], 'description': ev.get('description'), 'startTime': start_time,
            'endTime': ev['endDate'].timestamp() if ev.get('endDate') else None,
            'isCanceled': ev.get('isCancelled') or False, 'extraGuestsAllowed': ev.get('extraGuestsAllowed'),
            'isScheduleCall': ev.get('isScheduleCall') or False, 'location': ev.get('location')})
    elif has_non_nullish_property(message, 'poll'):
        poll = message['poll']
        poll['selectableCount'] = poll.get('selectableCount') or 0
        poll['toAnnouncementGroup'] = poll.get('toAnnouncementGroup') or False
        if not isinstance(poll.get('values'), list):
            raise Boom('Invalid poll values', status_code=400)
        if poll['selectableCount'] < 0 or poll['selectableCount'] > len(poll['values']):
            raise Boom(f"poll.selectableCount in poll should be >= 0 and <= {len(poll['values'])}", status_code=400)
        m['messageContextInfo'] = {'messageSecret': poll.get('messageSecret') or os.urandom(32)}
        creation = {'name': poll['name'], 'selectableOptionsCount': poll['selectableCount'],
                    'options': [{'optionName': n} for n in poll['values']]}
        if poll['toAnnouncementGroup']:
            m['pollCreationMessageV2'] = creation  # poll v2 is for community announcement groups
        elif poll['selectableCount'] == 1:
            m['pollCreationMessageV3'] = creation  # poll v3 is for single select polls
        else:
            m['pollCreationMessage'] = creation  # poll for multiple choice polls
    elif has_non_nullish_property(message, 'album'):
        m['albumMessage'] = {'expectedImageCount': message['album'].get('expectedImageCount'),
                             'expectedVideoCount': message['album'].get('expectedVideoCount')}
    elif has_non_nullish_property(message, 'sharePhoneNumber'):
        m['protocolMessage'] = {'type': proto.Message.ProtocolMessage.SHARE_PHONE_NUMBER}
    elif has_non_nullish_property(message, 'requestPhoneNumber'):
        m['requestPhoneNumberMessage'] = {}
    elif has_non_nullish_property(message, 'limitSharing'):
        m['protocolMessage'] = {
            'type': proto.Message.ProtocolMessage.LIMIT_SHARING,
            'limitSharing': {'sharingLimited': message['limitSharing'] is True, 'trigger': 1,
                             'limitSharingSettingTimestamp': _now_ms(), 'initiatedByMe': True}}
    else:
        m = await prepare_wa_message_media(message, options)

    if _has_optional(message, 'viewOnce') and message['viewOnce']:
        m = {'viewOnceMessage': {'message': m}}

    if (_has_optional(message, 'mentions') and message['mentions']) or \
            (_has_optional(message, 'mentionAll') and message['mentionAll']):
        message_type = next(iter(m))
        key = m[message_type]
        if isinstance(key, dict) and 'contextInfo' in key:
            key['contextInfo'] = key['contextInfo'] or {}
            if message.get('mentions'):
                key['contextInfo']['mentionedJid'] = message['mentions']
            if message.get('mentionAll'):
                key['contextInfo']['nonJidMentions'] = 1
        elif isinstance(key, dict):
            key['contextInfo'] = {'mentionedJid': message.get('mentions'),
                                  'nonJidMentions': 1 if message.get('mentionAll') else 0}

    if _has_optional(message, 'edit'):
        m = {'protocolMessage': {'key': message['edit'], 'editedMessage': m, 'timestampMs': _now_ms(),
                                 'type': proto.Message.ProtocolMessage.MESSAGE_EDIT}}

    if _has_optional(message, 'contextInfo') and message['contextInfo']:
        message_type = next(iter(m))
        key = m[message_type]
        if isinstance(key, dict) and key.get('contextInfo'):
            key['contextInfo'] = {**key['contextInfo'], **message['contextInfo']}
        elif isinstance(key, dict):
            key['contextInfo'] = message['contextInfo']

    if _has_optional(message, 'albumParentKey') and message['albumParentKey']:
        m['messageContextInfo'] = {**(m.get('messageContextInfo') or {}), 'messageAssociation': {
            'associationType': proto.MessageAssociation.MEDIA_ALBUM,
            'parentMessageKey': message['albumParentKey']}}

    if should_include_reporting_token(m):
        m['messageContextInfo'] = m.get('messageContextInfo') or {}
        if not m['messageContextInfo'].get('messageSecret'):
            m['messageContextInfo']['messageSecret'] = os.urandom(32)

    return normalize(proto.Message, m)


def generate_wa_message_from_content(jid: str, message: dict, options: dict) -> dict:
    if not options.get('timestamp'):
        options['timestamp'] = datetime.now()
    inner_message = normalize_message_content(message)
    key = get_content_type(inner_message)
    timestamp = unix_timestamp_seconds(options['timestamp'])
    quoted, user_jid = options.get('quoted'), options.get('userJid')

    if quoted and not is_jid_newsletter(jid):
        participant = user_jid if quoted['key'].get('fromMe') else (
            quoted.get('participant') or quoted['key'].get('participant') or quoted['key'].get('remoteJid'))
        quoted_msg = normalize_message_content(quoted['message'])
        msg_type = get_content_type(quoted_msg)
        quoted_msg = {msg_type: quoted_msg[msg_type]}  # strip any redundant properties
        quoted_content = quoted_msg[msg_type]
        if isinstance(quoted_content, dict) and 'contextInfo' in quoted_content:
            quoted_content = {k: v for k, v in quoted_content.items() if k != 'contextInfo'}
            quoted_msg[msg_type] = quoted_content
        inner_val = inner_message[key]
        context_info = (inner_val.get('contextInfo') if isinstance(inner_val, dict) else None) or {}
        context_info['participant'] = jid_normalized_user(participant)
        context_info['stanzaId'] = quoted['key'].get('id')
        context_info['quotedMessage'] = quoted_msg
        # if a participant is quoted, then it must be a group; hence, remoteJid of group must also be entered
        if jid != quoted['key'].get('remoteJid'):
            context_info['remoteJid'] = quoted['key'].get('remoteJid')
        if context_info and isinstance(inner_val, dict):
            inner_val['contextInfo'] = context_info

    if (options.get('ephemeralExpiration') and key != 'protocolMessage' and key != 'ephemeralMessage'
            and not is_jid_newsletter(jid)):
        inner_val = inner_message[key]
        inner_val['contextInfo'] = {**(inner_val.get('contextInfo') or {}),
                                    'expiration': options.get('ephemeralExpiration') or WA_DEFAULT_EPHEMERAL}

    message_json = {
        'key': {'remoteJid': jid, 'fromMe': True, 'id': options.get('messageId') or generate_message_id_v2()},
        'message': message,
        'messageTimestamp': timestamp,
        'messageStubParameters': [],
        'participant': user_jid if (is_jid_group(jid) or is_jid_status_broadcast(jid)) else None,
        'status': WAMessageStatus.PENDING,
    }
    return normalize(proto.WebMessageInfo, message_json)


async def generate_wa_message(jid: str, content: dict, options: dict) -> dict:
    logger = options.get('logger')
    if logger:
        options['logger'] = logger.child({'msgId': options.get('messageId')})
    return generate_wa_message_from_content(
        jid, await generate_wa_message_content(content, {**options, 'jid': jid}), options)


def get_content_type(content: Optional[dict]) -> Optional[str]:
    """Get the key to access the true type of content"""
    if content:
        for k in content.keys():
            if (k == 'conversation' or 'Message' in k) and k != 'senderKeyDistributionMessage':
                return k
    return None


def normalize_message_content(content: Optional[dict]) -> Optional[dict]:
    """Normalizes ephemeral, view once messages to regular message content"""
    if not content:
        return None

    def get_future_proof_message(message):
        for k in ('ephemeralMessage', 'viewOnceMessage', 'documentWithCaptionMessage', 'viewOnceMessageV2',
                  'viewOnceMessageV2Extension', 'editedMessage', 'associatedChildMessage', 'groupStatusMessage',
                  'groupStatusMessageV2'):
            if message and message.get(k):
                return message[k]
        return None

    for _ in range(5):  # max iterations to prevent an infinite loop
        inner = get_future_proof_message(content)
        if not inner:
            break
        content = inner.get('message')
        if content is None:
            return None
    return content


def extract_message_content(content: Optional[dict]) -> Optional[dict]:
    """Extract the true message content from a message (inner message of ephemeral / view once / templates)"""
    def extract_from_template_message(msg: dict):
        if msg.get('imageMessage'):
            return {'imageMessage': msg['imageMessage']}
        if msg.get('documentMessage'):
            return {'documentMessage': msg['documentMessage']}
        if msg.get('videoMessage'):
            return {'videoMessage': msg['videoMessage']}
        if msg.get('locationMessage'):
            return {'locationMessage': msg['locationMessage']}
        return {'conversation': msg.get('contentText', msg.get('hydratedContentText', '')) if (
            'contentText' in msg or 'hydratedContentText' in msg) else ''}

    content = normalize_message_content(content)
    if content and content.get('buttonsMessage'):
        return extract_from_template_message(content['buttonsMessage'])
    tm = (content or {}).get('templateMessage')
    if tm:
        for k in ('hydratedFourRowTemplate', 'hydratedTemplate', 'fourRowTemplate'):
            if tm.get(k):
                return extract_from_template_message(tm[k])
    return content


def get_device(id_: str) -> str:
    """Returns the device predicted by message ID"""
    if re.match(r'^3A.{18}$', id_, re.S):
        return 'ios'
    if re.match(r'^3E.{20}$', id_, re.S):
        return 'web'
    if re.match(r'^(.{21}|.{32})$', id_, re.S):
        return 'android'
    if re.match(r'^(3F|.{18}$)', id_, re.S):
        return 'desktop'
    return 'unknown'


def update_message_with_receipt(msg: dict, receipt: dict):
    """Upserts a receipt in the message"""
    msg.setdefault('userReceipt', [])
    msg['userReceipt'] = msg['userReceipt'] or []
    for r in msg['userReceipt']:
        if r.get('userJid') == receipt.get('userJid'):
            r.update(receipt)
            return
    msg['userReceipt'].append(receipt)


def update_message_with_reaction(msg: dict, reaction: dict):
    """Update the message with a new reaction"""
    author_id = get_key_author(reaction.get('key'))
    reactions = [r for r in (msg.get('reactions') or []) if get_key_author(r.get('key')) != author_id]
    reaction['text'] = reaction.get('text') or ''
    reactions.append(reaction)
    msg['reactions'] = reactions


def update_message_with_poll_update(msg: dict, update: dict):
    author_id = get_key_author(update.get('pollUpdateMessageKey'))
    reactions = [r for r in (msg.get('pollUpdates') or []) if get_key_author(r.get('pollUpdateMessageKey')) != author_id]
    if (update.get('vote') or {}).get('selectedOptions'):
        reactions.append(update)
    msg['pollUpdates'] = reactions


def update_message_with_event_response(msg: dict, update: dict):
    author_id = get_key_author(update.get('eventResponseMessageKey'))
    responses = [r for r in (msg.get('eventResponses') or [])
                 if get_key_author(r.get('eventResponseMessageKey')) != author_id]
    responses.append(update)
    msg['eventResponses'] = responses


def _opt_hash(b: bytes) -> str:
    return bytes(b).decode('utf-8', errors='replace')


def get_aggregate_votes_in_poll_message(msg: dict, me_id: Optional[str] = None) -> list:
    """Aggregates all poll updates in a poll. Returns a list of options & their voters"""
    message = msg.get('message') or {}
    opts = ((message.get('pollCreationMessage') or {}).get('options') or
            (message.get('pollCreationMessageV2') or {}).get('options') or
            (message.get('pollCreationMessageV3') or {}).get('options') or [])
    vote_hash_map = {}
    for opt in opts:
        h = _opt_hash(sha256((opt.get('optionName') or '').encode()))
        vote_hash_map[h] = {'name': opt.get('optionName') or '', 'voters': []}
    for update in msg.get('pollUpdates') or []:
        vote = update.get('vote')
        if not vote:
            continue
        for option in vote.get('selectedOptions') or []:
            h = _opt_hash(option)
            if h not in vote_hash_map:
                vote_hash_map[h] = {'name': 'Unknown', 'voters': []}
            vote_hash_map[h]['voters'].append(get_key_author(update.get('pollUpdateMessageKey'), me_id or 'me'))
    return list(vote_hash_map.values())


def get_aggregate_responses_in_event_message(msg: dict, me_id: Optional[str] = None) -> list:
    response_map = {t: {'response': t, 'responders': []} for t in ('GOING', 'NOT_GOING', 'MAYBE')}
    for update in msg.get('eventResponses') or []:
        response_type = update.get('eventResponse') or 'UNKNOWN'
        if isinstance(response_type, int):
            response_type = proto.Message.EventResponseMessage.EventResponseType.Name(response_type)
        if response_type != 'UNKNOWN' and response_type in response_map:
            response_map[response_type]['responders'].append(
                get_key_author(update.get('eventResponseMessageKey'), me_id or 'me'))
    return list(response_map.values())


def aggregate_message_keys_not_from_me(keys: list) -> list:
    """Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk"""
    key_map = {}
    for k in keys:
        if not k.get('fromMe'):
            uq = f"{k.get('remoteJid')}:{k.get('participant') or ''}"
            if uq not in key_map:
                key_map[uq] = {'jid': k.get('remoteJid'), 'participant': k.get('participant'), 'messageIds': []}
            key_map[uq]['messageIds'].append(k.get('id'))
    return list(key_map.values())


REUPLOAD_REQUIRED_STATUS = [410, 404]


async def download_media_message(message: dict, type_: str = 'buffer', options: Optional[dict] = None,
                                 ctx: Optional[dict] = None):
    """Downloads the given message. Throws an error if it's not a media message.
    type_ 'buffer' returns bytes, 'stream' returns an async iterator of bytes."""
    options = options or {}

    async def download_msg():
        m_content = extract_message_content(message.get('message'))
        if not m_content:
            raise Boom('No message present', status_code=400, data=message)
        content_type = get_content_type(m_content)
        media_type = content_type.replace('Message', '') if content_type else None
        media = m_content.get(content_type)
        if not media or not isinstance(media, dict) or ('url' not in media and 'thumbnailDirectPath' not in media):
            raise Boom(f'"{content_type}" message is not a media message')
        if 'thumbnailDirectPath' in media and 'url' not in media:
            download = {'directPath': media['thumbnailDirectPath'], 'mediaKey': media.get('mediaKey')}
            media_type = 'thumbnail-link'
        else:
            download = media
        stream = await download_content_from_message(download, media_type, options)
        if type_ == 'buffer':
            return await to_buffer(stream)
        return stream

    try:
        return await download_msg()
    except Exception as error:
        status = getattr(error, 'status', None)
        if status is None and isinstance(error, Boom):
            status = error.status_code
        if ctx and isinstance(status, int) and status in REUPLOAD_REQUIRED_STATUS:
            ctx['logger'].info({'key': message.get('key')}, 'sending reupload media request...')
            message = await ctx['reuploadRequest'](message)
            return await download_msg()
        raise


def assert_media_content(content: Optional[dict]) -> dict:
    """Checks whether the given message is a media message; if it is returns the inner content"""
    content = extract_message_content(content) or {}
    media = (content.get('documentMessage') or content.get('imageMessage') or content.get('videoMessage') or
             content.get('audioMessage') or content.get('stickerMessage'))
    if not media:
        raise Boom('given message is not a media message', status_code=400, data=content)
    return media
