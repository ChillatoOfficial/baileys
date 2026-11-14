from __future__ import annotations
import asyncio
import hashlib
import hmac as _hmac
import io
import json
import os
import subprocess
import tempfile
from typing import Any, AsyncIterator, Optional
from urllib.parse import quote, urlparse

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..Defaults import DEFAULT_ORIGIN, MEDIA_HKDF_KEY_MAPPING, MEDIA_PATH_MAP
from ..WABinary import Boom, get_binary_node_child, get_binary_node_child_buffer, jid_normalized_user
from ..WAProto import WAProto_pb2 as proto
from .crypto import aes_decrypt_gcm, aes_encrypt_gcm, hkdf
from .generics import generate_message_id_v2
from .proto_utils import from_object, to_dict


def _tmp_dir() -> str:
    return tempfile.gettempdir()


def hkdf_info_key(type_: str) -> str:
    return f'WhatsApp {MEDIA_HKDF_KEY_MAPPING[type_]} Keys'


async def _aiter_chunks(item):
    """Normalises bytes / file-like / (async) iterables to an async iterator of bytes."""
    if isinstance(item, (bytes, bytearray, memoryview)):
        yield bytes(item)
    elif hasattr(item, 'read'):
        while True:
            chunk = item.read(65536)
            if asyncio.iscoroutine(chunk):
                chunk = await chunk
            if not chunk:
                break
            yield chunk
    elif hasattr(item, '__aiter__'):
        async for c in item:
            yield bytes(c)
    else:
        for c in item:
            yield bytes(c)


async def to_buffer(stream) -> bytes:
    chunks = []
    async for c in _aiter_chunks(stream):
        chunks.append(c)
    return b''.join(chunks)


def to_readable(buffer: bytes):
    return _aiter_chunks(bytes(buffer))


async def get_http_stream(url: str, options: Optional[dict] = None):
    options = options or {}
    headers = options.get('headers') or {}
    client = httpx.AsyncClient(follow_redirects=True, timeout=None, **(options.get('client') or {}))
    req = client.build_request('GET', str(url), headers=headers)
    response = await client.send(req, stream=True)
    if response.status_code >= 400:
        await response.aclose()
        await client.aclose()
        err = Boom(f'Failed to fetch stream from {url}', status_code=response.status_code, data={'url': url})
        err.status = response.status_code
        raise err

    async def gen():
        try:
            async for chunk in response.aiter_bytes():
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()
    return gen()


async def get_stream(item, opts: Optional[dict] = None):
    """Returns (stream, type). item: bytes | {'stream': ...} | {'url': str}"""
    if isinstance(item, (bytes, bytearray, memoryview)):
        return to_readable(bytes(item)), 'buffer'
    if isinstance(item, dict) and 'stream' in item:
        return _aiter_chunks(item['stream']), 'readable'
    url_str = str(item['url'])
    if url_str.startswith('data:'):
        import base64
        return to_readable(base64.b64decode(url_str.split(',', 1)[1])), 'buffer'
    if url_str.startswith('http://') or url_str.startswith('https://'):
        return await get_http_stream(url_str, opts), 'remote'

    async def file_gen():
        with open(url_str, 'rb') as f:
            while True:
                c = f.read(65536)
                if not c:
                    break
                yield c
    return file_gen(), 'file'


async def get_raw_media_upload_data(media, media_type: str, logger=None) -> dict:
    stream, _ = await get_stream(media)
    if logger:
        logger.debug('got stream for raw upload')
    hasher = hashlib.sha256()
    file_path = os.path.join(_tmp_dir(), media_type + generate_message_id_v2())
    file_length = 0
    try:
        with open(file_path, 'wb') as f:
            async for data in stream:
                file_length += len(data)
                hasher.update(data)
                f.write(data)
        if logger:
            logger.debug('hashed data for raw upload')
        return {'filePath': file_path, 'fileSha256': hasher.digest(), 'fileLength': file_length}
    except Exception:
        try:
            os.unlink(file_path)
        except OSError:
            pass
        raise


async def get_media_keys(buffer, media_type: str) -> dict:
    """generates all the keys required to encrypt/decrypt & sign a media message"""
    if not buffer:
        raise Boom('Cannot derive from empty media key')
    if isinstance(buffer, str):
        import base64
        buffer = base64.b64decode(buffer.replace('data:;base64,', ''))
    expanded = hkdf(bytes(buffer), 112, info=hkdf_info_key(media_type))
    return {'iv': expanded[:16], 'cipherKey': expanded[16:48], 'macKey': expanded[48:80]}


async def _run_ffmpeg(*args) -> None:
    proc = await asyncio.create_subprocess_exec('ffmpeg', *args, stdout=asyncio.subprocess.DEVNULL,
                                                stderr=asyncio.subprocess.DEVNULL)
    code = await proc.wait()
    if code != 0:
        raise RuntimeError(f'ffmpeg exited with {code}')


async def extract_video_thumb(path: str, dest_path: str, time: str, size: dict):
    await _run_ffmpeg('-ss', time, '-i', path, '-y', '-vf', f"scale={size['width']}:-1", '-vframes', '1',
                      '-f', 'image2', dest_path)


async def extract_image_thumb(buffer_or_path, width: int = 32) -> dict:
    from PIL import Image
    if not isinstance(buffer_or_path, (bytes, bytearray, str)):
        buffer_or_path = await to_buffer(buffer_or_path)

    def work():
        src = io.BytesIO(buffer_or_path) if isinstance(buffer_or_path, (bytes, bytearray)) else buffer_or_path
        with Image.open(src) as img:
            img.load()
            ow, oh = img.size
            h = max(1, round(oh * width / ow))
            thumb = img.convert('RGB').resize((width, h), Image.BILINEAR)
            out = io.BytesIO()
            thumb.save(out, 'JPEG', quality=50)
            return {'buffer': out.getvalue(), 'original': {'width': ow, 'height': oh}}
    return await asyncio.get_running_loop().run_in_executor(None, work)


def encode_base64_encoded_string_for_upload(b64: str) -> str:
    return quote(b64.replace('+', '-').replace('/', '_').rstrip('='), safe="-_.!~*'()")


async def generate_profile_picture(media_upload, dimensions: Optional[dict] = None) -> dict:
    from PIL import Image
    w = (dimensions or {}).get('width', 640)
    h = (dimensions or {}).get('height', 640)
    if isinstance(media_upload, (bytes, bytearray)):
        buffer = bytes(media_upload)
    else:
        stream, _ = await get_stream(media_upload)
        buffer = await to_buffer(stream)

    def work():
        with Image.open(io.BytesIO(buffer)) as img:
            img = img.convert('RGB').resize((w, h), Image.BILINEAR)
            out = io.BytesIO()
            img.save(out, 'JPEG', quality=50)
            return out.getvalue()
    return {'img': await asyncio.get_running_loop().run_in_executor(None, work)}


def media_message_sha256_b64(message: dict):
    import base64
    media = next(iter(message.values()), None)
    if isinstance(media, dict) and media.get('fileSha256'):
        return base64.b64encode(bytes(media['fileSha256'])).decode()
    return None


async def get_audio_duration(buffer) -> Optional[float]:
    import mutagen

    def work():
        if isinstance(buffer, (bytes, bytearray)):
            f = mutagen.File(io.BytesIO(buffer))
        else:
            f = mutagen.File(buffer)
        return f.info.length if f is not None and f.info is not None else None
    return await asyncio.get_running_loop().run_in_executor(None, work)


async def get_audio_waveform(buffer, logger=None):
    """Waveform of 64 samples (0..100) via ffmpeg -> raw PCM. Referenced from wa-js prepareAudioWaveform."""
    try:
        if isinstance(buffer, str):
            src_args = ['-i', buffer]
            data_in = None
        else:
            data = buffer if isinstance(buffer, (bytes, bytearray)) else await to_buffer(buffer)
            src_args, data_in = ['-i', 'pipe:0'], bytes(data)
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg', *src_args, '-f', 's16le', '-ac', '1', '-ar', '16000', 'pipe:1',
            stdin=asyncio.subprocess.PIPE if data_in is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate(data_in)
        import struct
        n = len(out) // 2
        raw = struct.unpack('<%dh' % n, out[:n * 2])
        samples = 64
        block = n // samples
        if block == 0:
            return None
        filtered = []
        for i in range(samples):
            s = sum(abs(x) for x in raw[block * i: block * (i + 1)]) / 32768.0
            filtered.append(s / block)
        mx = max(filtered)
        if mx == 0:
            return bytes(samples)
        return bytes(int(100 * v / mx) for v in filtered)
    except Exception as e:
        if logger:
            logger.debug('Failed to generate waveform: ' + str(e))
        return None


async def generate_thumbnail(file: str, media_type: str, options: Optional[dict] = None) -> dict:
    import base64
    options = options or {}
    thumbnail = None
    dims = None
    if media_type == 'image':
        r = await extract_image_thumb(file)
        thumbnail = base64.b64encode(r['buffer']).decode()
        if r['original'].get('width') and r['original'].get('height'):
            dims = {'width': r['original']['width'], 'height': r['original']['height']}
    elif media_type == 'video':
        img_filename = os.path.join(_tmp_dir(), generate_message_id_v2() + '.jpg')
        try:
            await extract_video_thumb(file, img_filename, '00:00:00', {'width': 32, 'height': 32})
            with open(img_filename, 'rb') as f:
                thumbnail = base64.b64encode(f.read()).decode()
            os.unlink(img_filename)
        except Exception as err:
            if options.get('logger'):
                options['logger'].debug('could not generate video thumb: ' + str(err))
    return {'thumbnail': thumbnail, 'originalImageDimensions': dims}


async def encrypted_stream(media, media_type: str, logger=None, save_original_file_if_required: bool = False,
                           opts: Optional[dict] = None) -> dict:
    stream, type_ = await get_stream(media, opts)
    if logger:
        logger.debug('fetched media stream')
    media_key = os.urandom(32)
    keys = await get_media_keys(media_key, media_type)
    cipher_key, iv, mac_key = keys['cipherKey'], keys['iv'], keys['macKey']
    enc_file_path = os.path.join(_tmp_dir(), media_type + generate_message_id_v2() + '-enc')
    original_file_path = None
    original_f = None
    if save_original_file_if_required:
        original_file_path = os.path.join(_tmp_dir(), media_type + generate_message_id_v2() + '-original')
        original_f = open(original_file_path, 'wb')
    enc_f = open(enc_file_path, 'wb')
    file_length = 0
    aes = Cipher(algorithms.AES(cipher_key), modes.CBC(iv)).encryptor()
    padder = padding.PKCS7(128).padder()
    hmac = _hmac.new(mac_key, digestmod=hashlib.sha256)
    hmac.update(iv)
    sha_plain, sha_enc = hashlib.sha256(), hashlib.sha256()

    def on_chunk(buff: bytes):
        sha_enc.update(buff)
        hmac.update(buff)
        enc_f.write(buff)

    try:
        max_len = (opts or {}).get('maxContentLength')
        async for data in stream:
            file_length += len(data)
            if type_ == 'remote' and max_len and file_length + len(data) > max_len:
                raise Boom(f'content length exceeded when encrypting "{type_}"', data={'media': media, 'type': type_})
            if original_f:
                original_f.write(data)
            sha_plain.update(data)
            on_chunk(aes.update(padder.update(data)))
        on_chunk(aes.update(padder.finalize()) + aes.finalize())
        mac = hmac.digest()[:10]
        sha_enc.update(mac)
        file_sha256 = sha_plain.digest()
        file_enc_sha256 = sha_enc.digest()
        enc_f.write(mac)
        enc_f.close()
        if original_f:
            original_f.close()
        if logger:
            logger.debug('encrypted data successfully')
        return {'mediaKey': media_key, 'originalFilePath': original_file_path, 'encFilePath': enc_file_path,
                'mac': mac, 'fileEncSha256': file_enc_sha256, 'fileSha256': file_sha256, 'fileLength': file_length}
    except Exception as error:
        enc_f.close()
        if original_f:
            original_f.close()
        try:
            os.unlink(enc_file_path)
            if original_file_path:
                os.unlink(original_file_path)
        except OSError as err:
            if logger:
                logger.error({'err': err}, 'failed deleting tmp files')
        raise error


DEF_MEDIA_HOST = 'mmg.whatsapp.net'
AES_CHUNK_SIZE = 16


def _to_smallest_chunk_size(num: int) -> int:
    return (num // AES_CHUNK_SIZE) * AES_CHUNK_SIZE


def get_url_from_direct_path(direct_path: str, host: str = DEF_MEDIA_HOST) -> str:
    return f'https://{host}{direct_path}'


def _extract_host(url) -> Optional[str]:
    if not url:
        return None
    try:
        return urlparse(url).netloc or None
    except Exception:
        return None


async def download_content_from_message(downloadable: dict, type_: str, opts: Optional[dict] = None):
    """Returns an async iterator of decrypted bytes."""
    opts = opts or {}
    media_key = downloadable.get('mediaKey')
    direct_path = downloadable.get('directPath')
    url = downloadable.get('url')
    fallback_host = opts.get('host') or _extract_host(url) or DEF_MEDIA_HOST
    download_url = get_url_from_direct_path(direct_path, fallback_host) if direct_path else url
    if not download_url:
        raise Boom('No valid media URL or directPath present in message', status_code=400)
    keys = await get_media_keys(media_key, type_)
    return await download_encrypted_content(download_url, keys, opts)


async def download_encrypted_content(download_url: str, keys: dict, opts: Optional[dict] = None):
    """Decrypts and downloads an AES256-CBC encrypted file given the keys.
    Assumes the SHA256 of the plaintext is appended to the end of the ciphertext"""
    opts = opts or {}
    start_byte, end_byte = opts.get('startByte'), opts.get('endByte')
    options = opts.get('options') or {}
    cipher_key, iv = keys['cipherKey'], keys['iv']
    bytes_fetched = 0
    start_chunk = 0
    first_block_is_iv = False
    if start_byte:
        chunk = _to_smallest_chunk_size(start_byte or 0)
        if chunk:
            start_chunk = chunk - AES_CHUNK_SIZE
            bytes_fetched = chunk
            first_block_is_iv = True
    end_chunk = _to_smallest_chunk_size(end_byte or 0) + AES_CHUNK_SIZE if end_byte else None
    headers = dict(options.get('headers') or {})
    headers['Origin'] = DEFAULT_ORIGIN
    if start_chunk or end_chunk:
        headers['Range'] = f'bytes={start_chunk}-' + (str(end_chunk) if end_chunk else '')
    fetched = await get_http_stream(download_url, {**options, 'headers': headers})

    async def gen():
        nonlocal bytes_fetched
        remaining = b''
        aes = None
        unpadder = None

        def push_bytes(b: bytes):
            nonlocal bytes_fetched
            if start_byte or end_byte:
                start = None if bytes_fetched >= (start_byte or 0) else max((start_byte or 0) - bytes_fetched, 0)
                end = None if (end_byte is None or bytes_fetched + len(b) < end_byte) else max(end_byte - bytes_fetched, 0)
                out = b[start:end]
                bytes_fetched += len(b)
                return out
            return b

        async for chunk in fetched:
            data = remaining + chunk if remaining else chunk
            dl = _to_smallest_chunk_size(len(data))
            remaining = data[dl:]
            data = data[:dl]
            if aes is None:
                iv_value = iv
                if first_block_is_iv:
                    iv_value = data[:AES_CHUNK_SIZE]
                    data = data[AES_CHUNK_SIZE:]
                aes = Cipher(algorithms.AES(cipher_key), modes.CBC(iv_value)).decryptor()
                if not end_byte:
                    unpadder = padding.PKCS7(128).unpadder()
            dec = aes.update(data)
            if unpadder is not None:
                dec = unpadder.update(dec)
            out = push_bytes(dec)
            if out:
                yield out
        if aes is not None:
            tail = aes.finalize()
            if unpadder is not None:
                tail = unpadder.update(tail) + unpadder.finalize()
            out = push_bytes(tail)
            if out:
                yield out
    return gen()


def extension_for_media_message(message: dict) -> str:
    def get_extension(mimetype: str):
        parts = mimetype.split(';')[0].split('/')
        return parts[1] if len(parts) > 1 else None
    type_ = next(iter(message))
    if type_ in ('locationMessage', 'liveLocationMessage', 'productMessage'):
        return '.jpeg'
    return get_extension(message[type_]['mimetype'])


async def upload_media(url: str, file_path: str, headers: dict, timeout_ms: Optional[float] = None,
                       options: Optional[dict] = None, logger=None) -> Optional[dict]:
    """POSTs the (already encrypted) file to the WA media server, following redirects."""
    size = os.path.getsize(file_path)

    async def body():
        with open(file_path, 'rb') as f:
            while True:
                c = f.read(65536)
                if not c:
                    break
                yield c
    async with httpx.AsyncClient(follow_redirects=True, timeout=(timeout_ms / 1000 if timeout_ms else None),
                                 **(options or {})) as client:
        r = await client.post(url, content=body(), headers={**headers, 'Content-Length': str(size)})
    try:
        return r.json()
    except Exception:
        return None


def get_wa_upload_to_server(config: dict, refresh_media_conn):
    """Builds the `upload(filePath, {mediaType, fileEncSha256B64, timeoutMs})` function."""
    custom_upload_hosts = config.get('customUploadHosts') or []
    logger = config['logger']
    options = config.get('options') or {}

    async def upload(file_path: str, mediaType: str, fileEncSha256B64: str, timeoutMs=None, **_):
        media_type, file_enc_sha256_b64, timeout_ms = mediaType, fileEncSha256B64, timeoutMs
        upload_info = await refresh_media_conn(False)
        urls = None
        hosts = [*custom_upload_hosts, *upload_info['hosts']]
        file_enc_sha256_b64 = encode_base64_encoded_string_for_upload(file_enc_sha256_b64)
        headers = {**(options.get('headers') or {}), 'Content-Type': 'application/octet-stream',
                   'Origin': DEFAULT_ORIGIN}
        for host in hosts:
            hostname = host['hostname']
            logger.debug(f'uploading to "{hostname}"')
            auth = quote(upload_info['auth'], safe="-_.!~*'()")
            url = f"https://{hostname}{MEDIA_PATH_MAP[media_type]}/{file_enc_sha256_b64}" \
                  f"?auth={auth}&token={file_enc_sha256_b64}"
            result = None
            try:
                result = await upload_media(url, file_path, headers, timeout_ms, {k: v for k, v in options.items()
                                                                                  if k not in ('headers',)}, logger)
                if result and (result.get('url') or result.get('direct_path')):
                    urls = {'mediaUrl': result.get('url'), 'directPath': result.get('direct_path'),
                            'meta_hmac': result.get('meta_hmac'), 'fbid': result.get('fbid'), 'ts': result.get('ts')}
                    break
                upload_info = await refresh_media_conn(True)
                raise RuntimeError(f'upload failed, reason: {json.dumps(result)}')
            except Exception as error:
                is_last = hostname == (hosts[len(upload_info['hosts'])]['hostname']
                                       if len(upload_info['hosts']) < len(hosts) else None)
                logger.warn({'error': str(error), 'uploadResult': result},
                            f"Error in uploading to {hostname} {'' if is_last else ', retrying...'}")
        if not urls:
            raise Boom('Media upload failed on all hosts', status_code=500)
        return urls
    return upload


def _get_media_retry_key(media_key: bytes) -> bytes:
    return hkdf(bytes(media_key), 32, info='WhatsApp Media Retry Notification')


def encrypt_media_retry_request(key: dict, media_key: bytes, me_id: str) -> dict:
    """Generate a binary node that will request the phone to re-upload the media & return the newly uploaded URL"""
    recp_buffer = from_object(proto.ServerErrorReceipt, {'stanzaId': key.get('id')}).SerializeToString()
    iv = os.urandom(12)
    retry_key = _get_media_retry_key(media_key)
    ciphertext = aes_encrypt_gcm(recp_buffer, retry_key, iv, key['id'].encode())
    return {
        'tag': 'receipt',
        'attrs': {'id': key['id'], 'to': jid_normalized_user(me_id), 'type': 'server-error'},
        'content': [
            {'tag': 'encrypt', 'attrs': {}, 'content': [
                {'tag': 'enc_p', 'attrs': {}, 'content': ciphertext},
                {'tag': 'enc_iv', 'attrs': {}, 'content': iv}]},
            {'tag': 'rmr', 'attrs': {'jid': key.get('remoteJid'),
                                     'from_me': str(bool(key.get('fromMe'))).lower(),
                                     'participant': key.get('participant') or None}},
        ],
    }


_MEDIA_RETRY_STATUS_MAP = {
    proto.MediaRetryNotification.SUCCESS: 200,
    proto.MediaRetryNotification.DECRYPTION_ERROR: 412,
    proto.MediaRetryNotification.NOT_FOUND: 404,
    proto.MediaRetryNotification.GENERAL_ERROR: 418,
}


def get_status_code_for_media_retry(code: int) -> Optional[int]:
    return _MEDIA_RETRY_STATUS_MAP.get(code)


def decode_media_retry_node(node: dict) -> dict:
    rmr_node = get_binary_node_child(node, 'rmr')
    event = {'key': {'id': node['attrs'].get('id'), 'remoteJid': rmr_node['attrs'].get('jid'),
                     'fromMe': rmr_node['attrs'].get('from_me') == 'true',
                     'participant': rmr_node['attrs'].get('participant')}}
    error_node = get_binary_node_child(node, 'error')
    if error_node:
        code = int(error_node['attrs'].get('code'))
        event['error'] = Boom(f'Failed to re-upload media ({code})', data=error_node['attrs'],
                              status_code=get_status_code_for_media_retry(code))
    else:
        enc = get_binary_node_child(node, 'encrypt')
        ciphertext = get_binary_node_child_buffer(enc, 'enc_p')
        iv = get_binary_node_child_buffer(enc, 'enc_iv')
        if ciphertext and iv:
            event['media'] = {'ciphertext': ciphertext, 'iv': iv}
        else:
            event['error'] = Boom('Failed to re-upload media (missing ciphertext)', status_code=404)
    return event


def decrypt_media_retry_data(data: dict, media_key: bytes, msg_id: str):
    retry_key = _get_media_retry_key(media_key)
    plaintext = aes_decrypt_gcm(data['ciphertext'], retry_key, data['iv'], msg_id.encode())
    m = proto.MediaRetryNotification()
    m.ParseFromString(plaintext)
    return m
