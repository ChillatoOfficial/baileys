"""Sticker helpers: WhatsApp only renders stickers as WebP (512x512). Anything else uploads fine but never loads."""
from __future__ import annotations
import asyncio
import io
import os
import struct
import tempfile
from typing import Optional

SIZE = 512


def is_webp(data: bytes) -> bool:
    return data[:4] == b'RIFF' and data[8:12] == b'WEBP'


def _exif(pack: str, author: str) -> bytes:
    import json
    meta = json.dumps({'sticker-pack-id': 'baileyspy', 'sticker-pack-name': pack, 'sticker-pack-publisher': author,
                       'emojis': ['']}).encode()
    head = b'II*\x00\x08\x00\x00\x00\x01\x00AW\x07\x00' + struct.pack('<I', len(meta)) + b'\x16\x00\x00\x00'
    return head + meta


def image_to_webp(data: bytes, pack: Optional[str] = None, author: Optional[str] = None, quality: int = 80) -> bytes:
    """Static (or animated GIF/WebP) image -> 512x512 WebP with transparent padding (aspect ratio kept)."""
    from PIL import Image, ImageSequence
    img = Image.open(io.BytesIO(data))
    animated = getattr(img, 'is_animated', False)

    def fit(frame):
        frame = frame.convert('RGBA')
        frame.thumbnail((SIZE, SIZE), Image.LANCZOS)
        canvas = Image.new('RGBA', (SIZE, SIZE), (0, 0, 0, 0))
        canvas.paste(frame, ((SIZE - frame.width) // 2, (SIZE - frame.height) // 2), frame)
        return canvas

    kw = {'format': 'WEBP', 'quality': quality, 'method': 4}
    if pack or author:
        kw['exif'] = _exif(pack or '', author or '')
    out = io.BytesIO()
    if animated:
        frames = [fit(f) for f in ImageSequence.Iterator(img)]
        frames[0].save(out, save_all=True, append_images=frames[1:], loop=0,
                       duration=img.info.get('duration', 100), **kw)
    else:
        fit(img).save(out, **kw)
    return out.getvalue()


async def video_to_webp(data: bytes, seconds: int = 8) -> bytes:
    """Video/GIF -> animated WebP via ffmpeg (must be installed)."""
    with tempfile.TemporaryDirectory() as d:
        src, dst = os.path.join(d, 'in'), os.path.join(d, 'out.webp')
        with open(src, 'wb') as f:
            f.write(data)
        vf = (f'fps=15,scale={SIZE}:{SIZE}:force_original_aspect_ratio=decrease,'
              f'pad={SIZE}:{SIZE}:(ow-iw)/2:(oh-ih)/2:color=0x00000000')
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg', '-y', '-i', src, '-t', str(seconds), '-vf', vf, '-loop', '0', '-an', '-vcodec', 'libwebp',
            '-quality', '70', dst, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, err = await proc.communicate()
        if proc.returncode:
            raise RuntimeError(f'ffmpeg failed: {err.decode(errors="replace")[-300:]}')
        with open(dst, 'rb') as f:
            return f.read()


async def to_sticker(data: bytes, pack: Optional[str] = None, author: Optional[str] = None) -> bytes:
    """Convert image/gif/video bytes to a WhatsApp-ready WebP sticker (already-512 WebP passes through)."""
    if is_webp(data) and not (pack or author):
        return data
    try:
        return await asyncio.get_running_loop().run_in_executor(None, image_to_webp, data, pack, author)
    except Exception:
        return await video_to_webp(data)
