from __future__ import annotations
import re
from html.parser import HTMLParser
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

THUMBNAIL_WIDTH_PX = 192


class _MetaParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.meta: dict = {}
        self.title = ''
        self._in_title = False
        self.images: list = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'meta':
            key = a.get('property') or a.get('name')
            if key and a.get('content') is not None:
                self.meta.setdefault(key.lower(), a['content'])
        elif tag == 'title':
            self._in_title = True
        elif tag == 'img' and a.get('src'):
            self.images.append(a['src'])

    def handle_endtag(self, tag):
        if tag == 'title':
            self._in_title = False

    def handle_data(self, data):
        if self._in_title:
            self.title += data


async def get_compressed_jpeg_thumbnail(url: str, thumbnail_width: int, fetch_opts: dict):
    from .messages_media import extract_image_thumb, get_http_stream
    stream = await get_http_stream(url, fetch_opts)
    return await extract_image_thumb(stream, thumbnail_width)


async def get_url_info(text: str, opts: Optional[dict] = None) -> Optional[dict]:
    """Given a piece of text, checks for any URL present, generates link preview for the same and returns it.
    Returns None if the fetch failed or no URL was found."""
    opts = opts or {'thumbnailWidth': THUMBNAIL_WIDTH_PX, 'fetchOpts': {'timeout': 3000}}
    fetch_opts = opts.get('fetchOpts') or {}
    try:
        preview_link = text
        if not text.startswith('https://') and not text.startswith('http://'):
            preview_link = 'https://' + preview_link
        headers = {'user-agent': 'Mozilla/5.0 (compatible; baileys-link-preview)', **(fetch_opts.get('headers') or {})}
        async with httpx.AsyncClient(follow_redirects=True, max_redirects=5,
                                     timeout=(fetch_opts.get('timeout', 3000) / 1000),
                                     proxy=fetch_opts.get('proxyUrl')) as c:
            r = await c.get(preview_link, headers=headers)
        if 'html' not in r.headers.get('content-type', 'html'):
            return None
        p = _MetaParser()
        p.feed(r.text)
        title = p.meta.get('og:title') or p.meta.get('twitter:title') or p.title.strip()
        if not title:
            return None
        description = p.meta.get('og:description') or p.meta.get('twitter:description') or p.meta.get('description')
        image = p.meta.get('og:image') or p.meta.get('twitter:image') or (p.images[0] if p.images else None)
        if image:
            image = urljoin(str(r.url), image)
        url_info = {'canonical-url': p.meta.get('og:url') or str(r.url), 'matched-text': text, 'title': title,
                    'description': description, 'originalThumbnailUrl': image}
        if opts.get('uploadImage'):
            from .messages import prepare_wa_message_media
            res = await prepare_wa_message_media({'image': {'url': image}}, {
                'upload': opts['uploadImage'], 'mediaTypeOverride': 'thumbnail-link', 'options': fetch_opts})
            image_message = res.get('imageMessage')
            url_info['jpegThumbnail'] = bytes(image_message['jpegThumbnail']) \
                if image_message and image_message.get('jpegThumbnail') else None
            url_info['highQualityThumbnail'] = image_message
        else:
            try:
                if image:
                    url_info['jpegThumbnail'] = (await get_compressed_jpeg_thumbnail(
                        image, opts.get('thumbnailWidth', THUMBNAIL_WIDTH_PX), fetch_opts))['buffer']
            except Exception as err:
                if opts.get('logger'):
                    opts['logger'].debug({'err': str(err), 'url': preview_link}, 'error in generating thumbnail')
        return url_info
    except httpx.HTTPError:
        return None
