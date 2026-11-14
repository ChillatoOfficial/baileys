from __future__ import annotations
import asyncio
import base64
import hashlib
import os
import tempfile
from typing import Optional

from ..WABinary import Boom, get_binary_node_child, get_binary_node_child_string, get_binary_node_children
from .generics import generate_message_id_v2
from .messages_media import get_stream, get_url_from_direct_path


def _s(node, tag):
    return get_binary_node_child_string(node, tag)


def _f(v):
    return float(v) if v is not None else float('nan')


def parse_catalog_node(node: dict) -> dict:
    catalog_node = get_binary_node_child(node, 'product_catalog')
    products = [parse_product_node(p) for p in get_binary_node_children(catalog_node, 'product')]
    paging = get_binary_node_child(catalog_node, 'paging')
    return {'products': products, 'nextPageCursor': _s(paging, 'after') if paging else None}


def parse_collections_node(node: dict) -> dict:
    collections_node = get_binary_node_child(node, 'collections')
    collections = []
    for c in get_binary_node_children(collections_node, 'collection'):
        collections.append({'id': _s(c, 'id'), 'name': _s(c, 'name'),
                            'products': [parse_product_node(p) for p in get_binary_node_children(c, 'product')],
                            'status': _parse_status_info(c)})
    return {'collections': collections}


def parse_order_details_node(node: dict) -> dict:
    order_node = get_binary_node_child(node, 'order')
    products = []
    for p in get_binary_node_children(order_node, 'product'):
        image_node = get_binary_node_child(p, 'image')
        products.append({'id': _s(p, 'id'), 'name': _s(p, 'name'), 'imageUrl': _s(image_node, 'url'),
                         'price': _f(_s(p, 'price')), 'currency': _s(p, 'currency'),
                         'quantity': _f(_s(p, 'quantity'))})
    price_node = get_binary_node_child(order_node, 'price')
    return {'price': {'total': _f(_s(price_node, 'total')), 'currency': _s(price_node, 'currency')},
            'products': products}


def to_product_node(product_id: Optional[str], product: dict) -> dict:
    attrs, content = {}, []
    enc = lambda v: str(v).encode()
    if product_id is not None:
        content.append({'tag': 'id', 'attrs': {}, 'content': enc(product_id)})
    if product.get('name') is not None:
        content.append({'tag': 'name', 'attrs': {}, 'content': enc(product['name'])})
    if product.get('description') is not None:
        content.append({'tag': 'description', 'attrs': {}, 'content': enc(product['description'])})
    if product.get('retailerId') is not None:
        content.append({'tag': 'retailer_id', 'attrs': {}, 'content': enc(product['retailerId'])})
    if product.get('images'):
        images = []
        for img in product['images']:
            if 'url' not in img:
                raise Boom('Expected img for product to already be uploaded', status_code=400)
            images.append({'tag': 'image', 'attrs': {}, 'content': [
                {'tag': 'url', 'attrs': {}, 'content': enc(img['url'])}]})
        content.append({'tag': 'media', 'attrs': {}, 'content': images})
    if product.get('price') is not None:
        content.append({'tag': 'price', 'attrs': {}, 'content': enc(product['price'])})
    if product.get('currency') is not None:
        content.append({'tag': 'currency', 'attrs': {}, 'content': enc(product['currency'])})
    if 'originCountryCode' in product:
        if product['originCountryCode'] is None:
            attrs['compliance_category'] = 'COUNTRY_ORIGIN_EXEMPT'
        else:
            content.append({'tag': 'compliance_info', 'attrs': {}, 'content': [
                {'tag': 'country_code_origin', 'attrs': {}, 'content': enc(product['originCountryCode'])}]})
    if product.get('isHidden') is not None:
        attrs['is_hidden'] = str(bool(product['isHidden'])).lower()
    return {'tag': 'product', 'attrs': attrs, 'content': content}


def parse_product_node(product_node: dict) -> dict:
    is_hidden = product_node['attrs'].get('is_hidden') == 'true'
    media_node = get_binary_node_child(product_node, 'media')
    status_info_node = get_binary_node_child(product_node, 'status_info')
    return {
        'id': _s(product_node, 'id'),
        'imageUrls': _parse_image_urls(media_node),
        'reviewStatus': {'whatsapp': _s(status_info_node, 'status')},
        'availability': 'in stock',
        'name': _s(product_node, 'name'),
        'retailerId': _s(product_node, 'retailer_id'),
        'url': _s(product_node, 'url'),
        'description': _s(product_node, 'description'),
        'price': _f(_s(product_node, 'price')),
        'currency': _s(product_node, 'currency'),
        'isHidden': is_hidden,
    }


async def uploading_necessary_images_of_product(product: dict, wa_upload_to_server, timeout_ms: int = 30_000) -> dict:
    """Uploads images not already uploaded to WA's servers"""
    product = dict(product)
    if product.get('images'):
        product['images'] = await uploading_necessary_images(product['images'], wa_upload_to_server, timeout_ms)
    return product


async def uploading_necessary_images(images: list, wa_upload_to_server, timeout_ms: int = 30_000) -> list:
    async def one(img):
        if isinstance(img, dict) and 'url' in img:
            url = str(img['url'])
            if '.whatsapp.net' in url:
                return {'url': url}
        stream, _ = await get_stream(img)
        hasher = hashlib.sha256()
        file_path = os.path.join(tempfile.gettempdir(), 'img' + generate_message_id_v2())
        with open(file_path, 'wb') as f:
            async for block in stream:
                hasher.update(block)
                f.write(block)
        sha = base64.b64encode(hasher.digest()).decode()
        res = await wa_upload_to_server(file_path, mediaType='product-catalog-image', fileEncSha256B64=sha,
                                        timeoutMs=timeout_ms)
        try:
            os.unlink(file_path)
        except OSError as err:
            print('Error deleting temp file ', err)
        return {'url': get_url_from_direct_path(res['directPath'])}
    return list(await asyncio.gather(*[one(i) for i in images]))


def _parse_image_urls(media_node) -> dict:
    img_node = get_binary_node_child(media_node, 'image')
    return {'requested': _s(img_node, 'request_image_url'), 'original': _s(img_node, 'original_image_url')}


def _parse_status_info(media_node) -> dict:
    node = get_binary_node_child(media_node, 'status_info')
    return {'status': _s(node, 'status'), 'canAppeal': _s(node, 'can_appeal') == 'true'}
