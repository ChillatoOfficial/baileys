from __future__ import annotations
from ..Defaults import DEFAULT_CONNECTION_CONFIG
from .communities import make_communities_socket


def make_wa_socket(config: dict):
    """Creates the full WhatsApp socket (last socket layer). Must be called inside a running asyncio loop."""
    new_config = {**DEFAULT_CONNECTION_CONFIG, **config}
    return make_communities_socket(new_config)


makeWASocket = make_wa_socket
default = make_wa_socket
