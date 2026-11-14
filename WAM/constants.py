"""WAM event / global definitions (generated from Baileys' WAM/constants.ts into wam_constants.json)."""
import json
import os

with open(os.path.join(os.path.dirname(__file__), 'wam_constants.json'), 'r', encoding='utf-8') as _f:
    _data = json.load(_f)

WEB_EVENTS: list = _data['WEB_EVENTS']
WEB_GLOBALS: list = _data['WEB_GLOBALS']

FLAG_BYTE = 8
FLAG_GLOBAL = 0
FLAG_EVENT = 1
FLAG_FIELD = 2
FLAG_EXTENDED = 4
