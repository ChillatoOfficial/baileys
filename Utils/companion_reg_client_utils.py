from __future__ import annotations
from .browser_utils import CompanionWebClientType, _BROWSER_TO_COMPANION


def get_companion_web_client_type(browser) -> CompanionWebClientType:
    os_, browser_name = browser[0], browser[1]
    if browser_name == 'Desktop':
        return CompanionWebClientType.UWP if os_ == 'Windows' else CompanionWebClientType.ELECTRON
    return CompanionWebClientType(_BROWSER_TO_COMPANION.get(browser_name, CompanionWebClientType.OTHER_WEB_CLIENT))


def get_companion_platform_id(browser) -> str:
    return str(int(get_companion_web_client_type(browser)))


def build_pairing_qr_data(ref: str, noise_key_b64: str, identity_key_b64: str, adv_b64: str, browser) -> str:
    return 'https://wa.me/settings/linked_devices#' + ','.join(
        [ref, noise_key_b64, identity_key_b64, adv_b64, get_companion_platform_id(browser)])
