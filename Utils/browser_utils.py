from __future__ import annotations
import platform as _platform
from enum import IntEnum
from ..WAProto import WAProto_pb2 as proto

_PLATFORM_MAP = {'aix': 'AIX', 'darwin': 'Mac OS', 'windows': 'Windows', 'freebsd': 'FreeBSD',
                 'openbsd': 'OpenBSD', 'sunos': 'Solaris'}


class Browsers:
    @staticmethod
    def ubuntu(browser): return ['Ubuntu', browser, '22.04.4']
    @staticmethod
    def macOS(browser): return ['Mac OS', browser, '14.4.1']
    @staticmethod
    def baileys(browser): return ['Baileys', browser, '6.5.0']
    @staticmethod
    def windows(browser): return ['Windows', browser, '10.0.22631']
    @staticmethod
    def android(browser): return [browser, 'Android', '']
    @staticmethod
    def appropriate(browser):
        return [_PLATFORM_MAP.get(_platform.system().lower()) or 'Ubuntu', browser, _platform.release()]


def get_platform_id(browser: str) -> str:
    try:
        v = proto.DeviceProps.PlatformType.Value(browser.upper())
    except ValueError:
        v = 0
    return str(v) if v else '1'


class CompanionWebClientType(IntEnum):
    UNKNOWN = 0
    CHROME = 1
    EDGE = 2
    FIREFOX = 3
    IE = 4
    OPERA = 5
    SAFARI = 6
    ELECTRON = 7
    UWP = 8
    OTHER_WEB_CLIENT = 9


_BROWSER_TO_COMPANION = {'Chrome': 1, 'Edge': 2, 'Firefox': 3, 'IE': 4, 'Opera': 5, 'Safari': 6}
