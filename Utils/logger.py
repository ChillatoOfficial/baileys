"""pino-like logger: logger.info(obj, msg) / logger.info(msg)."""
from __future__ import annotations
import logging
import sys

TRACE = 5
logging.addLevelName(TRACE, 'TRACE')

_LEVELS = {'trace': TRACE, 'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARNING,
           'warning': logging.WARNING, 'error': logging.ERROR, 'fatal': logging.CRITICAL, 'silent': 100}


def _compact(o, depth=0):
    """Replace raw bytes by a short placeholder and cut very long strings, so logs stay readable."""
    if isinstance(o, (bytes, bytearray, memoryview)):
        return f'<bytes {len(o)}>'
    if isinstance(o, str):
        return o if len(o) <= 200 else f'{o[:200]}...<{len(o)} chars>'
    if depth > 8:
        return o
    if isinstance(o, dict):
        return {k: _compact(v, depth + 1) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return type(o)(_compact(v, depth + 1) for v in o[:50])
    return o


class ILogger:
    def __init__(self, name: str = 'baileys', level: str = 'info', bindings: dict | None = None):
        self._logger = logging.getLogger(name)
        if not self._logger.handlers and not logging.getLogger().handlers:
            h = logging.StreamHandler(sys.stderr)
            h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
            self._logger.addHandler(h)
        self._bindings = bindings or {}
        self.level = level
        self._logger.setLevel(_LEVELS.get(level, logging.INFO))

    def child(self, obj: dict | None = None) -> 'ILogger':
        b = dict(self._bindings)
        b.update(obj or {})
        c = ILogger(self._logger.name, self.level, b)
        return c

    def _log(self, lvl: int, obj, msg=None):
        if lvl < _LEVELS.get(self.level, logging.INFO):
            return
        if isinstance(obj, str) and msg is None:
            text = obj
        else:
            text = f'{msg or ""} {_compact(obj)!r}'.strip()
        if self._bindings:
            text = f'{text} {self._bindings}' if text else str(self._bindings)
        self._logger.log(lvl, text)

    def trace(self, obj=None, msg=None): self._log(TRACE, obj, msg)
    def debug(self, obj=None, msg=None): self._log(logging.DEBUG, obj, msg)
    def info(self, obj=None, msg=None): self._log(logging.INFO, obj, msg)
    def warn(self, obj=None, msg=None): self._log(logging.WARNING, obj, msg)
    warning = warn
    def error(self, obj=None, msg=None): self._log(logging.ERROR, obj, msg)


logger = ILogger('baileys', 'info')
default_logger = logger
