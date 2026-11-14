"""Command registry: options, permission checks and cooldowns."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .filters import Filter

DEFAULT_DENIED = {
    'admin_only': 'Only group admins can use this command.',
    'bot_admin': 'I need to be a group admin to do that.',
    'group_only': 'This command only works in groups.',
    'private_only': 'This command only works in private chats.',
    'owner_only': 'Only the bot owner can use this command.',
    'cooldown': 'Slow down, try again in {seconds}s.',
}


@dataclass
class Command:
    name: str
    fn: Callable[..., Any]
    aliases: tuple = ()
    help: Optional[str] = None
    skip_me: bool = True
    admin_only: bool = False
    bot_admin: bool = False
    group_only: bool = False
    private_only: bool = False
    owner_only: bool = False
    cooldown: float = 0.0
    hidden: bool = False
    filter: Optional[Filter] = None
    n_params: int = 1
    _last: dict = field(default_factory=dict, repr=False)

    def check_cooldown(self, who: str) -> float:
        """0 if allowed (and starts the cooldown), else remaining seconds."""
        if not self.cooldown:
            return 0.0
        now = time.monotonic()
        until = self._last.get(who, 0.0)
        if until > now:
            return until - now
        self._last[who] = now + self.cooldown
        if len(self._last) > 2000:
            self._last = {k: v for k, v in self._last.items() if v > now}
        return 0.0
