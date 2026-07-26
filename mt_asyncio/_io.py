from __future__ import annotations

from typing import Callable

from ._mt_asyncio import ScheduledIO as _ScheduledIO, Waiter


class ScheduledIO(_ScheduledIO):
    __slots__ = ()

    def arm_r(self, timeout: int | float | None = None) -> Waiter | None:
        timeout = round(max(0, timeout * 1_000_000)) if timeout is not None else timeout
        return self._arm_r(timeout)

    def arm_w(self, timeout: int | float | None = None) -> Waiter | None:
        timeout = round(max(0, timeout * 1_000_000)) if timeout is not None else timeout
        return self._arm_w(timeout)

    def arm_r_cb(self, callback: Callable[[], object]) -> bool:
        return self._arm_r_cb(callback)

    def arm_w_cb(self, callback: Callable[[], object]) -> bool:
        return self._arm_w_cb(callback)


def register(fd: int) -> ScheduledIO:
    return ScheduledIO(fd)
