from __future__ import annotations

from ._tonio import ScheduledIO as _ScheduledIO, Waiter


class ScheduledIO(_ScheduledIO):
    __slots__ = []

    def arm_r(self, timeout: int | float | None = None) -> Waiter | None:
        timeout = round(max(0, timeout * 1_000_000)) if timeout is not None else timeout
        return self._arm_r(timeout)

    def arm_w(self, timeout: int | float | None = None) -> Waiter | None:
        timeout = round(max(0, timeout * 1_000_000)) if timeout is not None else timeout
        return self._arm_w(timeout)


def register(fd: int) -> ScheduledIO:
    return ScheduledIO(fd)
