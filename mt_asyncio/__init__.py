"""mt_asyncio: a multi-threaded async runtime for free-threaded Python.

The public API is :mod:`mt_asyncio.asyncio` -- an asyncio-compatible event loop that
steps tasks in parallel across the runtime's worker threads::

    import mt_asyncio.asyncio as asyncio

    async def main():
        await asyncio.sleep(1)

    asyncio.run(main())

This module itself only exposes runtime construction, for programs that want to
size the runtime (worker threads, blocking pool, signals) before a loop starts;
otherwise the loop creates and shares one process-wide runtime on demand.
"""

from ._mt_asyncio import __version__ as __version__
from ._runtime import Runtime as Runtime, new as runtime


__all__ = ['Runtime', '__version__', 'runtime']
