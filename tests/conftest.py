import mt_asyncio


#: Both loops share one process-wide runtime, and whichever starts it first
#: fixes its options. Build it explicitly so the suite behaves the same
#: regardless of test order: `context=True` is required by `mt_asyncio.asyncio`
#: (current_task/get_running_loop are contextvars).
_runtime = mt_asyncio.runtime(threads=4, blocking_threadpool_size=8, blocking_threadpool_idle_ttl=10, context=True)
