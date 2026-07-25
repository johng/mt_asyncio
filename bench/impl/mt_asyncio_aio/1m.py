import argparse
import json
import time

import mt_asyncio.asyncio as aio


async def _task():
    await aio.sleep(0)
    return 3**2


async def _run():
    t0 = time.monotonic()
    tasks = [_task() for _ in range(1_000_000)]
    t1 = time.monotonic()
    await aio.gather(*tasks)
    t2 = time.monotonic()
    return (t1 - t0, t2 - t1, t2 - t0)


def main(threads):
    res = []
    loop = aio.new_event_loop(threads=threads)
    for _ in range(5):
        res.append(loop.run_until_complete(_run()))
    print(json.dumps(res))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=1, type=int, help='no of threads')
    main(**dict(parser.parse_args()._get_kwargs()))
