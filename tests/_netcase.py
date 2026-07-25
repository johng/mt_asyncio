"""One third-party networking scenario, run against one backend, in one process.

Invoked as ``python -m tests._netcase <backend> <case>``; prints a JSON result on
stdout. ``tests/test_netlibs.py`` runs each case under both backends and asserts
the results match.

A subprocess per arm is not fastidiousness, it is required. ``compat.install()``
is process-global, and libraries bind names at import time (psycopg's
``waiting.py`` opens with ``from asyncio import Event, get_event_loop, wait_for``),
so the two backends cannot coexist: whichever installs first decides what every
subsequently imported library sees.
"""

from __future__ import annotations

import json
import os
import socket
import ssl
import sys


CASES = {}


def case(fn):
    CASES[fn.__name__] = fn
    return fn


def _select_backend(name):
    """Return the asyncio-ish module to drive, before any library is imported."""
    if name == 'stdlib':
        import asyncio

        return asyncio
    if name == 'mt_asyncio':
        import mt_asyncio

        mt_asyncio.runtime(threads=4, blocking_threadpool_size=8, context=True)
        import mt_asyncio.asyncio as aio

        # must precede every library import below
        aio.compat.install()
        return aio
    raise SystemExit(f'unknown backend {name!r}')


def _tls_contexts():
    import trustme

    ca = trustme.CA()
    cert = ca.issue_cert('localhost')
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


# --------------------------------------------------------------------------
# aiohttp
# --------------------------------------------------------------------------


def _aiohttp_app():
    from aiohttp import web

    async def hello(request):
        return web.Response(text='hello ' + request.match_info['name'])

    async def echo(request):
        body = await request.read()
        return web.Response(body=body[::-1], content_type='application/octet-stream')

    async def chunked(request):
        response = web.StreamResponse()
        await response.prepare(request)
        for i in range(50):
            await response.write(f'chunk-{i};'.encode())
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get('/hello/{name}', hello)
    app.router.add_post('/echo', echo)
    app.router.add_get('/chunked', chunked)
    return app


@case
def aiohttp_http(aio):
    """aiohttp client and server over our transports.

    Note what this deliberately does *not* do: ``runner.cleanup()``. aiohttp's
    server shutdown iterates ``Server._connections`` while a task done-callback
    pops from it, which races under parallel stepping and is not fixable from
    our side (see "Shared state across tasks" in COMPATIBILITY.md). Serving
    traffic is what is under test here, and that part is reliable.
    """
    import aiohttp
    from aiohttp import web

    async def main():
        runner = web.AppRunner(_aiohttp_app())
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = list(runner.addresses)[0][1]

        out = {}
        async with aiohttp.ClientSession(base_url=f'http://127.0.0.1:{port}') as session:
            async with session.get('/hello/world') as resp:
                out['get_status'] = resp.status
                out['get_body'] = await resp.text()

            payload = bytes(range(256)) * 64
            async with session.post('/echo', data=payload) as resp:
                out['post_ok'] = (await resp.read()) == payload[::-1]

            async with session.get('/chunked') as resp:
                out['chunked'] = await resp.text()

            # concurrency: many requests in flight over a shared session
            async def one(i):
                async with session.get(f'/hello/{i}') as r:
                    return await r.text()

            results = await aio.gather(*[one(i) for i in range(50)])
            out['concurrent_ok'] = results == [f'hello {i}' for i in range(50)]

        return out

    return aio.run(main())


@case
def aiohttp_tls(aio):
    import aiohttp
    from aiohttp import web

    server_ctx, client_ctx = _tls_contexts()

    async def main():
        runner = web.AppRunner(_aiohttp_app())
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0, ssl_context=server_ctx)
        await site.start()
        port = list(runner.addresses)[0][1]

        out = {}
        connector = aiohttp.TCPConnector(ssl=client_ctx)
        async with aiohttp.ClientSession(base_url=f'https://localhost:{port}', connector=connector) as session:
            async with session.get('/hello/tls') as resp:
                out['status'] = resp.status
                out['body'] = await resp.text()

            payload = b'secret' * 1000
            async with session.post('/echo', data=payload) as resp:
                out['echo_ok'] = (await resp.read()) == payload[::-1]

        return out

    return aio.run(main())


# --------------------------------------------------------------------------
# websockets
# --------------------------------------------------------------------------


@case
def websockets_echo(aio):
    import websockets
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    async def handler(ws):
        async for message in ws:
            await ws.send(message.upper() if isinstance(message, str) else message[::-1])

    async def main():
        out = {}
        # entered without `async with`: __aexit__ runs Server._close, which
        # iterates self.handlers while the connection handler tasks delete
        # themselves from it. That races under parallel stepping and is not
        # fixable from our side -- see "Shared state across tasks" in
        # COMPATIBILITY.md. Framing and concurrency are what is under test.
        server = await serve(handler, '127.0.0.1', 0).__aenter__()
        port = server.sockets[0].getsockname()[1]

        async with connect(f'ws://127.0.0.1:{port}') as ws:
            await ws.send('hello')
            out['text'] = await ws.recv()
            await ws.send(b'\x00\x01\x02')
            out['binary'] = list(await ws.recv())
            out['pong'] = bool(await (await ws.ping()))

        # many sockets at once, each with its own frame sequence
        async def client(i):
            async with connect(f'ws://127.0.0.1:{port}') as ws:
                msgs = []
                for j in range(10):
                    await ws.send(f'c{i}-m{j}')
                    msgs.append(await ws.recv())
                return msgs

        results = await aio.gather(*[client(i) for i in range(20)])
        out['concurrent_ok'] = results == [[f'C{i}-M{j}' for j in range(10)] for i in range(20)]

        out['version'] = websockets.__version__
        return out

    return aio.run(main())


# --------------------------------------------------------------------------
# psycopg
# --------------------------------------------------------------------------


@case
def psycopg_wait_pattern(aio):
    """psycopg's ``wait_async`` loop, driven against a socketpair.

    This is the exact algorithm from psycopg/waiting.py -- add_reader/add_writer,
    await an Event, remove them in a finally -- run against a real generator, so
    it exercises our fd callbacks the way libpq does without needing a server.
    """
    from psycopg import waiting
    from psycopg._enums import Ready, Wait

    a, b = socket.socketpair()
    a.setblocking(False)

    def protocol():
        """Yield WAIT_R until three messages have arrived, like a query would."""
        got = []
        while len(got) < 3:
            ready = yield Wait.R
            if ready & Ready.R:
                got.append(a.recv(1024))
        return [chunk.decode() for chunk in got]

    async def main():
        async def feed():
            for i in range(3):
                await aio.sleep(0.01)
                b.sendall(f'msg{i}'.encode())

        feeder = aio.create_task(feed())
        result = await waiting.wait_async(protocol(), a.fileno(), interval=0.5)
        await feeder
        return {'received': result}

    try:
        return aio.run(main())
    finally:
        a.close()
        b.close()


@case
def psycopg_db(aio):
    """Real Postgres, if a DSN was provided."""
    import psycopg

    dsn = os.environ['MT_ASYNCIO_TEST_DSN']

    async def main():
        out = {}
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute('select 1 + 1')
                out['simple'] = (await cur.fetchone())[0]
                await cur.execute('select generate_series(1, 100)')
                out['rows'] = sum(r[0] for r in await cur.fetchall())

        async def query(i):
            async with await psycopg.AsyncConnection.connect(dsn) as conn:
                async with conn.cursor() as cur:
                    await cur.execute('select %s::int * 2', (i,))
                    return (await cur.fetchone())[0]

        out['concurrent'] = await aio.gather(*[query(i) for i in range(20)])
        return out

    return aio.run(main())


# --------------------------------------------------------------------------
# asyncio streams through the compat patch
# --------------------------------------------------------------------------


@case
def stdlib_streams_via_compat(aio):
    """A library using bare ``asyncio.open_connection`` must get our reader."""
    import asyncio as patched

    async def main():
        async def handle(reader, writer):
            data = await reader.readexactly(9)
            writer.write(data.upper())
            await writer.drain()
            writer.close()

        server = await patched.start_server(handle, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]

        async def client(i):
            reader, writer = await patched.open_connection('127.0.0.1', port)
            writer.write(f'req-{i:05d}'.encode())  # 9 bytes, matching readexactly
            await writer.drain()
            got = await reader.readexactly(9)
            writer.close()
            return got.decode()

        results = await patched.gather(*[client(i) for i in range(25)])
        server.close()
        await server.wait_closed()
        return {'results': results}

    return aio.run(main())


def main(argv):
    backend, case_name = argv[1], argv[2]
    aio = _select_backend(backend)
    result = CASES[case_name](aio)
    sys.stdout.write(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
