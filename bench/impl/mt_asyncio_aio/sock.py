import argparse
import socket

import mt_asyncio.asyncio as aio


async def echo_server(address):
    loop = aio.get_running_loop()
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen()
    sock.setblocking(False)

    with sock:
        while True:
            client, _ = await loop.sock_accept(sock)
            loop.create_task(echo_client(loop, client))


async def echo_client(loop, conn):
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, NameError):
        pass

    with conn:
        while True:
            data = await loop.sock_recv(conn, 102400)
            if not data:
                break
            await loop.sock_sendall(conn, data)


def main(addr, threads):
    addr = addr.split(':')
    addr[1] = int(addr[1])
    addr = tuple(addr)

    aio.run(echo_server(addr), threads=threads)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', default='127.0.0.1:25000', type=str)
    parser.add_argument('--threads', default=1, type=int, help='no of threads')
    main(**dict(parser.parse_args()._get_kwargs()))
