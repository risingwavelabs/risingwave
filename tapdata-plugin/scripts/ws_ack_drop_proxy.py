#!/usr/bin/env python3
"""Forward RisingWave ingest WebSockets but drop the first ACK on each connection."""

import argparse
import asyncio
import json
import ssl

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


async def proxy_connection(client, upstream_base: str, drop_ack: bool) -> None:
    path = client.request.path
    signature = client.request.headers.get("x-rw-signature")
    headers = [("x-rw-signature", signature)] if signature else None

    async with connect(
        upstream_base.rstrip("/") + path,
        additional_headers=headers,
        proxy=None,
        compression=None,
    ) as upstream:
        async def client_to_upstream() -> None:
            async for message in client:
                await upstream.send(message)

        async def upstream_to_client() -> None:
            async for message in upstream:
                try:
                    response = json.loads(message)
                except (TypeError, json.JSONDecodeError):
                    response = None
                if drop_ack and isinstance(response, dict) and "ack" in response:
                    print(f"dropped_ack={response['ack']}", flush=True)
                    await client.close(code=1011, reason="injected ACK loss")
                    return
                await client.send(message)

        tasks = {
            asyncio.create_task(client_to_upstream()),
            asyncio.create_task(upstream_to_client()),
        }
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*done, *pending, return_exceptions=True)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", default=4561, type=int)
    parser.add_argument("--upstream", default="ws://127.0.0.1:4560")
    parser.add_argument("--forward-acks", action="store_true")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    args = parser.parse_args()

    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error("--tls-cert and --tls-key must be provided together")
    tls_context = None
    if args.tls_cert:
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(args.tls_cert, args.tls_key)

    async with serve(
        lambda client: proxy_connection(client, args.upstream, not args.forward_acks),
        args.listen_host,
        args.listen_port,
        compression=None,
        ssl=tls_context,
    ):
        scheme = "wss" if tls_context else "ws"
        print(f"listening={scheme}://{args.listen_host}:{args.listen_port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
