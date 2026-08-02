#!/usr/bin/env python3
"""Ad-hoc WebSocket server fixture for the `websocket` source e2e test.

Per accepted connection the server optionally validates an `Authorization` header
and an init (subscribe) message, then sends `--count` JSON text messages of the
form `{"id": <i>, "value": "message_<i>"}` and keeps the connection open, replying
to client pings automatically.

Modes:
  serve  Run the server in the foreground.
  start  Spawn `serve` in the background, wait until the port accepts
         connections, write a pid file, and return.
  stop   Terminate the background server via its pid file.
"""

import argparse
import asyncio
import http
import json
import os
import signal
import socket
import subprocess
import sys
import time

PID_FILE_TEMPLATE = "/tmp/rw_e2e_ws_server_{port}.pid"
LOG_FILE_TEMPLATE = "/tmp/rw_e2e_ws_server_{port}.log"


def pid_file(port: int) -> str:
    return PID_FILE_TEMPLATE.format(port=port)


async def run_server(args) -> None:
    from websockets.asyncio.server import serve

    def process_request(connection, request):
        """Reject the handshake when the expected Authorization header is missing."""
        if args.auth_token:
            expected = f"Bearer {args.auth_token}"
            if request.headers.get("Authorization") != expected:
                return connection.respond(
                    http.HTTPStatus.UNAUTHORIZED, "unauthorized\n"
                )
        return None

    async def handler(connection):
        if args.require_init:
            init = await connection.recv()
            if init != args.require_init:
                await connection.close(code=1008, reason="unexpected init message")
                return
        for i in range(args.count):
            await connection.send(json.dumps({"id": i, "value": f"message_{i}"}))
        # Keep the connection open so that the reader does not reconnect (which
        # would deliver duplicate messages). Client pings are answered by the
        # library; incoming frames are drained and ignored.
        async for _ in connection:
            pass

    async with serve(handler, "127.0.0.1", args.port, process_request=process_request):
        await asyncio.get_running_loop().create_future()  # run forever


def wait_for_port(port: int, timeout_secs: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def start(args) -> None:
    stop(args, quiet=True)  # make retries idempotent

    log_path = LOG_FILE_TEMPLATE.format(port=args.port)
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "serve",
        "--port",
        str(args.port),
        "--count",
        str(args.count),
    ]
    if args.require_init:
        cmd += ["--require-init", args.require_init]
    if args.auth_token:
        cmd += ["--auth-token", args.auth_token]

    with open(log_path, "ab") as log:
        process = subprocess.Popen(
            cmd, stdout=log, stderr=log, stdin=subprocess.DEVNULL
        )

    if not wait_for_port(args.port):
        process.terminate()
        print(
            f"server failed to listen on port {args.port}, see {log_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(pid_file(args.port), "w") as f:
        f.write(str(process.pid))
    print(f"started ws server on port {args.port} (pid {process.pid})")


def stop(args, quiet: bool = False) -> None:
    path = pid_file(args.port)
    if not os.path.exists(path):
        if not quiet:
            print(f"no pid file for port {args.port}, nothing to stop")
        return
    with open(path) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    os.remove(path)
    if not quiet:
        print(f"stopped ws server on port {args.port} (pid {pid})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["serve", "start", "stop"])
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--count", type=int, default=100, help="messages sent per connection"
    )
    parser.add_argument(
        "--require-init",
        default=None,
        help="require this exact init message before sending data",
    )
    parser.add_argument(
        "--auth-token",
        default=None,
        help="require header `Authorization: Bearer <token>` during the handshake",
    )
    args = parser.parse_args()

    if args.mode == "serve":
        asyncio.run(run_server(args))
    elif args.mode == "start":
        start(args)
    else:
        stop(args)


if __name__ == "__main__":
    main()
