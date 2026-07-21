#!/usr/bin/env python3

import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time


def forward(left, right):
    try:
        while True:
            data = left.recv(65536)
            if not data:
                break
            right.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (left, right):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass


def handle_client(client, target_host, target_port):
    try:
        upstream = socket.create_connection((target_host, target_port), timeout=10)
    except OSError:
        client.close()
        return

    threading.Thread(target=forward, args=(client, upstream), daemon=True).start()
    threading.Thread(target=forward, args=(upstream, client), daemon=True).start()


def listen(family, host, listen_port, target_host, target_port):
    server = socket.socket(family, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if family == socket.AF_INET6:
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    server.bind((host, listen_port))
    server.listen(128)

    while True:
        client, _ = server.accept()
        threading.Thread(
            target=handle_client,
            args=(client, target_host, target_port),
            daemon=True,
        ).start()


def serve(args):
    target_port = int(args.target_port)
    listen_port = int(args.listen_port)
    threads = [
        threading.Thread(
            target=listen,
            args=(socket.AF_INET, "127.0.0.1", listen_port, args.target_host, target_port),
            daemon=True,
        ),
        threading.Thread(
            target=listen,
            args=(socket.AF_INET6, "::1", listen_port, args.target_host, target_port),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    while True:
        time.sleep(3600)


def stop_pid(pid_file):
    try:
        with open(pid_file) as f:
            pid = int(f.read().strip())
    except FileNotFoundError:
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    finally:
        try:
            os.remove(pid_file)
        except FileNotFoundError:
            pass


def wait_ready(port):
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
            with socket.create_connection(("::1", port), timeout=1):
                pass
            return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"proxy on port {port} did not become ready")


def start(args):
    stop_pid(args.pid_file)

    log = open(args.log_file, "ab", buffering=0)
    cmd = [
        sys.executable,
        __file__,
        "serve",
        "--target-host",
        args.target_host,
        "--target-port",
        args.target_port,
        "--listen-port",
        args.listen_port,
    ]
    proc = subprocess.Popen(cmd, stdout=log, stderr=log, close_fds=True)
    with open(args.pid_file, "w") as f:
        f.write(str(proc.pid))

    try:
        wait_ready(int(args.listen_port))
    except Exception:
        proc.terminate()
        raise


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start")
    start_parser.add_argument("--target-host", required=True)
    start_parser.add_argument("--target-port", required=True)
    start_parser.add_argument("--listen-port", required=True)
    start_parser.add_argument("--pid-file", required=True)
    start_parser.add_argument("--log-file", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--target-host", required=True)
    serve_parser.add_argument("--target-port", required=True)
    serve_parser.add_argument("--listen-port", required=True)

    stop_parser = subparsers.add_parser("stop")
    stop_parser.add_argument("--pid-file", required=True)

    args = parser.parse_args()
    if args.command == "start":
        start(args)
    elif args.command == "serve":
        serve(args)
    elif args.command == "stop":
        stop_pid(args.pid_file)


if __name__ == "__main__":
    main()
