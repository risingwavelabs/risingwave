#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import socket
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha1
from urllib.parse import urlparse


class SimpleWebSocketApp:
    def __init__(
        self,
        url: str,
        header: list[str],
        on_open: Callable[["SimpleWebSocketApp"], None],
        on_message: Callable[["SimpleWebSocketApp", str], None],
        on_error: Callable[["SimpleWebSocketApp", object], None],
        on_close: Callable[["SimpleWebSocketApp", int | None, str | None], None],
    ) -> None:
        self.url = url
        self.header = header
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self._socket: socket.socket | None = None
        self._read_buffer = bytearray()
        self._send_lock = threading.Lock()
        self._closed = False

    def run_forever(self) -> None:
        close_code: int | None = None
        close_msg: str | None = None
        try:
            parsed = urlparse(self.url)
            if parsed.scheme != "ws":
                raise RuntimeError(f"unsupported websocket scheme: {parsed.scheme}")
            if parsed.hostname is None or parsed.port is None:
                raise RuntimeError(f"invalid websocket url: {self.url}")

            request_key = base64.b64encode(os.urandom(16)).decode()
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"

            sock = socket.create_connection((parsed.hostname, parsed.port), timeout=10)
            sock.settimeout(10)
            self._socket = sock

            header_lines = "".join(f"{line}\r\n" for line in self.header)
            handshake = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {parsed.hostname}:{parsed.port}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {request_key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                f"{header_lines}\r\n"
            )
            sock.sendall(handshake.encode())
            response = self._recv_until(b"\r\n\r\n")
            self._validate_handshake(response.decode(errors="replace"), request_key)
            sock.settimeout(None)
            self.on_open(self)

            while not self._closed:
                opcode, payload = self._recv_frame()
                if opcode == 0x1:
                    self.on_message(self, payload.decode())
                elif opcode == 0x8:
                    close_code, close_msg = self._decode_close_payload(payload)
                    break
                elif opcode == 0x9:
                    self._send_frame(0xA, payload)
                elif opcode == 0xA:
                    continue
                else:
                    raise RuntimeError(f"unsupported websocket opcode: {opcode}")
        except Exception as err:  # noqa: BLE001
            if not self._closed and str(err) != "websocket closed unexpectedly":
                self.on_error(self, err)
        finally:
            self._closed = True
            if self._socket is not None:
                try:
                    self._socket.close()
                except OSError:
                    pass
                self._socket = None
            self.on_close(self, close_code, close_msg)

    def send(self, text: str) -> None:
        self._send_frame(0x1, text.encode())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self._socket is None:
            raise RuntimeError("websocket is not connected")

        first_byte = 0x80 | opcode
        payload_len = len(payload)
        if payload_len < 126:
            second_byte = 0x80 | payload_len
            header = bytes([first_byte, second_byte])
        elif payload_len < (1 << 16):
            second_byte = 0x80 | 126
            header = bytes([first_byte, second_byte]) + payload_len.to_bytes(2, "big")
        else:
            second_byte = 0x80 | 127
            header = bytes([first_byte, second_byte]) + payload_len.to_bytes(8, "big")

        mask = os.urandom(4)
        masked_payload = bytes(
            byte ^ mask[index % len(mask)] for index, byte in enumerate(payload)
        )

        with self._send_lock:
            self._socket.sendall(header + mask + masked_payload)

    def _recv_until(self, terminator: bytes) -> bytes:
        if self._socket is None:
            raise RuntimeError("websocket is not connected")

        while terminator not in self._read_buffer:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise RuntimeError("websocket closed during handshake")
            self._read_buffer.extend(chunk)

        end = self._read_buffer.index(terminator) + len(terminator)
        data = bytes(self._read_buffer[:end])
        del self._read_buffer[:end]
        return data

    def _recv_exact(self, size: int) -> bytes:
        if self._socket is None:
            raise RuntimeError("websocket is not connected")

        while len(self._read_buffer) < size:
            chunk = self._socket.recv(size - len(self._read_buffer))
            if not chunk:
                raise RuntimeError("websocket closed unexpectedly")
            self._read_buffer.extend(chunk)

        data = bytes(self._read_buffer[:size])
        del self._read_buffer[:size]
        return data

    def _recv_frame(self) -> tuple[int, bytes]:
        header = self._recv_exact(2)
        first_byte, second_byte = header
        opcode = first_byte & 0x0F
        masked = (second_byte & 0x80) != 0
        payload_len = second_byte & 0x7F
        if payload_len == 126:
            payload_len = int.from_bytes(self._recv_exact(2), "big")
        elif payload_len == 127:
            payload_len = int.from_bytes(self._recv_exact(8), "big")

        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(payload_len)
        if masked:
            payload = bytes(
                byte ^ mask[index % len(mask)] for index, byte in enumerate(payload)
            )
        return opcode, payload

    @staticmethod
    def _validate_handshake(response: str, request_key: str) -> None:
        lines = response.split("\r\n")
        if not lines or "101" not in lines[0]:
            raise RuntimeError(f"websocket upgrade failed: {lines[0] if lines else response}")

        headers = {}
        for line in lines[1:]:
            if not line or ":" not in line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()

        expected_accept = base64.b64encode(
            sha1(
                (
                    request_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                ).encode()
            ).digest()
        ).decode()
        actual_accept = headers.get("sec-websocket-accept")
        if actual_accept != expected_accept:
            raise RuntimeError(
                f"invalid Sec-WebSocket-Accept header: {actual_accept!r}"
            )

    @staticmethod
    def _decode_close_payload(payload: bytes) -> tuple[int | None, str | None]:
        if len(payload) >= 2:
            code = int.from_bytes(payload[:2], "big")
            message = payload[2:].decode(errors="replace") if len(payload) > 2 else ""
            return code, message
        return None, None


@dataclass
class WsState:
    acks_received: list[int] = field(default_factory=list)
    fatal: list[str] = field(default_factory=list)
    ws_connected: threading.Event = field(default_factory=threading.Event)
    ws_closed: threading.Event = field(default_factory=threading.Event)
    connect_error: str | None = None


def parse_header(header: str) -> tuple[str, str]:
    name, separator, value = header.partition(":")
    if not separator or not name.strip():
        raise argparse.ArgumentTypeError(
            f"header must be in `name: value` format: {header}"
        )
    return name.strip(), value.strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive websocket ingest e2e scenarios")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4560)
    parser.add_argument("--database", default="dev")
    parser.add_argument("--schema", default="public")
    parser.add_argument("--table", required=True)
    parser.add_argument("--secret", required=True)
    parser.add_argument(
        "--scenario",
        required=True,
        choices=[
            "success",
            "multi_column_no_pk",
            "single_jsonb_no_pk",
            "delete_only_pk",
            "error_missing_pk",
            "error_incomplete_composite_pk_insert",
            "error_incomplete_composite_pk_delete",
            "error_type",
            "timestamp_milli",
            "time_milli",
            "bigint_precise",
            "invalid_signature",
            "invalid_timestamp",
            "invalid_decoder_header",
        ],
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        type=parse_header,
        help="Extra websocket handshake header in `name: value` format",
    )
    parser.add_argument(
        "--expected-fatal-substring",
        default="",
        help="Expected fatal substring for failure scenarios",
    )
    return parser.parse_args()


def sign_payload(secret: str, payload: str) -> str:
    digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def wait_for(predicate, timeout: float, description: str) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {description}")


def connect_and_init(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    secret: str,
    extra_headers: dict[str, str],
    init_message: str | None = None,
    signature_secret: str | None = None,
) -> tuple[SimpleWebSocketApp, WsState]:
    state = WsState()
    ws_url = f"ws://{host}:{port}/ingest/{database}/{schema}/{table}"
    init_message = init_message or json.dumps(
        {"type": "init", "timestamp": int(time.time() * 1000)},
        separators=(",", ":"),
    )

    def on_message(_ws, message: str) -> None:
        msg = json.loads(message)
        print(f"  <- {message}")
        if "ack" in msg:
            state.acks_received.append(msg["ack"])
        elif "fatal" in msg:
            state.fatal.append(msg["fatal"])

    def on_error(_ws, error: object) -> None:
        state.connect_error = str(error)
        print(f"WebSocket error: {error}", file=sys.stderr)

    def on_close(_ws, close_status_code, close_msg) -> None:
        state.ws_closed.set()
        print(f"WebSocket closed: {close_status_code} {close_msg}")

    def on_open(_ws) -> None:
        state.ws_connected.set()
        print(f"WebSocket connected to {ws_url}")

    headers = {
        "x-rw-signature": sign_payload(signature_secret or secret, init_message)
    }
    headers.update(extra_headers)
    ws = SimpleWebSocketApp(
        ws_url,
        header=[f"{name}: {value}" for name, value in headers.items()],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    thread = threading.Thread(target=ws.run_forever, daemon=True)
    thread.start()

    if not state.ws_connected.wait(timeout=10):
        raise RuntimeError(
            f"failed to connect websocket {ws_url}: {state.connect_error or 'unknown error'}"
        )

    print("\n--- Sending init ---")
    print(f"  -> {init_message}")
    ws.send(init_message)
    time.sleep(0.2)
    return ws, state


def send_batch(ws: SimpleWebSocketApp, batch: list[dict]) -> None:
    text = json.dumps(batch, separators=(",", ":"))
    print("\n--- Sending batched DML payload ---")
    print(f"  -> {text}")
    ws.send(text)


def expect_no_fatal(state: WsState) -> None:
    if state.fatal:
        raise RuntimeError(f"unexpected fatal response: {state.fatal}")


def expect_fatal_and_close(
    state: WsState, expected_dml_id: int, expected_error_substring: str
) -> None:
    wait_for(
        lambda: bool(state.fatal),
        timeout=10,
        description="websocket fatal response",
    )
    if f"dml_id {expected_dml_id}" not in state.fatal[0]:
        raise RuntimeError(
            f"expected fatal containing dml_id {expected_dml_id}, got {state.fatal}"
        )
    if expected_error_substring not in state.fatal[0]:
        raise RuntimeError(f"unexpected fatal message: {state.fatal[0]!r}")
    wait_for(state.ws_closed.is_set, 10, "server-side websocket close after error")


def scenario_success(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "id": 401,
                    "customer_name": "TestUser",
                    "product": "Widget",
                    "quantity": 5,
                    "price": "19.99",
                    "status": "pending",
                    "created_at": "2026-04-15 10:00:00",
                },
            },
            {
                "dml_id": 2,
                "op": "upsert",
                "data": {
                    "id": 402,
                    "customer_name": "TestUser2",
                    "product": "Gadget",
                    "quantity": 2,
                    "price": "49.99",
                    "status": "shipped",
                    "created_at": "2026-04-15 10:01:00",
                },
            },
            {
                "dml_id": 3,
                "op": "upsert",
                "data": {
                    "id": 401,
                    "customer_name": "TestUser",
                    "product": "Widget",
                    "quantity": 5,
                    "price": "19.99",
                    "status": "delivered",
                    "created_at": "2026-04-15 10:00:00",
                },
            },
            {
                "dml_id": 4,
                "op": "delete",
                "data": {
                    "id": 402,
                    "customer_name": "TestUser2",
                    "product": "Gadget",
                    "quantity": 2,
                    "price": "49.99",
                    "status": "shipped",
                    "created_at": "2026-04-15 10:01:00",
                },
            },
        ],
    )
    wait_for(
        lambda: len(state.acks_received) >= 4 or bool(state.fatal),
        timeout=30,
        description="websocket success responses",
    )
    expect_no_fatal(state)
    if sorted(state.acks_received) != [1, 2, 3, 4]:
        raise RuntimeError(f"expected acks [1, 2, 3, 4], got {state.acks_received}")


def scenario_multi_column_no_pk(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "id": 601,
                    "customer_name": "NoPkAlice",
                    "amount": 61.5,
                },
            },
            {
                "dml_id": 2,
                "op": "upsert",
                "data": {
                    "id": 602,
                    "customer_name": "NoPkBob",
                    "amount": 62.5,
                },
            },
        ],
    )
    wait_for(
        lambda: len(state.acks_received) >= 2 or bool(state.fatal),
        timeout=30,
        description="multi-column no-pk responses",
    )
    expect_no_fatal(state)
    if sorted(state.acks_received) != [1, 2]:
        raise RuntimeError(f"expected acks [1, 2], got {state.acks_received}")


def scenario_single_jsonb_no_pk(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "id": 701,
                    "source": "ws-jsonb",
                    "status": "created",
                },
            },
            {
                "dml_id": 2,
                "op": "upsert",
                "data": {
                    "id": 702,
                    "source": "ws-jsonb",
                    "status": "updated",
                },
            },
        ],
    )
    wait_for(
        lambda: len(state.acks_received) >= 2 or bool(state.fatal),
        timeout=30,
        description="single-jsonb no-pk responses",
    )
    expect_no_fatal(state)
    if sorted(state.acks_received) != [1, 2]:
        raise RuntimeError(f"expected acks [1, 2], got {state.acks_received}")


def scenario_delete_only_pk(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "id": 501,
                    "customer_name": "DeleteOnlyPk",
                    "amount": 42.42,
                },
            },
            {
                "dml_id": 2,
                "op": "delete",
                "data": {
                    "id": 501,
                },
            },
        ],
    )
    wait_for(
        lambda: len(state.acks_received) >= 2 or bool(state.fatal),
        timeout=30,
        description="delete-only-pk responses",
    )
    expect_no_fatal(state)
    if sorted(state.acks_received) != [1, 2]:
        raise RuntimeError(f"expected acks [1, 2], got {state.acks_received}")


def scenario_error_missing_pk(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {"customer_name": "Alice", "amount": 99.99},
            }
        ],
    )
    expect_fatal_and_close(state, 1, "failed to decode webhook JSON payload")


def scenario_error_incomplete_composite_pk_insert(
    ws: SimpleWebSocketApp, state: WsState
) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "id": 1001,
                    "customer_name": "CompositeInsert",
                    "amount": 77.77,
                },
            }
        ],
    )
    expect_fatal_and_close(state, 1, "failed to decode webhook JSON payload")


def scenario_error_incomplete_composite_pk_delete(
    ws: SimpleWebSocketApp, state: WsState
) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {
                    "tenant_id": 11,
                    "id": 2002,
                    "customer_name": "CompositeDelete",
                    "amount": 88.88,
                },
            }
        ],
    )
    wait_for(
        lambda: 1 in state.acks_received or bool(state.fatal),
        timeout=10,
        description="seed row before incomplete composite delete",
    )
    expect_no_fatal(state)

    send_batch(
        ws,
        [
            {
                "dml_id": 2,
                "op": "delete",
                "data": {
                    "tenant_id": 11,
                },
            }
        ],
    )
    expect_fatal_and_close(state, 2, "failed to decode webhook JSON payload")


def scenario_error_type(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [
            {
                "dml_id": 1,
                "op": "upsert",
                "data": {"id": "not-an-int", "customer_name": "Bob", "amount": 77.77},
            }
        ],
    )
    expect_fatal_and_close(state, 1, "failed to decode webhook JSON payload")


def scenario_timestamp_milli(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [{"dml_id": 1, "op": "upsert", "data": {"id": 1, "event_time": 1712800800123}}],
    )
    wait_for(lambda: 1 in state.acks_received or bool(state.fatal), 10, "timestamp ack")
    expect_no_fatal(state)


def scenario_time_milli(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [{"dml_id": 1, "op": "upsert", "data": {"id": 1, "event_time": 3723123}}],
    )
    wait_for(lambda: 1 in state.acks_received or bool(state.fatal), 10, "time ack")
    expect_no_fatal(state)


def scenario_bigint_precise(ws: SimpleWebSocketApp, state: WsState) -> None:
    send_batch(
        ws,
        [{"dml_id": 1, "op": "upsert", "data": {"id": 1, "amount": "AeJA"}}],
    )
    wait_for(lambda: 1 in state.acks_received or bool(state.fatal), 10, "bigint ack")
    expect_no_fatal(state)


def scenario_invalid_decoder_header(
    ws: SimpleWebSocketApp, state: WsState, expected_fatal_substring: str
) -> None:
    send_batch(
        ws,
        [{"dml_id": 1, "op": "upsert", "data": {"id": 1, "customer_name": "Alice", "amount": 99.99}}],
    )
    wait_for(lambda: bool(state.fatal), 10, "fatal websocket response")
    if expected_fatal_substring not in state.fatal[0]:
        raise RuntimeError(
            f"expected fatal containing {expected_fatal_substring!r}, got {state.fatal[0]!r}"
        )


def scenario_expect_fatal(state: WsState, expected_fatal_substring: str) -> None:
    wait_for(lambda: bool(state.fatal), 10, "fatal websocket response")
    if expected_fatal_substring not in state.fatal[0]:
        raise RuntimeError(
            f"expected fatal containing {expected_fatal_substring!r}, got {state.fatal[0]!r}"
        )


def main() -> int:
    args = parse_args()

    scenario_headers: dict[str, str] = {}
    init_message: str | None = None
    signature_secret: str | None = None
    if args.scenario == "timestamp_milli":
        scenario_headers["x-rw-webhook-json-timestamp-handling-mode"] = "milli"
    elif args.scenario == "time_milli":
        scenario_headers["x-rw-webhook-json-time-handling-mode"] = "milli"
    elif args.scenario == "bigint_precise":
        scenario_headers["x-rw-webhook-json-bigint-unsigned-handling-mode"] = "precise"
    elif args.scenario == "invalid_signature":
        signature_secret = f"{args.secret}_INVALID"
    elif args.scenario == "invalid_timestamp":
        init_message = json.dumps(
            {"type": "init", "timestamp": 0},
            separators=(",", ":"),
        )
    scenario_headers.update(dict(args.header))

    ws, state = connect_and_init(
        host=args.host,
        port=args.port,
        database=args.database,
        schema=args.schema,
        table=args.table,
        secret=args.secret,
        extra_headers=scenario_headers,
        init_message=init_message,
        signature_secret=signature_secret,
    )

    try:
        if args.scenario == "success":
            scenario_success(ws, state)
        elif args.scenario == "multi_column_no_pk":
            scenario_multi_column_no_pk(ws, state)
        elif args.scenario == "single_jsonb_no_pk":
            scenario_single_jsonb_no_pk(ws, state)
        elif args.scenario == "delete_only_pk":
            scenario_delete_only_pk(ws, state)
        elif args.scenario == "error_missing_pk":
            scenario_error_missing_pk(ws, state)
        elif args.scenario == "error_incomplete_composite_pk_insert":
            scenario_error_incomplete_composite_pk_insert(ws, state)
        elif args.scenario == "error_incomplete_composite_pk_delete":
            scenario_error_incomplete_composite_pk_delete(ws, state)
        elif args.scenario == "error_type":
            scenario_error_type(ws, state)
        elif args.scenario == "timestamp_milli":
            scenario_timestamp_milli(ws, state)
        elif args.scenario == "time_milli":
            scenario_time_milli(ws, state)
        elif args.scenario == "bigint_precise":
            scenario_bigint_precise(ws, state)
        elif args.scenario == "invalid_signature":
            scenario_expect_fatal(state, "Signature verification failed")
        elif args.scenario == "invalid_timestamp":
            scenario_expect_fatal(state, "timestamp skew")
        elif args.scenario == "invalid_decoder_header":
            scenario_invalid_decoder_header(
                ws, state, args.expected_fatal_substring
            )
        else:
            raise RuntimeError(f"unsupported scenario: {args.scenario}")
    except RuntimeError as err:
        print(err, file=sys.stderr)
        return 1
    finally:
        ws.close()
        time.sleep(0.2)

    print("\n--- Results ---")
    print(f"Acks received: {sorted(state.acks_received)}")
    print(f"Fatal: {state.fatal}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
