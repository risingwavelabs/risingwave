#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


QUEUE_SUFFIXES = ("default", "direct", "unmatched", "protobuf")
QUEUE_BINDINGS = (("direct", "selected"), ("unmatched", "other"))
HTTP_TIMEOUT_SECONDS = 10
DEFAULT_VERIFY_TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 0.2
MESSAGE_BATCH_SIZE = 256
PERSISTENT_DELIVERY_MODE = 2
JSON_CONTENT_TYPE = "application/json"
PROTOBUF_CONTENT_TYPE = "application/x-protobuf"
PAYLOAD_ENCODING = "base64"


class RabbitMq:
    def __init__(self):
        self.url = os.environ["RISEDEV_RABBITMQ_MANAGEMENT_URL"].rstrip("/")
        self.vhost = os.environ["RISEDEV_RABBITMQ_VHOST"]
        credentials = "{}:{}".format(
            os.environ["RISEDEV_RABBITMQ_USERNAME"],
            os.environ["RISEDEV_RABBITMQ_PASSWORD"],
        )
        self.authorization = "Basic " + base64.b64encode(credentials.encode()).decode()

    def request(self, method, *segments, body=None, allow_missing=False):
        path = "/api/" + "/".join(quote(segment, safe="") for segment in segments)
        request = Request(
            self.url + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": self.authorization,
                "Content-Type": JSON_CONTENT_TYPE,
            },
        )
        try:
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                data = response.read()
                return json.loads(data) if data else None
        except HTTPError as error:
            if allow_missing and error.code == 404:
                return None
            raise RuntimeError(
                f"RabbitMQ {method} {path} failed with HTTP {error.code}"
            ) from None

    def cleanup(self, prefix):
        for suffix in QUEUE_SUFFIXES:
            self.request(
                "DELETE", "queues", self.vhost, f"{prefix}_{suffix}", allow_missing=True
            )
        self.request("DELETE", "exchanges", self.vhost, prefix, allow_missing=True)

    def setup(self, prefix):
        self.cleanup(prefix)
        for suffix in QUEUE_SUFFIXES:
            self.request(
                "PUT",
                "queues",
                self.vhost,
                f"{prefix}_{suffix}",
                body={"durable": True, "auto_delete": False, "arguments": {}},
            )
        self.request(
            "PUT",
            "exchanges",
            self.vhost,
            prefix,
            body={
                "type": "direct",
                "durable": True,
                "auto_delete": False,
                "arguments": {},
            },
        )
        for suffix, key in QUEUE_BINDINGS:
            self.request(
                "POST",
                "bindings",
                self.vhost,
                "e",
                prefix,
                "q",
                f"{prefix}_{suffix}",
                body={"routing_key": key, "arguments": {}},
            )

    def get_messages(self, queue):
        return self.request(
            "POST",
            "queues",
            self.vhost,
            queue,
            "get",
            body={
                "count": MESSAGE_BATCH_SIZE,
                "ackmode": "ack_requeue_false",
                "encoding": PAYLOAD_ENCODING,
            },
        )

    def cleanup_recovery(self, prefix):
        # This user belongs exclusively to the serial recovery test. Deleting it
        # also closes connections left by an interrupted run.
        self.request("DELETE", "users", prefix, allow_missing=True)
        self.cleanup(prefix)
        self.request(
            "DELETE", "exchanges", self.vhost, f"{prefix}_missing", allow_missing=True
        )

    def setup_recovery(self, prefix):
        self.cleanup_recovery(prefix)
        self.setup(prefix)
        self.request(
            "PUT",
            "users",
            prefix,
            body={"password": "recovery-test-only", "tags": ""},
        )
        self.request(
            "PUT",
            "permissions",
            self.vhost,
            prefix,
            body={"configure": "^$", "write": f"^{prefix}.*$", "read": "^$"},
        )

    def bind_recovery(self, prefix):
        self.request(
            "POST",
            "bindings",
            self.vhost,
            "e",
            prefix,
            "q",
            f"{prefix}_direct",
            body={"routing_key": "recovered", "arguments": {}},
        )

    def declare_recovery_exchange(self, prefix):
        self.request(
            "PUT",
            "exchanges",
            self.vhost,
            f"{prefix}_missing",
            body={
                "type": "direct", "durable": True, "auto_delete": False,
                "arguments": {},
            },
        )
        self.request(
            "POST",
            "bindings",
            self.vhost,
            "e",
            f"{prefix}_missing",
            "q",
            f"{prefix}_default",
            body={"routing_key": "recovered", "arguments": {}},
        )

    def interrupt_recovery(self, prefix):
        # Management statistics can lag behind actual AMQP connections.
        deadline = time.monotonic() + 30
        while True:
            connections = [
                connection
                for connection in self.request("GET", "connections")
                if connection["user"] == prefix and connection["vhost"] == self.vhost
            ]
            if connections:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("No recovery-test connection to interrupt")
            time.sleep(POLL_INTERVAL_SECONDS)
        container = os.environ.get("RABBITMQ_TEST_RESTART_CONTAINER")
        if container:
            # Optional local variant: restart a dedicated test broker. The CI
            # container has no Docker socket and uses scoped connection closure.
            print(f"Restarting dedicated RabbitMQ container {container}", file=sys.stderr)
            subprocess.run(
                ["docker", "restart", container],
                check=True,
                timeout=90,
                stdout=subprocess.DEVNULL,
            )
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    self.request("GET", "overview")
                    return
                except (OSError, RuntimeError):
                    time.sleep(POLL_INTERVAL_SECONDS)
            raise TimeoutError("RabbitMQ management API did not recover after restart")
        print(f"Closing {len(connections)} recovery-test connections", file=sys.stderr)
        for connection in connections:
            # A short-lived validation connection can disappear after listing.
            # The SLT separately requires a failure from the actual sink writer.
            self.request("DELETE", "connections", connection["name"], allow_missing=True)

    def verify(self, args):
        if args.ids:
            expected = [{"id": i} for i in range(args.ids[0], args.ids[1] + 1)]
        else:
            expected = json.loads(Path(args.expected).read_text())

        def canonical(value):
            return json.dumps(value, sort_keys=True, ensure_ascii=False)

        expected = {canonical(value) for value in expected}
        allowed_previous = set()
        if args.allow_previous_ids:
            first, last = args.allow_previous_ids
            allowed_previous = {canonical({"id": i}) for i in range(first, last + 1)}
        seen = set()
        deadline = time.monotonic() + args.timeout
        content_type = (
            JSON_CONTENT_TYPE if args.encoding == "json" else PROTOBUF_CONTENT_TYPE
        )
        while time.monotonic() < deadline:
            messages = self.get_messages(args.queue)
            for message in messages:
                assert message["exchange"] == args.exchange, message
                assert message["routing_key"] == args.routing_key, message
                assert message["properties"]["content_type"] == content_type, message
                assert (
                    message["properties"]["delivery_mode"] == PERSISTENT_DELIVERY_MODE
                ), message
                assert message["payload_encoding"] == PAYLOAD_ENCODING, message
                payload = base64.b64decode(message["payload"], validate=True)
                value = json.loads(payload) if args.encoding == "json" else payload.hex()
                actual = canonical(value)
                assert actual in expected | allowed_previous, (
                    f"Unexpected message in {args.queue}: {value!r}"
                )
                if actual in expected:
                    seen.add(actual)
            # Duplicate deliveries are valid for an at-least-once sink. Every expected
            # payload must still arrive, and every received payload must match exactly.
            if seen == expected and not messages:
                return
            time.sleep(POLL_INTERVAL_SECONDS)
        raise AssertionError(f"Missing messages in {args.queue}: {sorted(expected - seen)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in (
        "setup",
        "cleanup",
        "setup-recovery",
        "cleanup-recovery",
        "bind-recovery",
        "declare-recovery-exchange",
        "interrupt-recovery",
    ):
        commands.add_parser(name).add_argument("prefix")
    commands.add_parser("empty").add_argument("queue")
    verify = commands.add_parser("verify")
    verify.add_argument("queue")
    expected = verify.add_mutually_exclusive_group(required=True)
    expected.add_argument("--expected")
    expected.add_argument("--ids", nargs=2, type=int)
    verify.add_argument("--allow-previous-ids", nargs=2, type=int)
    verify.add_argument("--encoding", choices=("json", "protobuf"), default="json")
    verify.add_argument("--exchange", default="")
    verify.add_argument("--routing-key", required=True)
    verify.add_argument("--timeout", type=float, default=DEFAULT_VERIFY_TIMEOUT_SECONDS)
    args = parser.parse_args()
    rabbitmq = RabbitMq()
    if args.command == "verify":
        rabbitmq.verify(args)
    elif args.command == "empty":
        messages = rabbitmq.get_messages(args.queue)
        assert not messages, f"Unexpected messages in {args.queue}: {messages}"
    else:
        getattr(rabbitmq, args.command.replace("-", "_"))(args.prefix)


if __name__ == "__main__":
    main()
