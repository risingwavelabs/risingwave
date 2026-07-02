#!/usr/bin/env python3

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_MANAGEMENT_URL = os.getenv(
    "RABBITMQ_MANAGEMENT_URL", "http://127.0.0.1:15672"
)
DEFAULT_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
DEFAULT_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
DEFAULT_VHOST = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")


class RabbitMQManagementClient:
    def __init__(
        self, management_url: str, username: str, password: str, vhost: str
    ) -> None:
        self.management_url = management_url.rstrip("/")
        self.vhost = urllib.parse.quote(vhost, safe="")
        credentials = base64.b64encode(
            f"{username}:{password}".encode("utf-8")
        ).decode("ascii")
        self.headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        expected_statuses: tuple[int, ...] = (200, 201, 204),
    ):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"{self.management_url}{path}",
            data=data,
            headers=self.headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status not in expected_statuses:
                    raise RuntimeError(
                        f"RabbitMQ API returned unexpected status {response.status}"
                    )
                payload = response.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            if error.code in expected_statuses:
                return None
            response_body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"RabbitMQ API {method} {path} failed with status "
                f"{error.code}: {response_body}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(
                f"failed to connect to RabbitMQ Management API at "
                f"{self.management_url}: {error.reason}"
            ) from error

    def wait_until_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                self.request("GET", "/api/overview")
                return
            except RuntimeError as error:
                last_error = error
                time.sleep(0.5)
        raise RuntimeError(
            f"RabbitMQ did not become ready within {timeout:g} seconds: {last_error}"
        )

    def setup(self, exchange: str, queue: str, routing_key: str) -> None:
        exchange_path = urllib.parse.quote(exchange, safe="")
        queue_path = urllib.parse.quote(queue, safe="")

        self.request(
            "PUT",
            f"/api/exchanges/{self.vhost}/{exchange_path}",
            {
                "type": "direct",
                "durable": False,
                "auto_delete": False,
                "internal": False,
                "arguments": {},
            },
        )
        self.request(
            "PUT",
            f"/api/queues/{self.vhost}/{queue_path}",
            {
                "durable": False,
                "auto_delete": False,
                "arguments": {},
            },
        )
        self.request(
            "POST",
            f"/api/bindings/{self.vhost}/e/{exchange_path}/q/{queue_path}",
            {"routing_key": routing_key, "arguments": {}},
        )

    def purge(self, queue: str) -> None:
        queue_path = urllib.parse.quote(queue, safe="")
        self.request("DELETE", f"/api/queues/{self.vhost}/{queue_path}/contents")

    def get_messages(self, queue: str, count: int) -> list[dict]:
        queue_path = urllib.parse.quote(queue, safe="")
        return self.request(
            "POST",
            f"/api/queues/{self.vhost}/{queue_path}/get",
            {
                "count": count,
                "ackmode": "ack_requeue_false",
                "encoding": "auto",
                "truncate": 50000,
            },
        )

    def delete(self, exchange: str, queue: str) -> None:
        queue_path = urllib.parse.quote(queue, safe="")
        exchange_path = urllib.parse.quote(exchange, safe="")
        self.request(
            "DELETE",
            f"/api/queues/{self.vhost}/{queue_path}",
            expected_statuses=(204, 404),
        )
        self.request(
            "DELETE",
            f"/api/exchanges/{self.vhost}/{exchange_path}",
            expected_statuses=(204, 404),
        )


def consume(
    client: RabbitMQManagementClient, queue: str, count: int, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    messages = []

    while len(messages) < count and time.monotonic() < deadline:
        messages.extend(client.get_messages(queue, count - len(messages)))
        if len(messages) < count:
            time.sleep(0.5)

    if len(messages) != count:
        raise RuntimeError(
            f"expected {count} messages from queue `{queue}`, got {len(messages)} "
            f"within {timeout:g} seconds"
        )

    for message in messages:
        payload = message["payload"]
        try:
            # Normalize JSON output so SLT assertions are stable.
            print(json.dumps(json.loads(payload), sort_keys=True, separators=(",", ":")))
        except (json.JSONDecodeError, TypeError):
            print(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage RabbitMQ resources used by RisingWave sink tests."
    )
    parser.add_argument("--management-url", default=DEFAULT_MANAGEMENT_URL)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--virtual-host", default=DEFAULT_VHOST)
    parser.add_argument("--ready-timeout", type=float, default=30)

    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser(
        "setup", help="Create a direct exchange, queue, and binding."
    )
    setup_parser.add_argument("--exchange", required=True)
    setup_parser.add_argument("--queue", required=True)
    setup_parser.add_argument("--routing-key", required=True)

    purge_parser = subparsers.add_parser(
        "purge", help="Remove all messages from a queue."
    )
    purge_parser.add_argument("--queue", required=True)

    consume_parser = subparsers.add_parser(
        "consume", help="Consume and print an exact number of messages."
    )
    consume_parser.add_argument("--queue", required=True)
    consume_parser.add_argument("--count", type=int, required=True)
    consume_parser.add_argument("--timeout", type=float, default=30)

    delete_parser = subparsers.add_parser(
        "delete", help="Delete the test queue and exchange."
    )
    delete_parser.add_argument("--exchange", required=True)
    delete_parser.add_argument("--queue", required=True)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    client = RabbitMQManagementClient(
        args.management_url,
        args.username,
        args.password,
        args.virtual_host,
    )

    try:
        client.wait_until_ready(args.ready_timeout)
        if args.command == "setup":
            client.setup(args.exchange, args.queue, args.routing_key)
        elif args.command == "purge":
            client.purge(args.queue)
        elif args.command == "consume":
            if args.count <= 0:
                raise ValueError("--count must be greater than zero")
            consume(client, args.queue, args.count, args.timeout)
        elif args.command == "delete":
            client.delete(args.exchange, args.queue)
    except (RuntimeError, ValueError) as error:
        print(f"rabbitmq_operation: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
