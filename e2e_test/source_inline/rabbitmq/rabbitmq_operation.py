#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
import urllib.parse

import requests

DEFAULT_MANAGEMENT_URL = "http://rabbitmq-server:15672"
DEFAULT_USER = "guest"
DEFAULT_PASSWORD = "guest"
DEFAULT_VHOST = "/"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


class RabbitmqAdmin:
    def __init__(self) -> None:
        self.base_url = env("RISEDEV_RABBITMQ_MANAGEMENT_URL", env("RABBITMQ_MANAGEMENT_URL", DEFAULT_MANAGEMENT_URL)).rstrip("/")
        self.user = env("RISEDEV_RABBITMQ_USER", env("RABBITMQ_USER", DEFAULT_USER))
        self.password = env("RISEDEV_RABBITMQ_PASSWORD", env("RABBITMQ_PASSWORD", DEFAULT_PASSWORD))
        self.vhost = env("RISEDEV_RABBITMQ_VHOST", env("RABBITMQ_VHOST", DEFAULT_VHOST))
        self.session = requests.Session()
        self.session.auth = (self.user, self.password)

    def _url(self, *parts: str) -> str:
        encoded = [urllib.parse.quote(part, safe="") for part in parts]
        return f"{self.base_url}/api/{'/'.join(encoded)}"

    def _request(self, method: str, path_parts: tuple[str, ...], **kwargs) -> requests.Response:
        response = self.session.request(method, self._url(*path_parts), timeout=10, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"RabbitMQ API {method} {response.url} failed: {response.status_code} {response.text}")
        return response

    def health(self) -> None:
        last_error = None
        for _ in range(60):
            try:
                response = self.session.get(f"{self.base_url}/api/overview", timeout=5)
                if response.status_code == 200:
                    return
                last_error = f"HTTP {response.status_code}: {response.text}"
            except Exception as exc:  # noqa: BLE001 - report final readiness failure clearly.
                last_error = str(exc)
            time.sleep(1)
        raise RuntimeError(f"RabbitMQ management API is not ready: {last_error}")

    def reset(self, exchange: str, queue: str) -> None:
        # Delete queue first so bindings are removed before deleting the exchange.
        for path in (("queues", self.vhost, queue), ("exchanges", self.vhost, exchange)):
            url = f"{self.base_url}/api/{'/'.join(urllib.parse.quote(part, safe='') for part in path)}"
            response = self.session.delete(url, timeout=10)
            if response.status_code not in (204, 404):
                raise RuntimeError(f"RabbitMQ API DELETE {url} failed: {response.status_code} {response.text}")

    def setup(self, exchange: str, queue: str, routing_key: str) -> None:
        self._request("PUT", ("exchanges", self.vhost, exchange), json={"type": "direct", "durable": False, "auto_delete": False, "arguments": {}})
        self._request("PUT", ("queues", self.vhost, queue), json={"durable": False, "auto_delete": False, "arguments": {}})
        self._request(
            "POST",
            ("bindings", self.vhost, "e", exchange, "q", queue),
            json={"routing_key": routing_key, "arguments": {}},
        )

    def publish_json(self, exchange: str, routing_key: str, count: int) -> None:
        for i in range(count):
            payload = json.dumps({"id": i, "value": f"message_{i}"}, separators=(",", ":"))
            response = self._request(
                "POST",
                ("exchanges", self.vhost, exchange, "publish"),
                json={
                    "properties": {"content_type": "application/json"},
                    "routing_key": routing_key,
                    "payload": payload,
                    "payload_encoding": "string",
                },
            )
            routed = response.json().get("routed")
            if routed is not True:
                raise RuntimeError(f"RabbitMQ publish did not route message {i}: {response.text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="RabbitMQ source-inline test helper")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health")

    for name in ("reset", "setup"):
        p = sub.add_parser(name)
        p.add_argument("--exchange", required=True)
        p.add_argument("--queue", required=True)
        p.add_argument("--routing-key", required=True)

    p = sub.add_parser("publish-json")
    p.add_argument("--exchange", required=True)
    p.add_argument("--routing-key", required=True)
    p.add_argument("--count", type=int, default=100)

    args = parser.parse_args()
    admin = RabbitmqAdmin()

    if args.command == "health":
        admin.health()
    elif args.command == "reset":
        admin.health()
        admin.reset(args.exchange, args.queue)
    elif args.command == "setup":
        admin.health()
        admin.setup(args.exchange, args.queue, args.routing_key)
    elif args.command == "publish-json":
        admin.health()
        admin.publish_json(args.exchange, args.routing_key, args.count)
    else:
        parser.error(f"unknown command: {args.command}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
