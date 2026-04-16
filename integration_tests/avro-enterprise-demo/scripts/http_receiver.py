from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


ARTIFACT_DIR = Path("/workspace/artifacts/http-receiver")
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
RECEIVED_FILE = ARTIFACT_DIR / "received.jsonl"
ERROR_FILE = ARTIFACT_DIR / "error.log"
STATE_FILE = ARTIFACT_DIR / "fail-once-state.json"
FAIL_ONCE_CUSTOMER_KEY = os.getenv("FAIL_ONCE_CUSTOMER_KEY", "")


def has_failed_once(customer_key: str) -> bool:
    if not STATE_FILE.exists():
        return False
    state = json.loads(STATE_FILE.read_text())
    return state.get(customer_key, False)


def mark_failed_once(customer_key: str) -> None:
    state = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text())
    state[customer_key] = True
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            ERROR_FILE.write_text(f"invalid-json: {exc}\n")
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"invalid json")
            return

        customer_key = payload.get("customer_key", "")
        if FAIL_ONCE_CUSTOMER_KEY and customer_key == FAIL_ONCE_CUSTOMER_KEY and not has_failed_once(customer_key):
            mark_failed_once(customer_key)
            with ERROR_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"customer_key": customer_key, "error": "forced failure"}) + "\n")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"forced failure")
            return

        with RECEIVED_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    port = int(os.getenv("RECEIVER_PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"HTTP receiver listening on {port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
