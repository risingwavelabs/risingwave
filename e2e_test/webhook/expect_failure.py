import argparse
import hashlib
import hmac
import sys

import requests

SERVER_URL = "http://127.0.0.1:4560/webhook/dev/public/"


def generate_signature_hmac(secret: str, payload: str) -> str:
    return "sha1=" + hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        digestmod=hashlib.sha1,
    ).hexdigest()


def parse_header(header: str) -> tuple[str, str]:
    name, separator, value = header.partition(":")
    if not separator or not name.strip():
        raise argparse.ArgumentTypeError(
            f"header must be in `name: value` format: {header}"
        )
    return name.strip(), value.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Expect webhook request failure")
    parser.add_argument("--secret", required=True, help="Secret key for generating signature")
    parser.add_argument("--table", required=True, help="Webhook table that should reject requests")
    parser.add_argument("--payload", required=True, help="Request payload")
    parser.add_argument("--expected-status", required=True, type=int, help="Expected HTTP status")
    parser.add_argument("--expected-error", required=True, help="Expected error substring")
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        type=parse_header,
        help="Extra request header in `name: value` format",
    )
    args = parser.parse_args()

    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": generate_signature_hmac(args.secret, args.payload),
    }
    headers.update(dict(args.header))

    response = requests.post(SERVER_URL + args.table, headers=headers, data=args.payload)

    if response.status_code == args.expected_status and args.expected_error in response.text:
        print("Webhook request rejected as expected:", response.text)
        return 0

    print(
        f"Unexpected response, Status Code: {response.status_code}, Response: {response.text}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
