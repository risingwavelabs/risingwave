import argparse
import requests
import json
import sys
import hmac
import hashlib

message = {
    "event": "order.created",
    "source": "placeholder",
    "auth_algo": "placeholder",
    "data": {
        "order_id": 1234,
        "customer_name": "Alice",
        "amount": 99.99,
        "currency": "USD"
    },
    "timestamp": 1639581841
}

SERVER_URL = "http://127.0.0.1:8080/message/root/dev/public/"


def generate_signature_hmac(secret, payload, auth_algo):
    secret_bytes = bytes(secret, 'utf-8')
    payload_bytes = bytes(payload, 'utf-8')
    signature = ""
    if auth_algo == "sha1":
        signature = "sha1=" + hmac.new(secret_bytes, payload_bytes, digestmod=hashlib.sha1).hexdigest()
    elif auth_algo == "sha256":
        signature = "sha256=" + hmac.new(secret_bytes, payload_bytes, digestmod=hashlib.sha256).hexdigest()
    else:
        print("Unsupported auth type")
        sys.exit(1)
    return signature


def send_webhook(url, headers, payload_json):
    response = requests.post(url, headers=headers, data=payload_json)

    # Check response status and exit on failure
    if response.status_code == 200:
        print("Webhook sent successfully:", response)
    else:
        print(f"Webhook failed to send, Status Code: {response.status_code}, Response: {response.text}")
        sys.exit(1)  # Exit the program with an error


def send_github_sha1(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "sha1"
    url = SERVER_URL + "github_sha1"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha1')
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_github_sha256(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "sha256"
    url = SERVER_URL + "github_sha256"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha256')
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate sending Webhook messages")
    parser.add_argument("--secret", required=True, help="Secret key for generating signature")
    args = parser.parse_args()
    secret = args.secret
    # send data
    send_github_sha1(secret)
    send_github_sha256(secret)
