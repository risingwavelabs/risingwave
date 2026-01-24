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

SERVER_URL = "http://127.0.0.1:4560/webhook/dev/public/"


def generate_signature_hmac(secret, payload, auth_algo, prefix):
    secret_bytes = bytes(secret, 'utf-8')
    payload_bytes = bytes(payload, 'utf-8')
    signature = ""
    if auth_algo == "sha1":
        signature = prefix + hmac.new(secret_bytes, payload_bytes, digestmod=hashlib.sha1).hexdigest()
    elif auth_algo == "sha256":
        signature = prefix + hmac.new(secret_bytes, payload_bytes, digestmod=hashlib.sha256).hexdigest()
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


def send_github_hmac_sha1(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "hmac_sha1"
    url = SERVER_URL + "github_hmac_sha1"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha1', "sha1=")
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_test_primary_key(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "hmac_sha1"
    url = SERVER_URL + "test_primary_key"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha1', "sha1=")
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_validate_raw_string(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "hmac_sha1"
    url = SERVER_URL + "validate_raw_string"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha1', "sha1=")
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_github_hmac_sha256(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "hmac_sha256"
    url = SERVER_URL + "github_hmac_sha256"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha256', "sha256=")
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_rudderstack(secret):
    # apply to both rudderstack, AWS EventBridge and HubSpot with API Key.
    payload = message
    payload['source'] = "rudderstack"
    payload['auth_algo'] = "plain"
    url = SERVER_URL + "rudderstack"

    payload_json = json.dumps(payload)
    signature = secret
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_segment_hmac_sha1(secret):
    payload = message
    payload['source'] = "segment"
    payload['auth_algo'] = "hmac_sha1"
    url = SERVER_URL + "segment_hmac_sha1"

    payload_json = json.dumps(payload)
    signature = generate_signature_hmac(secret, payload_json, 'sha1', '')
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "x-signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)


def send_hubspot_sha256_v2(secret):
    payload = message
    payload['source'] = "hubspot"
    payload['auth_algo'] = "sha256_v2"
    url = SERVER_URL + "hubspot_sha256_v2"

    payload_json = json.dumps(payload)
    payload = secret + 'POST' + url + str(payload_json)
    signature = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "x-hubspot-signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_json)

def send_batched(secret):
    payload = message
    payload['source'] = "github"
    payload['auth_algo'] = "hmac_sha1"
    url = SERVER_URL + "batched"

    payload_jsonl = '\n'.join([json.dumps(payload) for i in range(10)])

    signature = generate_signature_hmac(secret, payload_jsonl, 'sha1', "sha1=")
    # Webhook message headers
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature": signature  # Custom signature header
    }
    send_webhook(url, headers, payload_jsonl)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate sending Webhook messages")
    parser.add_argument("--secret", required=True, help="Secret key for generating signature")
    args = parser.parse_args()
    secret = args.secret
    # send data
    # github
    send_github_hmac_sha1(secret)
    send_github_hmac_sha256(secret)
    # rudderstack, AWS EventBridge and HubSpot with API Key.
    send_rudderstack(secret)
    # segment
    send_segment_hmac_sha1(secret)
    # hubspot
    send_hubspot_sha256_v2(secret)

    # ensure the single column can still work as normal
    send_test_primary_key(secret)
    # check using raw string to verify the signature
    send_validate_raw_string(secret)
    # batch multiple rows per request
    send_batched(secret)
