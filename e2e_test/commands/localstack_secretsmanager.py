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
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


CONTAINER_NAME = "rw-localstack-secretsmanager-slt"
IMAGE = "localstack/localstack:3.0"
PORT = 14566
ENDPOINT = f"http://127.0.0.1:{PORT}"
REGION = "us-east-1"


def run(cmd, *, check=True):
    return subprocess.run(
        cmd, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


def secretsmanager_request(target, payload):
    body = json.dumps(payload).encode()
    request = urllib.request.Request(
        ENDPOINT,
        data=body,
        headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": f"secretsmanager.{target}",
            "X-Amz-Date": "20260101T000000Z",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            response_body = response.read()
            if not response_body:
                return {}
            return json.loads(response_body.decode())
    except urllib.error.HTTPError as e:
        response_body = e.read().decode(errors="replace")
        raise RuntimeError(
            f"LocalStack Secrets Manager {target} failed with HTTP {e.code}: {response_body}"
        ) from e


def wait_until_ready():
    deadline = time.time() + 60
    last_error = None
    while time.time() < deadline:
        try:
            secretsmanager_request("ListSecrets", {})
            return
        except Exception as e:
            last_error = e
            time.sleep(1)
    raise RuntimeError(f"LocalStack Secrets Manager did not become ready: {last_error}")


def start(_args):
    run(["docker", "rm", "-f", CONTAINER_NAME], check=False)
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            f"{PORT}:4566",
            "-e",
            "SERVICES=secretsmanager",
            "-e",
            f"AWS_DEFAULT_REGION={REGION}",
            IMAGE,
        ]
    )
    wait_until_ready()


def stop(_args):
    run(["docker", "rm", "-f", CONTAINER_NAME], check=False)


def put(args):
    try:
        secretsmanager_request(
            "CreateSecret",
            {
                "Name": args.name,
                "SecretString": args.secret_string,
                "ClientRequestToken": str(uuid.uuid4()),
            },
        )
    except RuntimeError as e:
        if "ResourceExistsException" not in str(e):
            raise
        secretsmanager_request(
            "PutSecretValue",
            {
                "SecretId": args.name,
                "SecretString": args.secret_string,
                "ClientRequestToken": str(uuid.uuid4()),
            },
        )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start")
    start_parser.set_defaults(func=start)

    stop_parser = subparsers.add_parser("stop")
    stop_parser.set_defaults(func=stop)

    put_parser = subparsers.add_parser("put")
    put_parser.add_argument("--name", required=True)
    put_parser.add_argument("--secret-string", required=True)
    put_parser.set_defaults(func=put)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
