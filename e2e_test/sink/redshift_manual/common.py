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

"""Shared utilities for explicitly enabled live Redshift tests."""

import os
import re
import time
from urllib.parse import quote_plus, urlsplit


def require_opt_in():
    if os.environ.get("REDSHIFT_E2E_RUN") != "1":
        raise RuntimeError("Live Redshift tests require run.py --run-live")
    if any(
        os.environ.get(key, "").lower() not in ("", "0", "false")
        for key in ("CI", "BUILDKITE", "GITHUB_ACTIONS")
    ):
        raise RuntimeError("Live Redshift tests are disabled in CI")


def config(mode):
    require_opt_in()
    keys = [
        "REDSHIFT_JDBC_URL",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
        "REDSHIFT_SCHEMA",
    ]
    if mode == "s3":
        keys += [
            "S3_BUCKET",
            "S3_PREFIX",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
    missing = [key for key in keys if not os.environ.get(key)]
    if missing:
        raise ValueError("Missing environment variables: " + ", ".join(missing))
    result = {key: os.environ[key] for key in keys}
    url = urlsplit(result["REDSHIFT_JDBC_URL"].removeprefix("jdbc:"))
    if url.scheme != "redshift" or not url.hostname or not url.path.strip("/"):
        raise ValueError("REDSHIFT_JDBC_URL must contain a Redshift host and database")
    if url.query or url.username or url.password or url.fragment:
        raise ValueError(
            "Provide a JDBC URL without credentials, query parameters, or fragments"
        )
    if mode == "s3" and not result["S3_PREFIX"].strip("/"):
        raise ValueError("S3_PREFIX must be a nonempty dedicated test prefix")
    return result


def redact(text):
    values = [
        value
        for key, value in os.environ.items()
        if key.startswith(("REDSHIFT_", "AWS_", "S3_"))
        and not key.startswith("REDSHIFT_E2E_")
        and value
    ]
    url = urlsplit(os.environ.get("REDSHIFT_JDBC_URL", "").removeprefix("jdbc:"))
    values += [url.hostname or "", url.path.strip("/")]
    values += [quote_plus(value) for value in values]
    for value in sorted(set(values), key=len, reverse=True):
        if value:
            # Short database/schema names should not redact unrelated words
            # such as "development" when the database is named "dev".
            if len(value) < 8:
                text = re.sub(
                    r"(?<!\w)" + re.escape(value) + r"(?!\w)", "[redacted]", text
                )
            else:
                text = text.replace(value, "[redacted]")
    return text


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def redshift(cfg):
    import psycopg2

    url = urlsplit(cfg["REDSHIFT_JDBC_URL"].removeprefix("jdbc:"))
    return psycopg2.connect(
        host=url.hostname,
        port=url.port or 5439,
        dbname=url.path.strip("/"),
        user=cfg["REDSHIFT_USER"],
        password=cfg["REDSHIFT_PASSWORD"],
        sslmode="require",
        connect_timeout=15,
    )


def s3_client(cfg):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=cfg["AWS_REGION"],
        aws_access_key_id=cfg["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=cfg["AWS_SECRET_ACCESS_KEY"],
        config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}),
    )


def s3_objects(client, cfg, prefix):
    return [
        obj
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=cfg["S3_BUCKET"], Prefix=prefix + "/"
        )
        for obj in page.get("Contents", [])
    ]


def clean_s3(client, cfg, prefix):
    objects = s3_objects(client, cfg, prefix)
    for start in range(0, len(objects), 1000):
        result = client.delete_objects(
            Bucket=cfg["S3_BUCKET"],
            Delete={
                "Objects": [
                    {"Key": obj["Key"]} for obj in objects[start : start + 1000]
                ],
                "Quiet": True,
            },
        )
        if result.get("Errors"):
            raise RuntimeError("S3 cleanup failed for some test objects")
    assert not s3_objects(client, cfg, prefix), "S3 test objects remain"


def wait_for(read, expected, label, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if read() == expected:
            print("PASS " + label, flush=True)
            return
        time.sleep(2)
    raise AssertionError(label + " did not converge within the timeout")
