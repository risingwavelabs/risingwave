#!/usr/bin/env python3
"""Test-only catalog fixture for streaming_updates.slt; never rewrites snapshots."""

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


def request(method, url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read()
        return json.loads(body) if body else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "verify", "drop"])
    args = parser.parse_args()
    name = os.environ["ICEBERG_TEST_TABLE"]
    # Never mutate an existing application table, even if the environment is wrong.
    if not re.fullmatch(r"rw_update_e2e_[0-9a-f]{32}", name):
        raise ValueError("ICEBERG_TEST_TABLE must have a fresh rw_update_e2e_<uuid> name")
    version = int(os.environ["ICEBERG_TEST_FORMAT_VERSION"])
    if version not in (2, 3):
        raise ValueError("only Iceberg V2/V3 are supported")
    base = os.environ["ICEBERG_TEST_CATALOG_URI"].rstrip("/")
    warehouse = os.environ["ICEBERG_TEST_WAREHOUSE"]
    config = request("GET", base + "/v1/config?" + urllib.parse.urlencode({"warehouse": warehouse}))
    settings = {**config.get("defaults", {}), **config.get("overrides", {})}
    prefix = settings.get("prefix", "")
    api = base + "/v1" + ("/" + prefix.strip("/") if prefix else "")
    namespace = "public"
    tables = f"{api}/namespaces/{namespace}/tables"
    table = f"{tables}/{name}"
    if args.action == "prepare":
        try:
            request("POST", api + "/namespaces", {"namespace": [namespace]})
        except urllib.error.HTTPError as error:
            if error.code != 409:
                raise
        # The exact test writer key is input.id. Publishing here is a fixture,
        # not production rollout. CREATE SINK subsequently validates this key.
        request(
            "POST",
            tables,
            {
                "name": name,
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": False, "type": "int"},
                        {"id": 2, "name": "label", "required": False, "type": "string"},
                        {"id": 3, "name": "v", "required": False, "type": "long"},
                    ],
                },
                "properties": {
                    "format-version": str(version),
                    "risingwave.source-contract.version": "1",
                    "risingwave.source-contract.writer": "pk-index",
                    "risingwave.source-contract.key-field-ids": "[1]",
                },
            },
        )
        metadata = request("GET", table)["metadata"]
        assert metadata["format-version"] == version
        assert not metadata.get("snapshots"), "fixture must be empty before the real writer starts"
        assert metadata["properties"]["risingwave.source-contract.key-field-ids"] == "[1]"
    elif args.action == "verify":
        metadata = request("GET", table)["metadata"]
        snapshots = metadata["snapshots"]
        assert len(snapshots) >= 4, "bootstrap/update/delete/reinsert must reach separate commits"
        assert metadata["format-version"] == version
        for snapshot in snapshots:
            summary = snapshot["summary"]
            assert summary.get("risingwave.source-contract.version") == "1"
            assert summary.get("risingwave.commit.kind") == "data", "compaction is outside this test"
    else:
        request("DELETE", table + "?purgeRequested=true")


if __name__ == "__main__":
    main()
