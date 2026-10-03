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

"""Checks the points written to Qdrant by qdrant_sink.slt."""

import json
import math
import sys
import urllib.request
import uuid


def request(url, api_key, path, body=None):
    request = urllib.request.Request(
        f"{url}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"api-key": api_key, "content-type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)["result"]


def points(url, api_key, collection):
    body = {"limit": 100, "with_payload": True, "with_vector": True}
    result = request(url, api_key, f"/collections/{collection}/points/scroll", body)
    return {str(p["id"]): p for p in result["points"]}


def close(actual, expected):
    return len(actual) == len(expected) and all(
        math.isclose(a, e, abs_tol=1e-6) for a, e in zip(actual, expected)
    )


def main():
    url, api_key = sys.argv[1:]

    unnamed = points(url, api_key, "rw_unnamed")
    assert sorted(unnamed) == ["1", "3", "4"], unnamed
    assert close(unnamed["1"]["vector"], [0.1, 0.2, 0.3]), unnamed["1"]
    assert unnamed["1"]["payload"] == {
        "id": 1,
        "body": "first",
        "price": 1.5,
        "published_at": "2026-06-16T01:02:03.000000Z",
    }, unnamed["1"]
    assert unnamed["3"]["payload"]["body"] == "third updated", unnamed["3"]
    assert close(unnamed["3"]["vector"], [0.9, 0.8, 0.7]), unnamed["3"]
    assert unnamed["4"]["payload"]["body"] == "fourth", unnamed["4"]

    named = points(url, api_key, "rw_named")
    key = lambda *columns: str(
        uuid.uuid5(uuid.NAMESPACE_OID, json.dumps(columns, separators=(",", ":")))
    )
    a, b = named[key("1", "a")], named[key("1", "b")]
    assert len(named) == 2, named
    assert close(a["vector"]["text"], [1, 0, 0]), a
    assert close(a["vector"]["image"], [0.5, 0.5]), a
    assert a["payload"] == {"tenant": 1, "doc": "a", "tags": ["x", "y"]}, a
    assert sorted(b["vector"]) == ["text"], b

    info = request(url, api_key, "/collections/rw_created")
    assert info["config"]["params"]["vectors"] == {"size": 2, "distance": "Dot"}, info
    created = points(url, api_key, "rw_created")
    assert sorted(created) == ["1", "2"], created
    assert close(created["2"]["vector"], [3, 4]), created

    print("qdrant sink check passed")


if __name__ == "__main__":
    main()
