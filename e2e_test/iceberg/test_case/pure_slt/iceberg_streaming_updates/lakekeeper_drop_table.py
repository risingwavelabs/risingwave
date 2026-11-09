#!/usr/bin/env python3
"""Drop a table from the RiseDev Lakekeeper warehouse; a missing table is not an error."""

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request


def request(method, url):
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read()
        return json.loads(body) if body else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--warehouse", default="risingwave-warehouse")
    parser.add_argument("--namespace", default="public")
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    base = args.catalog_uri.rstrip("/")
    query = urllib.parse.urlencode({"warehouse": args.warehouse})
    config = request("GET", f"{base}/v1/config?{query}")
    settings = {**config.get("defaults", {}), **config.get("overrides", {})}
    prefix = settings.get("prefix", "").strip("/")
    api = f"{base}/v1" + (f"/{prefix}" if prefix else "")
    namespace = urllib.parse.quote(args.namespace, safe="")
    table = urllib.parse.quote(args.table, safe="")
    try:
        request("DELETE", f"{api}/namespaces/{namespace}/tables/{table}?purgeRequested=true")
    except urllib.error.HTTPError as error:
        # 404 covers both a missing table and a missing namespace.
        if error.code != 404:
            raise


if __name__ == "__main__":
    main()
