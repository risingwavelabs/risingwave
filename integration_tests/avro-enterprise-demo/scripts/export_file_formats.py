from __future__ import annotations

"""Legacy helper for non-current-scope export formats.

This script is intentionally kept for internal experimentation / historical reference.
CSV, XML, and fixed-width outputs are not part of the current customer demo scope.
"""

import csv
import io
import json
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import Element, SubElement, tostring

import boto3


EXPORT_DIR = Path("/workspace/artifacts/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
PREFIX = "demo/customer_change_json/"


def read_records() -> list[dict[str, Any]]:
    client = boto3.client(
        "s3",
        endpoint_url="http://minio-0:9301",
        aws_access_key_id="hummockadmin",
        aws_secret_access_key="hummockadmin",
        region_name="custom",
    )
    paginator = client.get_paginator("list_objects_v2")
    records: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket="hummock001", Prefix=PREFIX):
        for item in page.get("Contents", []):
            body = client.get_object(Bucket="hummock001", Key=item["Key"])["Body"].read().decode("utf-8")
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
    return records


def flatten(record: dict[str, Any]) -> dict[str, str]:
    fields = [
        "customer_key",
        "action_type",
        "amount",
        "action_ts",
        "full_name",
        "status",
        "segment",
        "city",
    ]
    return {field: str(record.get(field, "")) for field in fields}


def write_csv(rows: list[dict[str, str]]) -> None:
    path = EXPORT_DIR / "customer_change_export.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: list[dict[str, str]]) -> None:
    (EXPORT_DIR / "customer_change_export.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


def write_xml(rows: list[dict[str, str]]) -> None:
    root = Element("customer_changes")
    for row in rows:
        item = SubElement(root, "change")
        for key, value in row.items():
            child = SubElement(item, key)
            child.text = value
    xml_bytes = tostring(root, encoding="utf-8")
    (EXPORT_DIR / "customer_change_export.xml").write_bytes(xml_bytes)


def write_fixed_width(rows: list[dict[str, str]]) -> None:
    specs = [
        ("customer_key", 12),
        ("action_type", 14),
        ("amount", 10),
        ("action_ts", 20),
        ("full_name", 18),
        ("status", 10),
        ("segment", 14),
        ("city", 16),
    ]
    output = io.StringIO()
    output.write("".join(name.ljust(width) for name, width in specs).rstrip() + "\n")
    for row in rows:
        output.write("".join(row[name][:width].ljust(width) for name, width in specs).rstrip() + "\n")
    (EXPORT_DIR / "customer_change_export.txt").write_text(output.getvalue(), encoding="utf-8")


def main() -> None:
    rows = [flatten(record) for record in read_records()]
    if not rows:
        raise RuntimeError("No records found in MinIO JSON sink prefix")
    write_csv(rows)
    write_json(rows)
    write_xml(rows)
    write_fixed_width(rows)


if __name__ == "__main__":
    main()
