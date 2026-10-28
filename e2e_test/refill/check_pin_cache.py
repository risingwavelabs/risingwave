#!/usr/bin/env python3

"""Check Pin publication and withdrawal in the real-process refill profile."""

import argparse
import os
from pathlib import Path
import re
import sys
import time
import urllib.request

import psycopg2
import yaml


TABLES = ("mt_pin_a", "mt_pin_b", "mt_ordinary")


def worker_state(node, data_dir, pinned_ids):
    url = f"http://{node['address']}:{node['exporter-port']}/metrics"
    with urllib.request.urlopen(url, timeout=5) as response:
        lines = response.read().decode().splitlines()
    metrics = {}
    local_hits = 0
    for line in lines:
        if line.startswith("pin_cache_"):
            key, value = line.rsplit(" ", 1)
            metrics[key] = float(value)
        elif line.startswith("state_store_sst_store_block_request_counts"):
            table = re.search(r'table_id="(\d+)"', line)
            if table is None or int(table[1]) not in pinned_ids:
                continue
            if 'type="pin_data_hit"' in line:
                local_hits += int(line.rsplit(" ", 1)[1])
            elif 'type="pin_data_memory_hit"' in line:
                local_hits -= int(line.rsplit(" ", 1)[1])
    directory = data_dir / "pin-cache" / str(node["port"]) / "pinned_ssts"
    files = list(directory.glob("*.sst"))
    objects = {int(path.name.split("-", 1)[0]) for path in files}
    errors = []
    if metrics.get("pin_cache_published_objects") != len(files):
        errors.append(f"{node['id']}: publication/file counts differ")
    if metrics.get('pin_cache_capacity_bytes{state="accounted"}') != sum(
        path.stat().st_size for path in files
    ):
        errors.append(f"{node['id']}: physical reclamation has not finished")
    return objects, local_hits, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-objects", help="SST object IDs observed before this write round")
    parser.add_argument("pinned_tables", nargs="*")
    args = parser.parse_args()
    previous = (
        {int(oid) for oid in args.previous_objects.split(",") if oid}
        if args.previous_objects is not None
        else None
    )
    prefix = Path(os.environ.get("PREFIX", ".risingwave"))
    config_dir = Path(os.environ.get("PREFIX_CONFIG", prefix / "config"))
    data_dir = Path(os.environ.get("PREFIX_DATA", prefix / "data"))
    profile = yaml.safe_load((config_dir / "risedev-expanded.yml").read_text())
    workers = [node for node in profile if node["use"] == "compute-node"]
    if not workers or any(not node["enable-pin-cache"] for node in workers):
        raise RuntimeError("the refill profile must enable Pin Cache on every CN")

    with psycopg2.connect(
        host=os.environ.get("SLT_HOST", "127.0.0.1"),
        port=os.environ.get("SLT_PORT", "4566"),
        dbname=os.environ.get("SLT_DB", "dev"),
        user="root",
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM rw_tables WHERE name = ANY(%s)", (list(TABLES),))
            ids = {name: int(table_id) for table_id, name in cursor.fetchall()}
        if set(ids) != (set(TABLES) if args.pinned_tables else set()):
            raise RuntimeError(f"unexpected fixture tables: {ids}")
        pinned_ids = {ids[name] for name in args.pinned_tables}
        deadline = time.monotonic() + 30
        while True:
            errors = []
            with conn.cursor() as cursor:
                cursor.execute("SELECT object_id, table_ids FROM rw_hummock_sstables")
                objects = {}
                for object_id, tables in cursor.fetchall():
                    # Logical projections in different groups can share a physical object.
                    objects.setdefault(int(object_id), set()).update(tables)
            for name, table_id in ids.items():
                current = {oid for oid, tables in objects.items() if table_id in tables}
                if not current:
                    errors.append(f"{name}: no SST objects")
                elif previous is not None and not current - previous:
                    errors.append(f"{name}: no new SST objects after this write round")
            expected = {oid for oid, tables in objects.items() if tables & pinned_ids}
            owned = {"streaming": set(), "serving": set()}
            local_hits = 0
            for node in workers:
                try:
                    actual, hits, pending = worker_state(node, data_dir, pinned_ids)
                    errors.extend(pending)
                    if actual - expected:
                        errors.append(f"{node['id']}: unexpected Pin objects {actual - expected}")
                    for role in owned:
                        if node["role"] in (role, "both"):
                            owned[role].update(actual)
                    local_hits += hits
                except OSError as error:
                    errors.append(f"{node['id']}: {error}")
            for role, actual in owned.items():
                if actual != expected:
                    errors.append(f"{role}: Pin objects {actual}, expected {expected}")
            if expected and local_hits <= 0:
                errors.append("no Pin file reads observed")
            if not errors:
                print("Pin cache state converged")
                return 0
            if time.monotonic() >= deadline:
                print("\n".join(errors), file=sys.stderr)
                return 1
            time.sleep(0.2)


if __name__ == "__main__":
    sys.exit(main())
