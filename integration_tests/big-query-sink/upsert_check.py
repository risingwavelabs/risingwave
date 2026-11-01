"""BigQuery primary-key regression checks, invoked by sink_check.py."""

import json
import os
import subprocess
import time
import uuid
from util import (
    DATASET,
    DEMO_DIR,
    bq,
    bq_execute,
    bq_query,
    drop_bq_table,
    require_success,
)


def rw_query(sql):
    return subprocess.run(
        [
            "psql",
            "-X",
            "-h",
            "localhost",
            "-p",
            "4566",
            "-d",
            "dev",
            "-U",
            "root",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ],
        cwd=DEMO_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )


def wait_for_rows(target, expected):
    deadline = time.monotonic() + int(os.getenv("BQ_UPSERT_TIMEOUT_SECONDS", "300"))
    actual = None
    while time.monotonic() < deadline:
        rows = bq_query(
            f"SELECT id, tenant_id, name FROM `{target}` ORDER BY tenant_id, id",
            timeout=max(1, deadline - time.monotonic()),
        )
        # The bq CLI represents INT64 values as JSON strings.
        actual = [(int(row["id"]), int(row["tenant_id"]), row["name"]) for row in rows]
        if actual == expected:
            return
        time.sleep(min(5, max(0, deadline - time.monotonic())))
    raise AssertionError(
        f"Timed out waiting for {target}: expected {expected}, got {actual}"
    )


def check_case(
    label, existing_pk=None, auto_create=True, append_only=False, reject=False
):
    # Separate objects allow retries without deleting another run's fixtures.
    name = f"bq_pk_{label}_{uuid.uuid4().hex[:12]}"
    source = f"{name}_source"
    sink = f"{name}_sink"
    target = f"{DATASET}.{name}"
    print(f"Checking BigQuery {label}: {target}", flush=True)
    failed = False
    try:
        require_success(
            rw_query(
                f"CREATE TABLE {source} (id BIGINT, tenant_id BIGINT, name VARCHAR, "
                "PRIMARY KEY (id, tenant_id))"
            )
        )
        if existing_pk is not None:
            constraint = (
                f", PRIMARY KEY ({existing_pk}) NOT ENFORCED" if existing_pk else ""
            )
            bq_execute(
                f"CREATE TABLE `{target}` (id INT64, tenant_id INT64, name STRING{constraint})"
            )

        sink_type = "append-only" if append_only else "upsert"
        mode_options = (
            "force_append_only = 'true'"
            if append_only
            else "primary_key = 'id,tenant_id'"
        )
        result = rw_query(f"""
            CREATE SINK {sink} AS SELECT id, tenant_id, name FROM {source}
            WITH (
                connector = 'bigquery', type = '{sink_type}', {mode_options},
                bigquery.project = 'rwctest', bigquery.dataset = 'bqtest',
                bigquery.table = '{name}', bigquery.local.path = '/gcp-rwctest.json',
                auto_create = '{str(auto_create).lower()}'
            )
        """)
        if reject:
            if result.returncode == 0:
                raise AssertionError(
                    f"CREATE SINK unexpectedly accepted invalid primary key on {target}"
                )
            # Authentication, schema, and network failures must not pass this check.
            for message in (
                target,
                "primary key does not match",
                "expected",
                "actual",
                "PRIMARY KEY (...) NOT ENFORCED",
            ):
                if message not in result.stderr:
                    raise AssertionError(
                        f"Expected {message!r} in DDL error: {result.stderr}"
                    )
            return
        require_success(result)

        metadata = json.loads(bq("show", "--format=json", target.replace(".", ":", 1)))
        constraints = metadata.get("tableConstraints") or {}
        primary_key = constraints.get("primaryKey") or {}
        columns = primary_key.get("columns", [])
        expected_columns = [] if append_only else ["id", "tenant_id"]
        if sorted(columns) != expected_columns:
            raise AssertionError(f"Unexpected primary key for {target}: {columns}")

        # Repeated IDs in different tenants exercise the complete composite key.
        require_success(
            rw_query(f"INSERT INTO {source} VALUES (1, 10, 'alice'), (1, 20, 'bob')")
        )
        wait_for_rows(target, [(1, 10, "alice"), (1, 20, "bob")])
        if not append_only:
            require_success(
                rw_query(
                    f"UPDATE {source} SET name = 'alice_updated' WHERE id = 1 AND tenant_id = 10"
                )
            )
            wait_for_rows(target, [(1, 10, "alice_updated"), (1, 20, "bob")])
            require_success(
                rw_query(f"DELETE FROM {source} WHERE id = 1 AND tenant_id = 10")
            )
            wait_for_rows(target, [(1, 20, "bob")])
    except BaseException:
        failed = True
        raise
    finally:
        # Stop the writer before deleting its destination. Attempt every cleanup
        # even if one fails, while preserving the original regression failure.
        cleanup_errors = []
        for sql in (f"DROP SINK IF EXISTS {sink}", f"DROP TABLE IF EXISTS {source}"):
            try:
                require_success(rw_query(sql))
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            drop_bq_table(target)
        except Exception as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            message = "Cleanup failed:\n" + "\n".join(cleanup_errors)
            if failed:
                print(message, flush=True)
            else:
                raise RuntimeError(message)


def upsert_checks():
    # Auto-created upsert tables need a composite primary key.
    check_case("auto")
    # Validate existing keys with either auto_create setting.
    for auto_create in (False, True):
        suffix = str(auto_create).lower()
        # Key column order does not matter.
        check_case(
            f"existing_{suffix}", existing_pk="tenant_id, id", auto_create=auto_create
        )
        # Reject missing keys at DDL time.
        check_case(
            f"missing_{suffix}", existing_pk="", auto_create=auto_create, reject=True
        )
        # Reject incomplete keys.
        check_case(
            f"mismatch_{suffix}", existing_pk="id", auto_create=auto_create, reject=True
        )
    # Append-only tables need no primary key.
    check_case("append", append_only=True)


if __name__ == "__main__":
    upsert_checks()
