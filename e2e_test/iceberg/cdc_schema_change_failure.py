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

"""CDC schema changes must not turn an Iceberg sink outage into a database stall.

Run with the ci-iceberg-test profile and the PostgreSQL CDC fixture. Only this
sink's MinIO endpoint goes through the proxy; Hummock and other jobs are unaffected.
"""

import os
import select
import socket
import socketserver
import subprocess
import sys
import threading
import time
import uuid


class StorageProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, upstream):
        self.upstream = upstream
        self.failed = threading.Event()
        super().__init__(("127.0.0.1", 0), StorageRelay)


class StorageRelay(socketserver.BaseRequestHandler):
    def handle(self):
        proxy = self.server
        if proxy.failed.is_set():
            return
        try:
            with socket.create_connection(proxy.upstream, timeout=5) as upstream:
                # Forward bytes unchanged, including Host and the S3 signature.
                sockets = (self.request, upstream)
                for connection in sockets:
                    connection.settimeout(1)
                while not proxy.failed.is_set():
                    ready, _, _ = select.select(sockets, [], [], 0.1)
                    for connection in ready:
                        data = connection.recv(65536)
                        if not data:
                            return
                        peer = upstream if connection is self.request else self.request
                        peer.sendall(data)
        except OSError:
            # Closing both existing and new connections models a sink-only outage.
            return


def postgres_connection():
    return {
        "host": os.environ.get("CDC_PGHOST", "db"),
        "port": os.environ.get("CDC_PGPORT", "5432"),
        "user": os.environ.get("CDC_PGUSER", "postgres"),
        "database": os.environ.get("CDC_PGDATABASE", "postgres"),
        "password": os.environ.get("CDC_PGPASSWORD", "post\\tgres"),
    }


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def sql(statement, *, upstream=False):
    env = os.environ.copy()
    if upstream:
        connection = postgres_connection()
        host, port = connection["host"], connection["port"]
        user, database = connection["user"], connection["database"]
        env["PGPASSWORD"] = connection["password"]
    else:
        host = env.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS", "127.0.0.1")
        port = env.get("RISEDEV_RW_FRONTEND_PORT", "4566")
        user = "root"
        database = "dev"
        env.pop("PGPASSWORD", None)
    result = subprocess.run(
        [
            "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
            "-h", host, "-p", port, "-U", user, "-d", database,
        ],
        input=statement,
        text=True,
        capture_output=True,
        env=env,
        timeout=40,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def wait_for(check, description, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except RuntimeError as error:
            last_error = error
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {description}: {last_error}")


def run():
    postgres = {key: literal(value) for key, value in postgres_connection().items()}
    suffix = uuid.uuid4().hex[:8]
    table = f"cdc_isolation_{suffix}"
    source = f"{table}_source"
    sink = f"{table}_sink"
    independent = f"{table}_input"
    mv = f"{table}_mv"
    iceberg_source = f"{table}_iceberg"
    with StorageProxy(("127.0.0.1", 9301)) as proxy:
        server_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        server_thread.start()
        endpoint = f"http://127.0.0.1:{proxy.server_address[1]}"
        catalog = f"""
            connector = 'iceberg', catalog.type = 'storage',
            warehouse.path = 's3a://hummock001/iceberg-isolation',
            database.name = 'public', table.name = '{table}',
            s3.endpoint = '{endpoint}', s3.region = 'custom',
            s3.access.key = 'hummockadmin', s3.secret.key = 'hummockadmin',
            s3.path.style.access = 'true'
        """
        try:
            sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, v1 INT);", upstream=True)
            sql(f"""
                CREATE SOURCE {source} WITH (
                    connector = 'postgres-cdc', hostname = {postgres["host"]},
                    port = {postgres["port"]}, username = {postgres["user"]},
                    password = {postgres["password"]},
                    database.name = {postgres["database"]}, schema.name = 'public',
                    slot.name = '{table}', auto.schema.change = 'true'
                );
                CREATE TABLE {table} (id INT PRIMARY KEY, v1 INT)
                    FROM {source} TABLE 'public.{table}';
                CREATE TABLE {independent} (id INT PRIMARY KEY);
                CREATE MATERIALIZED VIEW {mv} AS SELECT count(*) AS n FROM {independent};
                SET sink_decouple = true;
                CREATE SINK {sink} FROM {table} WITH (
                    {catalog}, catalog.name = 'iceberg', type = 'append-only',
                    force_append_only = 'true', primary_key = 'id',
                    auto.schema.change = 'true', is_exactly_once = 'true',
                    create_table_if_not_exists = 'true', commit_checkpoint_interval = 1
                );
            """)
            sql(f"INSERT INTO {table} VALUES (1, 10);", upstream=True)

            def iceberg_rows(expected):
                # Recreate the source to load the current downstream schema as well as its rows.
                sql(f"DROP SOURCE IF EXISTS {iceberg_source};")
                sql(f"CREATE SOURCE {iceberg_source} WITH ({catalog});")
                actual = sql(f"SELECT * FROM {iceberg_source} ORDER BY id;")
                if actual != expected:
                    raise RuntimeError(f"Unexpected Iceberg rows: {actual!r}")
                return True

            wait_for(lambda: iceberg_rows("1|10"), "initial Iceberg commit")
            proxy.failed.set()
            sql(f"INSERT INTO {table} VALUES (2, 20);", upstream=True)

            def error_count():
                return int(sql(f"""
                    SELECT count(*) FROM rw_catalog.rw_event_logs
                    WHERE event_type = 'SINK_FAIL' AND info::text LIKE '%{sink}%';
                """))

            wait_for(lambda: error_count() > 0, "sink to fail before the schema change")
            initial_errors = error_count()
            print("Sink outage is active", flush=True)

            def committed_epoch():
                return int(sql(f"""
                    SELECT committed_epoch FROM rw_catalog.rw_hummock_snapshot
                    WHERE table_id = '{mv}'::regclass;
                """))

            count = 0
            for column, values in (
                ("v2", "(3, 30, 300)"),
                ("v3", "(4, 40, 400, 4000)"),
            ):
                # PostgreSQL emits the new logical-replication relation schema with DML.
                # Retain a row from each schema version while Iceberg remains unavailable.
                sql(
                    f"ALTER TABLE {table} ADD COLUMN {column} INT; "
                    f"INSERT INTO {table} VALUES {values};",
                    upstream=True,
                )
                wait_for(
                    lambda: sql(f"""
                        SELECT count(*) FROM information_schema.columns
                        WHERE table_name = '{table}' AND column_name = '{column}';
                    """) == "1",
                    f"CDC to add {column} while the sink is failing",
                )
                before = committed_epoch()
                for _ in range(3):
                    count += 1
                    sql(f"INSERT INTO {independent} VALUES ({count}); FLUSH;")
                    after = committed_epoch()
                    assert after > before, "unrelated MV checkpoint stalled"
                    assert sql(f"SELECT n FROM {mv};") == str(count)
                    before = after
                assert proxy.failed.is_set()
                print(f"CDC added {column}; unrelated MV completed {count} checkpoints", flush=True)

            # The outage remains in place across both schema changes and all six checkpoints.
            wait_for(lambda: error_count() > initial_errors, "sink to keep failing")
            sql(
                f"INSERT INTO {table} (id, v1, v2, v3) VALUES (5, 50, 500, 5000);",
                upstream=True,
            )
            proxy.failed.clear()
            print("Storage restored; waiting for all retained schema versions", flush=True)
            wait_for(
                lambda: iceberg_rows(
                    "1|10||\n2|20||\n3|30|300|\n4|40|400|4000\n5|50|500|5000"
                ),
                "all schema versions to drain into Iceberg without losing or duplicating rows",
                timeout=180,
            )
        finally:
            proxy.failed.clear()
            # Cleanup must not hide the original failure or leave fixed-name external resources.
            original_failure = sys.exc_info()[0] is not None
            cleanup_errors = []
            for statement in (
                f"DROP SOURCE IF EXISTS {iceberg_source};",
                f"DROP SINK IF EXISTS {sink};",
                f"DROP TABLE IF EXISTS {table};",
                f"DROP SOURCE IF EXISTS {source};",
                f"DROP MATERIALIZED VIEW IF EXISTS {mv};",
                f"DROP TABLE IF EXISTS {independent};",
            ):
                try:
                    sql(statement)
                except (RuntimeError, subprocess.TimeoutExpired) as error:
                    cleanup_errors.append(str(error))
            try:
                sql(f"DROP TABLE IF EXISTS {table};", upstream=True)

                def remove_slot():
                    # DROP SOURCE returns before Debezium necessarily releases its connection.
                    sql(
                        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                        f"WHERE slot_name = '{table}' AND NOT active;",
                        upstream=True,
                    )
                    return sql(
                        "SELECT count(*) FROM pg_replication_slots "
                        f"WHERE slot_name = '{table}';",
                        upstream=True,
                    ) == "0"

                wait_for(remove_slot, "PostgreSQL replication slot cleanup", timeout=30)
            except (RuntimeError, subprocess.TimeoutExpired, AssertionError) as error:
                cleanup_errors.append(str(error))
            proxy.shutdown()
            server_thread.join(timeout=5)
            if cleanup_errors:
                if not original_failure:
                    raise RuntimeError(f"Fixture cleanup failed: {cleanup_errors}")
                print(f"Fixture cleanup also failed: {cleanup_errors}", file=sys.stderr)
    print("CDC schema change failure isolation passed")


if __name__ == "__main__":
    run()
