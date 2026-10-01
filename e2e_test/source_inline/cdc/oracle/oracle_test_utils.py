#!/usr/bin/env python3

"""Oracle fixtures and composable SQL, transaction, and backfill operations.

SLTs own fixture SQL and teardown, and attach hooks at named operation boundaries.
The CLI exposes preparation, table cleanup, and the transaction worker.
Functions are ordered as public APIs, shared helpers (_), then private helpers (__).
Single-caller private helpers are defined inside their caller.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import oracledb

# Contract: keep these TEST_ mappings and fixture constants synchronized with
# oracle_test_env.slt.part. Fixture names must not be added to service profiles.
TEST_ORACLE_HOST = os.environ["ORACLE_HOST"]
TEST_ORACLE_PORT = int(os.environ["ORACLE_PORT"])
TEST_ORACLE_USER = os.environ["ORACLE_USER"].upper()
TEST_ORACLE_PASSWORD = os.environ["ORACLE_PASSWORD"]
TEST_ORACLE_DATABASE = os.environ["ORACLE_DATABASE"].upper()
TEST_ORACLE_PDB = os.environ["ORACLE_PDB"].upper()
TEST_ORACLE_SOURCE_SCHEMA = "APP"
ORACLE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
TABLE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
DDL_STATEMENT_TIMEOUT_SECONDS = 120
PSQL_PROCESS_TIMEOUT_SECONDS = 150
CHECKPOINT_QUERY_TIMEOUT_SECONDS = 30
TRANSACTION_TIMEOUT_SECONDS = 300
BACKFILL_RESUME_RATE = 1000


@dataclass
class SqlOutcome:
    sql: str = ""
    result: subprocess.CompletedProcess[str] | None = None
    exception: Exception | None = None


@dataclass
class HookContext:
    outcomes: list[SqlOutcome] = field(default_factory=list)
    table: str | None = None
    transaction: dict | None = None


# Public APIs.
def prepare(
    *,
    create_schema_hook=None,
    create_schema_hook_kwargs=None,
    create_heartbeat_tables_hook=None,
    create_heartbeat_tables_hook_kwargs=None,
) -> None:
    """Set up Oracle infrastructure; omitted creation hooks use their defaults."""
    from oracle_test_hooks import (
        create_default_schema,
        create_default_heartbeat_tables,
    )

    def __prepare_base() -> None:
        """Provision logging and the connector user/schema, not source tables."""

        def __wait_for_database(service: str):
            deadline = time.monotonic() + 600
            while True:
                try:
                    return _connect_as_sys(service)
                except oracledb.DatabaseError as error:
                    if time.monotonic() >= deadline:
                        raise
                    print(
                        f"Waiting for Oracle service {service}: {error}",
                        file=sys.stderr,
                    )
                    time.sleep(5)

        def __execute_ignoring(cursor, sql: str, ignored_codes: set[int]) -> None:
            try:
                cursor.execute(sql)
            except oracledb.DatabaseError as error:
                if _error_code(error) not in ignored_codes:
                    raise

        user = TEST_ORACLE_USER
        password = TEST_ORACLE_PASSWORD.replace('"', '""')
        with __wait_for_database(TEST_ORACLE_DATABASE) as connection:
            with connection.cursor() as cursor:
                __execute_ignoring(cursor, "ALTER DATABASE FORCE LOGGING", {12920})
                __execute_ignoring(
                    cursor, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA", {32588}
                )
                try:
                    cursor.execute(
                        f'CREATE USER {user} IDENTIFIED BY "{password}" CONTAINER=ALL'
                    )
                except oracledb.DatabaseError as error:
                    if _error_code(error) != 1920:
                        raise
                    cursor.execute(
                        f'ALTER USER {user} IDENTIFIED BY "{password}" CONTAINER=ALL'
                    )
                cursor.execute(
                    "GRANT CREATE SESSION, SET CONTAINER, FLASHBACK ANY TABLE, "
                    "SELECT ANY TABLE, SELECT ANY TRANSACTION, LOGMINING, LOCK ANY TABLE, "
                    f"CREATE TABLE, CREATE SEQUENCE TO {user} CONTAINER=ALL"
                )
                cursor.execute(
                    "GRANT SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE "
                    f"TO {user} CONTAINER=ALL"
                )
        with __wait_for_database(TEST_ORACLE_PDB) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"ALTER USER {user} QUOTA UNLIMITED ON USERS")

    outcome = SqlOutcome()
    try:
        __prepare_base()
        drop_tables()
    except Exception as error:
        outcome.exception = error
    context = HookContext([outcome])
    __invoke_hook(
        create_schema_hook or create_default_schema,
        context,
        create_schema_hook_kwargs,
    )
    __invoke_hook(
        create_heartbeat_tables_hook or create_default_heartbeat_tables,
        context,
        create_heartbeat_tables_hook_kwargs,
    )


def drop_tables() -> None:
    """Discover and drop existing tables in the two dedicated test schemas."""
    with _connect_as_sys(TEST_ORACLE_PDB) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES "
                "WHERE OWNER IN (:1, :2) AND DROPPED = 'NO' "
                "ORDER BY OWNER, TABLE_NAME",
                [TEST_ORACLE_USER, TEST_ORACLE_SOURCE_SCHEMA],
            )
            for owner, table in cursor.fetchall():
                table = table.replace('"', '""')
                cursor.execute(
                    f'DROP TABLE "{owner}"."{table}" CASCADE CONSTRAINTS PURGE'
                )


def execute_oracle_sqls(
    sqls: list[str],
    *,
    after_oracle_sqls_hook=None,
    after_oracle_sqls_hook_kwargs=None,
) -> None:
    """Execute and commit in order, then invoke the optional success hook."""
    with _connect_as_sys(TEST_ORACLE_PDB) as connection:
        with connection.cursor() as cursor:
            for sql in sqls:
                cursor.execute(sql)
        connection.commit()
    __invoke_hook(
        after_oracle_sqls_hook,
        HookContext([SqlOutcome(sql=sql) for sql in sqls]),
        after_oracle_sqls_hook_kwargs,
    )


def execute_sqls(
    sqls: list[str],
    is_concurrent=False,
    *,
    after_sqls_hook=None,
    after_sqls_hook_kwargs=None,
) -> None:
    outcomes = __run_each(sqls, __sql_outcome, is_concurrent)
    __invoke_hook(after_sqls_hook, HookContext(outcomes), after_sqls_hook_kwargs)


def begin_tx(
    name: str,
    dmls: list,
    *,
    after_tx_begin_hook=None,
    after_tx_begin_hook_kwargs=None,
) -> None:
    """Hold explicit DML; (sql, bind_rows) entries preserve executemany operations."""
    context = HookContext()
    try:
        directory = __state_path("transaction", name)
        if directory.exists():
            if not (directory / "done.json").exists():
                raise RuntimeError(f"unfinished transaction fixture: {directory}")
            # Clear every previous result/request before starting a new worker.
            # A stale request.json could immediately end the new transaction.
            for path in directory.iterdir():
                path.unlink()
        else:
            directory.mkdir()
        with (directory / "worker.log").open("w") as log:
            worker = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "__hold_tx", name],
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=log,
                text=True,
                start_new_session=True,
            )
            # Closing stdin lets json.load finish without waiting for transaction end.
            with worker.stdin as stdin:
                json.dump(dmls, stdin)
        context.transaction = __wait_file(directory / "ready.json")
    except Exception as error:
        context.outcomes.append(SqlOutcome(exception=error))
    __invoke_hook(after_tx_begin_hook, context, after_tx_begin_hook_kwargs)


def end_tx(
    name: str,
    commit=True,
    *,
    after_tx_end_hook=None,
    after_tx_end_hook_kwargs=None,
) -> None:
    context = HookContext()
    try:
        directory = __state_path("transaction", name)
        context.transaction = _transaction_state(name)
        __publish(directory / "request.json", {"commit": commit})
        result = __wait_file(directory / "done.json")
        if not result.get("ok"):
            raise RuntimeError(result)
    except Exception as error:
        context.outcomes.append(SqlOutcome(exception=error))
    __invoke_hook(after_tx_end_hook, context, after_tx_end_hook_kwargs)


def begin_backfills(
    tables: list[str],
    is_concurrent=False,
    *,
    before_backfill_pause_hook=None,
    before_backfill_pause_hook_kwargs=None,
    after_backfill_pause_hook=None,
    after_backfill_pause_hook_kwargs=None,
) -> None:
    """Wait/check through the pre-pause hook, then pause and checkpoint each table."""

    def __pause(table):
        if not TABLE_IDENTIFIER.fullmatch(table):
            raise RuntimeError(f"invalid table name: {table}")
        context = HookContext(table=table)
        __invoke_hook(
            before_backfill_pause_hook, context, before_backfill_pause_hook_kwargs
        )
        context.outcomes = [
            __sql_outcome(f"ALTER TABLE {table} SET backfill_rate_limit = 0;"),
            __sql_outcome("FLUSH;"),
        ]
        __invoke_hook(
            after_backfill_pause_hook,
            context,
            after_backfill_pause_hook_kwargs,
        )

    __run_each(tables, __pause, is_concurrent)


def end_backfills(
    tables: list[str],
    is_concurrent=False,
    *,
    after_backfill_resume_hook=None,
    after_backfill_resume_hook_kwargs=None,
) -> None:
    """Remove throttling; waiting for completion is an optional post-resume check."""

    def __resume(table):
        if not TABLE_IDENTIFIER.fullmatch(table):
            raise RuntimeError(f"invalid table name: {table}")
        context = HookContext(
            [
                __sql_outcome(
                    f"ALTER TABLE {table} SET backfill_rate_limit = {BACKFILL_RESUME_RATE};"
                )
            ],
            table=table,
        )
        __invoke_hook(
            after_backfill_resume_hook,
            context,
            after_backfill_resume_hook_kwargs,
        )

    __run_each(tables, __resume, is_concurrent)


# Helpers shared with oracle_test_hooks.py.
def _normalize_identifier(value: str, description: str) -> str:
    value = value.upper()
    if not ORACLE_IDENTIFIER.fullmatch(value):
        raise RuntimeError(f"{description} is not a valid unquoted Oracle identifier")
    return value


def _connect_as_sys(service: str):
    def __dsn(service: str) -> str:
        return oracledb.makedsn(
            TEST_ORACLE_HOST, TEST_ORACLE_PORT, service_name=service
        )

    return oracledb.connect(
        user="SYS",
        password=TEST_ORACLE_PASSWORD,
        dsn=__dsn(service),
        mode=oracledb.AUTH_MODE_SYSDBA,
    )


def _error_code(error: oracledb.DatabaseError) -> int:
    return error.args[0].code


def _query_one(sql: str):
    with _connect_as_sys(TEST_ORACLE_PDB) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"query returned no rows: {sql}")
            return row[0]


# Checkpoint readers use checkpoint visibility, never actor-local/non-durable state.
def _source_state_table(source: str) -> str:
    return __state_table(source, "source")


def _backfill_state_table(table: str) -> str:
    return __state_table(table, "table", "AND name LIKE '%_streamcdcscan_%'")


def _read_source_split(table: str) -> dict | None:
    rows = __query(
        "SELECT offset_info->'split_info'->'oracle_split' "
        f"FROM {table} WHERE offset_info->'split_info'->'oracle_split' IS NOT NULL;"
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"expected one Oracle source split, got {rows}")
    return json.loads(rows[0])


def _read_backfill_state(table: str) -> dict | None:
    rows = __query(
        "SELECT jsonb_build_object('pk', \"ID\", 'finished', backfill_finished, "
        f"'rows', row_count, 'offset', cdc_offset) FROM {table};"
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"expected one backfill state row, got {rows}")
    return json.loads(rows[0])


def _save_checkpoint(table: str, value: dict) -> None:
    __publish(__state_path("checkpoint", table), value)


def _load_checkpoint(table: str) -> dict:
    return json.loads(__state_path("checkpoint", table).read_text())


def _transaction_state(name: str) -> dict:
    return json.loads((__state_path("transaction", name) / "ready.json").read_text())


# Module-private helpers.
def __invoke_hook(hook, context: HookContext, kwargs: dict | None) -> None:
    # Import lazily: hooks use the connection and checkpoint readers above.
    if hook is None:
        from oracle_test_hooks import check_success

        hook = check_success
    hook(context, **(kwargs or {}))


def __run_psql(sql: str, *, checkpoint=False, timeout=PSQL_PROCESS_TIMEOUT_SECONDS):
    def __env(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise RuntimeError(f"{name} is not set")
        return value

    process_env = os.environ.copy()
    options = f"-c statement_timeout={DDL_STATEMENT_TIMEOUT_SECONDS}s"
    if checkpoint:
        options += " -c visibility_mode=checkpoint"
    process_env["PGOPTIONS"] = (
        process_env.get("PGOPTIONS", "") + " " + options
    ).strip()
    return subprocess.run(
        [
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
            "-q",
            "-h",
            __env("SLT_HOST"),
            "-p",
            __env("SLT_PORT"),
            "-d",
            __env("SLT_DB"),
            "-U",
            "root",
        ],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
        env=process_env,
        timeout=timeout,
    )


def __query(sql: str) -> list[str]:
    result = __run_psql(sql, checkpoint=True, timeout=CHECKPOINT_QUERY_TIMEOUT_SECONDS)
    result.check_returncode()
    return [line for line in result.stdout.splitlines() if line.strip()]


def __sql_outcome(sql: str) -> SqlOutcome:
    try:
        return SqlOutcome(sql, result=__run_psql(sql))
    except Exception as error:
        return SqlOutcome(sql, exception=error)


def __run_each(items, operation, is_concurrent: bool):
    if is_concurrent and items:
        barrier = threading.Barrier(len(items))

        def __run(item):
            barrier.wait()
            return operation(item)

        with ThreadPoolExecutor(max_workers=len(items)) as executor:
            return list(executor.map(__run, items))
    return [operation(item) for item in items]


def __state_table(name: str, job_type: str, extra_filter="") -> str:
    if not TABLE_IDENTIFIER.fullmatch(name):
        raise RuntimeError(f"invalid {job_type} name: {name}")
    rows = __query(
        "SELECT name FROM rw_catalog.rw_internal_table_info "
        f"WHERE job_name = '{name}' AND job_type = '{job_type}' "
        f"AND schema_name = 'public' {extra_filter};"
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"expected one {job_type} state table for {name}, got {rows}"
        )
    return '"' + rows[0].replace('"', '""') + '"'


def __state_path(kind: str, name: str) -> Path:
    if not TABLE_IDENTIFIER.fullmatch(name):
        raise RuntimeError(f"invalid fixture name: {name}")
    return Path(tempfile.gettempdir()) / f"rw-oracle-{kind}-{name}"


def __publish(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def __wait_file(path: Path, timeout=30) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text())
        done = path.parent / "done.json"
        if done.exists():
            result = json.loads(done.read_text())
            if not result.get("ok"):
                raise RuntimeError(result)
        time.sleep(0.1)
    raise RuntimeError(f"transaction fixture timed out waiting for {path}")


def __main() -> None:
    def __hold_tx(name: str) -> None:
        directory = __state_path("transaction", name)
        try:
            dmls = json.load(sys.stdin)
            with _connect_as_sys(TEST_ORACLE_PDB) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                    before_scn = int(cursor.fetchone()[0])
                    for dml in dmls:
                        if isinstance(dml, str):
                            cursor.execute(dml)
                        else:
                            sql, rows = dml
                            cursor.executemany(sql, rows)
                    cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                    operation_scn = int(cursor.fetchone()[0])
                __publish(
                    directory / "ready.json",
                    {
                        "name": name,
                        "before_scn": before_scn,
                        "operation_scn": operation_scn,
                    },
                )
                # Preserve the existing fixture's automatic rollback on an abandoned SLT.
                request = __wait_file(
                    directory / "request.json", TRANSACTION_TIMEOUT_SECONDS
                )
                if request["commit"]:
                    connection.commit()
                else:
                    connection.rollback()
            __publish(directory / "done.json", {"ok": True})
        except Exception as error:
            __publish(directory / "done.json", {"error": str(error)})
            raise

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("prepare")
    subparsers.add_parser("drop-tables")
    worker_parser = subparsers.add_parser("__hold_tx")
    worker_parser.add_argument("name")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare()
    elif args.command == "drop-tables":
        drop_tables()
    else:
        __hold_tx(args.name)


if __name__ == "__main__":
    __main()
