#!/usr/bin/env python3

"""Oracle fixtures and composable SQL operations.

SLTs own fixture SQL and teardown, and attach hooks at named operation boundaries.
Functions are ordered as public APIs, shared helpers (_), then private helpers (__).
Single-caller private helpers are defined inside their caller.
"""

import argparse
import os
import re
import subprocess
import sys
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
DDL_STATEMENT_TIMEOUT_SECONDS = 120
PSQL_PROCESS_TIMEOUT_SECONDS = 150


@dataclass
class SqlOutcome:
    sql: str = ""
    result: subprocess.CompletedProcess[str] | None = None
    exception: Exception | None = None


@dataclass
class HookContext:
    outcomes: list[SqlOutcome] = field(default_factory=list)


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
        __drop_tables()
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


def cleanup() -> None:
    __drop_tables()


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


# Module-private helpers.
def __drop_tables() -> None:
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


def __invoke_hook(hook, context: HookContext, kwargs: dict | None) -> None:
    # Import lazily: hooks use the connection helpers above.
    if hook is None:
        from oracle_test_hooks import check_success

        hook = check_success
    hook(context, **(kwargs or {}))


def __run_psql(sql: str):
    def __env(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise RuntimeError(f"{name} is not set")
        return value

    process_env = os.environ.copy()
    options = f"-c statement_timeout={DDL_STATEMENT_TIMEOUT_SECONDS}s"
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
        timeout=PSQL_PROCESS_TIMEOUT_SECONDS,
    )


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


def __main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("prepare")
    subparsers.add_parser("cleanup")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare()
    elif args.command == "cleanup":
        cleanup()
    else:
        raise ValueError(f"unknown fixture command: {args.command}")


if __name__ == "__main__":
    __main()
