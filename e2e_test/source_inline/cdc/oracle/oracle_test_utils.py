#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import oracledb

HEARTBEAT_TABLE = "RW_HEARTBEAT"
MISSING_SCHEMA = "RW_MISSING_SCHEMA"
PRESERVED_HEARTBEAT_VALUE = 7
SOURCE_TABLE = "CUSTOMERS"
SOURCE_SCHEMA = "APP"
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
TABLE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
DDL_STATEMENT_TIMEOUT_SECONDS = 120
PSQL_PROCESS_TIMEOUT_SECONDS = 150
HEARTBEAT_ASSERT_TIMEOUT_SECONDS = 30

ERROR_SCENARIOS = {
    "missing_oracle_schema": (
        MISSING_SCHEMA,
        "table_and_seed_row",
        f"schema '{MISSING_SCHEMA}' does not exist",
    ),
    "missing_app_oracle_hb": (
        SOURCE_SCHEMA,
        "table_and_seed_row",
        f"Failed to create Oracle heartbeat table '{SOURCE_SCHEMA}.{HEARTBEAT_TABLE}'",
    ),
    "empty_app_oracle_hb": (
        SOURCE_SCHEMA,
        "seed_row",
        f"Failed to insert the seed row into Oracle heartbeat table '{SOURCE_SCHEMA}.{HEARTBEAT_TABLE}'",
    ),
    "seeded_app_oracle_hb": (
        SOURCE_SCHEMA,
        "access_grants",
        f"needs UPDATE permission on heartbeat table '{SOURCE_SCHEMA}.{HEARTBEAT_TABLE}'",
    ),
}


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def normalize_identifier(value: str, description: str) -> str:
    value = value.upper()
    if not IDENTIFIER.fullmatch(value):
        raise RuntimeError(f"{description} is not a valid unquoted Oracle identifier")
    return value


def identifier(name: str) -> str:
    return normalize_identifier(env(name), name)


def dsn(service: str) -> str:
    return oracledb.makedsn(
        env("ORACLE_HOST"), int(env("ORACLE_PORT")), service_name=service
    )


def connect_as_sys(service: str):
    return oracledb.connect(
        user="SYS",
        password=env("ORACLE_PASSWORD"),
        dsn=dsn(service),
        mode=oracledb.AUTH_MODE_SYSDBA,
    )


def wait_for_database(service: str):
    deadline = time.monotonic() + 600
    while True:
        try:
            return connect_as_sys(service)
        except oracledb.DatabaseError as error:
            if time.monotonic() >= deadline:
                raise
            print(f"Waiting for Oracle service {service}: {error}", file=sys.stderr)
            time.sleep(5)


def error_code(error: oracledb.DatabaseError) -> int:
    return error.args[0].code


def execute_ignoring(cursor, sql: str, ignored_codes: set[int]) -> None:
    try:
        cursor.execute(sql)
    except oracledb.DatabaseError as error:
        if error_code(error) not in ignored_codes:
            raise


def quoted_password() -> str:
    return '"' + env("ORACLE_PASSWORD").replace('"', '""') + '"'


def prepare_base() -> None:
    user = identifier("ORACLE_USER")
    database = identifier("ORACLE_DATABASE")
    pdb = identifier("ORACLE_PDB")
    password = quoted_password()

    with wait_for_database(database) as connection:
        with connection.cursor() as cursor:
            execute_ignoring(cursor, "ALTER DATABASE FORCE LOGGING", {12920})
            execute_ignoring(
                cursor, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA", {32588}
            )
            try:
                cursor.execute(
                    f"CREATE USER {user} IDENTIFIED BY {password} CONTAINER=ALL"
                )
            except oracledb.DatabaseError as error:
                if error_code(error) != 1920:
                    raise
                cursor.execute(
                    f"ALTER USER {user} IDENTIFIED BY {password} CONTAINER=ALL"
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

    with wait_for_database(pdb) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"CREATE USER {SOURCE_SCHEMA} IDENTIFIED BY {password} "
                    "QUOTA UNLIMITED ON USERS"
                )
            except oracledb.DatabaseError as error:
                if error_code(error) != 1920:
                    raise
                cursor.execute(
                    f"ALTER USER {SOURCE_SCHEMA} IDENTIFIED BY {password}"
                )
            cursor.execute(f"GRANT CREATE SESSION TO {SOURCE_SCHEMA}")
            cursor.execute(f"ALTER USER {user} QUOTA UNLIMITED ON USERS")
            execute_ignoring(
                cursor, f"DROP TABLE {user}.{HEARTBEAT_TABLE} PURGE", {942}
            )
            execute_ignoring(
                cursor, f"DROP TABLE {SOURCE_SCHEMA}.{HEARTBEAT_TABLE} PURGE", {942}
            )
            execute_ignoring(
                cursor, f"DROP TABLE {SOURCE_SCHEMA}.{SOURCE_TABLE} PURGE", {942}
            )
            execute_ignoring(cursor, f"DROP USER {MISSING_SCHEMA} CASCADE", {1918})
            cursor.execute(
                f"CREATE TABLE {SOURCE_SCHEMA}.{SOURCE_TABLE} "
                "(ID NUMBER PRIMARY KEY, NAME VARCHAR2(100))"
            )
            cursor.execute(
                f"ALTER TABLE {SOURCE_SCHEMA}.{SOURCE_TABLE} "
                "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
            )
            cursor.execute(
                f"INSERT INTO {SOURCE_SCHEMA}.{SOURCE_TABLE} (ID, NAME) "
                "VALUES (1, 'RisingWave')"
            )
        connection.commit()


def prepare(state: str) -> None:
    prepare_base()
    user = identifier("ORACLE_USER")
    pdb = identifier("ORACLE_PDB")

    with connect_as_sys(pdb) as connection:
        with connection.cursor() as cursor:
            if state in {
                "missing_oracle_hb",
                "missing_app_oracle_hb",
                "missing_oracle_schema",
            }:
                return

            connector_owned_states = {
                "empty_oracle_hb",
                "seeded_oracle_hb",
                "incompatible_oracle_hb",
            }
            owner = user if state in connector_owned_states else SOURCE_SCHEMA
            if state == "incompatible_oracle_hb":
                cursor.execute(
                    f"CREATE TABLE {user}.{HEARTBEAT_TABLE} "
                    "(ID NUMBER(1) PRIMARY KEY, HEARTBEAT VARCHAR2(20) NOT NULL)"
                )
                cursor.execute(
                    f"INSERT INTO {user}.{HEARTBEAT_TABLE} (ID, HEARTBEAT) "
                    "VALUES (1, 'unchanged')"
                )
            else:
                cursor.execute(
                    f"CREATE TABLE {owner}.{HEARTBEAT_TABLE} "
                    "(ID NUMBER(1) PRIMARY KEY, HEARTBEAT NUMBER(1) NOT NULL)"
                )
                if state in {"seeded_oracle_hb", "seeded_app_oracle_hb"}:
                    cursor.execute(
                        f"INSERT INTO {owner}.{HEARTBEAT_TABLE} (ID, HEARTBEAT) "
                        f"VALUES (1, {PRESERVED_HEARTBEAT_VALUE})"
                    )
        connection.commit()


def query_one(sql: str):
    pdb = identifier("ORACLE_PDB")
    with connect_as_sys(pdb) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"query returned no rows: {sql}")
            return row[0]


def assert_table_missing(owner: str) -> None:
    count = query_one(
        "SELECT COUNT(*) FROM ALL_TABLES "
        f"WHERE OWNER = '{owner}' AND TABLE_NAME = '{HEARTBEAT_TABLE}'"
    )
    if count != 0:
        raise RuntimeError(f"expected {owner}.{HEARTBEAT_TABLE} to be absent")


def assert_schema_missing() -> None:
    count = query_one(
        f"SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = '{MISSING_SCHEMA}'"
    )
    if count != 0:
        raise RuntimeError(f"expected schema {MISSING_SCHEMA} to be absent")


def assert_empty(owner: str) -> None:
    count = query_one(f"SELECT COUNT(*) FROM {owner}.{HEARTBEAT_TABLE}")
    if count != 0:
        raise RuntimeError(
            f"expected {owner}.{HEARTBEAT_TABLE} to be empty, got {count} rows"
        )


def assert_seed(owner: str, expected_value: int | None) -> None:
    pdb = identifier("ORACLE_PDB")
    with connect_as_sys(pdb) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT HEARTBEAT FROM {owner}.{HEARTBEAT_TABLE} WHERE ID = 1"
            )
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise RuntimeError(
                    f"expected one seed row in {owner}.{HEARTBEAT_TABLE}, got {len(rows)}"
                )
            if expected_value is not None and rows[0][0] != expected_value:
                raise RuntimeError(
                    f"expected heartbeat value {expected_value}, got {rows[0][0]}"
                )


def assert_heartbeat_active(owner: str) -> None:
    pdb = identifier("ORACLE_PDB")
    deadline = time.monotonic() + HEARTBEAT_ASSERT_TIMEOUT_SECONDS
    while True:
        with connect_as_sys(pdb) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT HEARTBEAT FROM {owner}.{HEARTBEAT_TABLE} WHERE ID = 1"
                )
                rows = cursor.fetchall()
        if len(rows) == 1 and rows[0][0] in {0, 1}:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"expected an active heartbeat value of 0 or 1 in "
                f"{owner}.{HEARTBEAT_TABLE}, got {rows}"
            )
        time.sleep(0.5)


def assert_incompatible_unchanged() -> None:
    user = identifier("ORACLE_USER")
    data_type = query_one(
        "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS "
        f"WHERE OWNER = '{user}' AND TABLE_NAME = '{HEARTBEAT_TABLE}' "
        "AND COLUMN_NAME = 'HEARTBEAT'"
    )
    value = query_one(
        f"SELECT HEARTBEAT FROM {user}.{HEARTBEAT_TABLE} WHERE ID = 1"
    )
    if data_type != "VARCHAR2" or value != "unchanged":
        raise RuntimeError(
            f"incompatible table was modified: data type={data_type}, value={value}"
        )


def create_table_sql(name: str, heartbeat_table: str | None = None) -> str:
    common_options = env("RISEDEV_ORACLE_WITH_OPTIONS_COMMON")
    user = identifier("ORACLE_USER")
    heartbeat_table = heartbeat_table or f"{user}.{HEARTBEAT_TABLE}"
    return f"""
CREATE TABLE {name} (
    id DECIMAL,
    name VARCHAR,
    PRIMARY KEY (id)
) WITH (
    {common_options},
    schema.name = '{SOURCE_SCHEMA}',
    table.name = '{SOURCE_TABLE}',
    debezium.heartbeat.interval.ms = '2147483647',
    heartbeat.table.name = '{heartbeat_table}',
    heartbeat.table.auto.initialize = 'true'
) FORMAT DEBEZIUM ENCODE JSON;
"""


def manual_setup_sql(owner: str, setup: str) -> str:
    pdb = identifier("ORACLE_PDB")
    user = identifier("ORACLE_USER")
    qualified_name = f"{owner}.{HEARTBEAT_TABLE}"
    lines = [f"ALTER SESSION SET CONTAINER = {pdb};"]
    if setup == "table_and_seed_row":
        lines.append(
            f"CREATE TABLE {qualified_name} "
            "(ID NUMBER(1) PRIMARY KEY, HEARTBEAT NUMBER(1) NOT NULL);"
        )
    if setup != "access_grants":
        lines.extend(
            [
                f"INSERT INTO {qualified_name} (ID, HEARTBEAT) VALUES (1, 0);",
                "COMMIT;",
            ]
        )
    if owner != user:
        lines.append(
            f'GRANT UPDATE (HEARTBEAT) ON {qualified_name} TO "{user}";'
        )
    return "\n".join(lines)


def run_psql(sql: str, table_name: str) -> subprocess.CompletedProcess[str]:
    process_env = os.environ.copy()
    existing_options = process_env.get("PGOPTIONS", "")
    timeout_option = f"-c statement_timeout={DDL_STATEMENT_TIMEOUT_SECONDS}s"
    process_env["PGOPTIONS"] = f"{existing_options} {timeout_option}".strip()
    try:
        return subprocess.run(
            [
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                env("SLT_HOST"),
                "-p",
                env("SLT_PORT"),
                "-d",
                env("SLT_DB"),
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
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"CREATE TABLE {table_name} timed out after "
            f"{PSQL_PROCESS_TIMEOUT_SECONDS} seconds"
        ) from error


def assert_create_table_error(state: str) -> None:
    owner, setup, expected_message = ERROR_SCENARIOS[state]
    heartbeat_table = f"{owner}.{HEARTBEAT_TABLE}"
    result = run_psql(create_table_sql(state, heartbeat_table), state)
    if result.returncode == 0:
        raise RuntimeError(f"CREATE TABLE {state} unexpectedly succeeded")
    if expected_message not in result.stderr:
        raise RuntimeError(
            f"CREATE TABLE {state} did not report the expected error:\n{result.stderr}"
        )

    expected_setup_sql = manual_setup_sql(owner, setup)
    setup_start = result.stderr.find(expected_setup_sql.splitlines()[0])
    if setup_start == -1:
        raise RuntimeError(
            f"CREATE TABLE {state} did not include DBA setup SQL:\n{result.stderr}"
        )
    actual_setup_sql = result.stderr[setup_start:].strip()
    if actual_setup_sql != expected_setup_sql:
        raise RuntimeError(
            f"CREATE TABLE {state} returned unexpected DBA setup SQL\n"
            f"expected:\n{expected_setup_sql}\nactual:\n{actual_setup_sql}"
        )


def create_tables_concurrently(prefix: str, count: int) -> None:
    if not TABLE_IDENTIFIER.fullmatch(prefix):
        raise RuntimeError(f"invalid table prefix: {prefix}")
    if count < 2:
        raise RuntimeError("concurrent table count must be at least 2")

    barrier = threading.Barrier(count)

    def create(index: int) -> tuple[str, subprocess.CompletedProcess[str]]:
        name = f"{prefix}_{index}"
        barrier.wait()
        return name, run_psql(create_table_sql(name), name)

    with ThreadPoolExecutor(max_workers=count) as executor:
        results = list(executor.map(create, range(1, count + 1)))

    failures = [(name, result) for name, result in results if result.returncode != 0]
    if failures:
        details = "\n".join(
            f"{name}:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            for name, result in failures
        )
        raise RuntimeError(f"concurrent CREATE TABLE failed:\n{details}")


def cleanup() -> None:
    user = identifier("ORACLE_USER")
    pdb = identifier("ORACLE_PDB")
    with connect_as_sys(pdb) as connection:
        with connection.cursor() as cursor:
            for owner in [user, SOURCE_SCHEMA]:
                execute_ignoring(
                    cursor, f"DROP TABLE {owner}.{HEARTBEAT_TABLE} PURGE", {942}
                )
            execute_ignoring(
                cursor, f"DROP TABLE {SOURCE_SCHEMA}.{SOURCE_TABLE} PURGE", {942}
            )
            execute_ignoring(cursor, f"DROP USER {MISSING_SCHEMA} CASCADE", {1918})


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument(
        "state",
        choices=[
            "missing_oracle_hb",
            "empty_oracle_hb",
            "seeded_oracle_hb",
            "missing_app_oracle_hb",
            "empty_app_oracle_hb",
            "seeded_app_oracle_hb",
            "incompatible_oracle_hb",
            "missing_oracle_schema",
        ],
    )

    missing_parser = subparsers.add_parser("assert_table_missing")
    missing_parser.add_argument("owner", choices=[SOURCE_SCHEMA])
    subparsers.add_parser("assert_schema_missing")

    empty_parser = subparsers.add_parser("assert_empty")
    empty_parser.add_argument("owner", choices=[SOURCE_SCHEMA])

    seed_parser = subparsers.add_parser("assert_seed")
    seed_parser.add_argument("owner")
    seed_parser.add_argument("--expected_value", type=int)

    active_parser = subparsers.add_parser("assert_heartbeat_active")
    active_parser.add_argument("owner")

    subparsers.add_parser("assert_incompatible_unchanged")

    error_parser = subparsers.add_parser("assert_create_table_error")
    error_parser.add_argument("state", choices=ERROR_SCENARIOS)

    concurrent_parser = subparsers.add_parser("create_tables_concurrently")
    concurrent_parser.add_argument("prefix")
    concurrent_parser.add_argument("--count", type=int, default=2)

    subparsers.add_parser("cleanup")
    args = parser.parse_args()

    if args.command == "prepare":
        prepare(args.state)
    elif args.command == "assert_table_missing":
        assert_table_missing(args.owner)
    elif args.command == "assert_schema_missing":
        assert_schema_missing()
    elif args.command == "assert_empty":
        assert_empty(args.owner)
    elif args.command == "assert_seed":
        assert_seed(normalize_identifier(args.owner, "owner"), args.expected_value)
    elif args.command == "assert_heartbeat_active":
        assert_heartbeat_active(normalize_identifier(args.owner, "owner"))
    elif args.command == "assert_incompatible_unchanged":
        assert_incompatible_unchanged()
    elif args.command == "assert_create_table_error":
        assert_create_table_error(args.state)
    elif args.command == "create_tables_concurrently":
        create_tables_concurrently(args.prefix, args.count)
    else:
        cleanup()


if __name__ == "__main__":
    main()
