"""Reusable creation and check hooks grouped by their lifecycle boundary.

Every hook takes HookContext first and checks preceding operation outcomes.
Only the expected-creation-error hook deliberately accepts an unsuccessful SQL.
Scenario fixture SQL and ordinary table-row assertions belong in the SLT.
"""

import time

import oracledb

from oracle_test_utils import (
    TEST_ORACLE_PASSWORD,
    TEST_ORACLE_PDB,
    TEST_ORACLE_SOURCE_SCHEMA,
    TEST_ORACLE_USER,
    HookContext,
    _connect_as_sys,
    _error_code,
    _normalize_identifier,
    _query_one,
)

# Contract: keep this test-owned fixture constant synchronized with
# TEST_ORACLE_HEARTBEAT_TABLE in oracle_test_env.slt.part.
TEST_ORACLE_HEARTBEAT_TABLE = "RW_HEARTBEAT"
HEARTBEAT_STATE_TIMEOUT_SECONDS = 30
HEARTBEAT_STATE_POLL_INTERVAL_SECONDS = 0.5


# Public hook APIs.
# SQL execution and expected creation failures.
def check_success(context: HookContext) -> None:
    for outcome in context.outcomes:
        if outcome.exception is not None:
            raise RuntimeError(
                f"operation failed: {outcome.sql}"
            ) from outcome.exception
        result = outcome.result
        if result is not None and result.returncode != 0:
            raise RuntimeError(
                f"SQL failed: {outcome.sql}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )


# Creation hooks never drop existing users or tables.
def no_op(context: HookContext, **kwargs) -> None:
    check_success(context)


def create_default_schema(context: HookContext) -> None:
    check_success(context)
    schema = TEST_ORACLE_SOURCE_SCHEMA
    password = TEST_ORACLE_PASSWORD.replace('"', '""')
    with _connect_as_sys(TEST_ORACLE_PDB) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f'CREATE USER {schema} IDENTIFIED BY "{password}" '
                    "QUOTA UNLIMITED ON USERS"
                )
            except oracledb.DatabaseError as error:
                if _error_code(error) != 1920:
                    raise
                cursor.execute(f'ALTER USER {schema} IDENTIFIED BY "{password}"')
            cursor.execute(f"GRANT CREATE SESSION TO {schema}")


def create_default_heartbeat_tables(context: HookContext, *, owners=None) -> None:
    check_success(context)
    owners = [TEST_ORACLE_USER] if owners is None else owners
    with _connect_as_sys(TEST_ORACLE_PDB) as connection:
        with connection.cursor() as cursor:
            for owner in owners:
                owner = _normalize_identifier(owner, "owner")
                table = f"{owner}.{TEST_ORACLE_HEARTBEAT_TABLE}"
                cursor.execute(
                    f"CREATE TABLE {table} "
                    "(ID NUMBER(1) PRIMARY KEY, HEARTBEAT NUMBER(1) NOT NULL)"
                )
                cursor.execute(f"INSERT INTO {table} (ID, HEARTBEAT) VALUES (1, 0)")
        connection.commit()


def check_heartbeat_creation_error(
    context: HookContext,
    *,
    owner: str,
    expected_message: str,
    create_table=True,
    insert_seed=True,
    after_error_check_hook=None,
    after_error_check_hook_kwargs=None,
) -> None:
    (outcome,) = context.outcomes
    if outcome.exception is not None:
        raise RuntimeError(
            "creation failed outside SQL validation"
        ) from outcome.exception
    result = outcome.result
    if result.returncode == 0:
        raise RuntimeError(f"creation unexpectedly succeeded: {outcome.sql}")
    if expected_message not in result.stderr:
        raise RuntimeError(
            f"creation did not report the expected error:\n{result.stderr}"
        )

    owner = _normalize_identifier(owner, "owner")
    pdb = TEST_ORACLE_PDB
    user = TEST_ORACLE_USER
    qualified_name = f"{owner}.{TEST_ORACLE_HEARTBEAT_TABLE}"
    lines = [f"ALTER SESSION SET CONTAINER = {pdb};"]
    if create_table:
        lines.append(
            f"CREATE TABLE {qualified_name} "
            "(ID NUMBER(1) PRIMARY KEY, HEARTBEAT NUMBER(1) NOT NULL);"
        )
    if insert_seed:
        lines.extend(
            [
                f"INSERT INTO {qualified_name} (ID, HEARTBEAT) VALUES (1, 0);",
                "COMMIT;",
            ]
        )
    if owner != user:
        lines.append(f'GRANT UPDATE (HEARTBEAT) ON {qualified_name} TO "{user}";')
    expected_setup_sql = "\n".join(lines)
    setup_start = result.stderr.find(lines[0])
    if setup_start == -1:
        raise RuntimeError(f"creation did not include DBA setup SQL:\n{result.stderr}")
    actual_setup_sql = result.stderr[setup_start:].strip()
    if actual_setup_sql != expected_setup_sql:
        raise RuntimeError(
            "creation returned unexpected DBA setup SQL\n"
            f"expected:\n{expected_setup_sql}\nactual:\n{actual_setup_sql}"
        )
    if after_error_check_hook is not None:
        # The expected DDL failure has been validated. State checks must not
        # interpret that same expected failure as an unsuccessful operation.
        after_error_check_hook(HookContext(), **(after_error_check_hook_kwargs or {}))


# Oracle heartbeat-table state, independent of RisingWave source-offset progress.
def check_heartbeat_missing(context: HookContext, *, owner: str) -> None:
    check_success(context)
    owner = _normalize_identifier(owner, "owner")
    count = _query_one(
        "SELECT COUNT(*) FROM ALL_TABLES "
        f"WHERE OWNER = '{owner}' AND TABLE_NAME = '{TEST_ORACLE_HEARTBEAT_TABLE}'"
    )
    if count != 0:
        raise RuntimeError(
            f"expected {owner}.{TEST_ORACLE_HEARTBEAT_TABLE} to be absent"
        )


def check_schema_missing(context: HookContext, *, owner: str) -> None:
    check_success(context)
    owner = _normalize_identifier(owner, "owner")
    count = _query_one(f"SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = '{owner}'")
    if count != 0:
        raise RuntimeError(f"expected schema {owner} to be absent")


def check_heartbeat_empty(context: HookContext, *, owner: str) -> None:
    check_success(context)
    owner = _normalize_identifier(owner, "owner")
    count = _query_one(f"SELECT COUNT(*) FROM {owner}.{TEST_ORACLE_HEARTBEAT_TABLE}")
    if count != 0:
        raise RuntimeError(
            f"expected {owner}.{TEST_ORACLE_HEARTBEAT_TABLE} to be empty, got {count} rows"
        )


def check_heartbeat_seed(
    context: HookContext,
    *,
    owner: str,
    allowed_values=None,
    timeout=0,
) -> None:
    check_success(context)
    owner = _normalize_identifier(owner, "owner")
    deadline = time.monotonic() + timeout
    while True:
        with _connect_as_sys(TEST_ORACLE_PDB) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT HEARTBEAT FROM {owner}.{TEST_ORACLE_HEARTBEAT_TABLE} WHERE ID = 1"
                )
                rows = cursor.fetchall()
        if len(rows) == 1:
            value = rows[0][0]
            if allowed_values is None or value in allowed_values:
                return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"unexpected seed in {owner}.{TEST_ORACLE_HEARTBEAT_TABLE}: {rows}; "
                f"allowed_values={allowed_values}"
            )
        time.sleep(HEARTBEAT_STATE_POLL_INTERVAL_SECONDS)


def check_incompatible_heartbeat_unchanged(context: HookContext) -> None:
    check_success(context)
    user = TEST_ORACLE_USER
    data_type = _query_one(
        "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS "
        f"WHERE OWNER = '{user}' AND TABLE_NAME = '{TEST_ORACLE_HEARTBEAT_TABLE}' "
        "AND COLUMN_NAME = 'HEARTBEAT'"
    )
    value = _query_one(
        f"SELECT HEARTBEAT FROM {user}.{TEST_ORACLE_HEARTBEAT_TABLE} WHERE ID = 1"
    )
    if data_type != "VARCHAR2" or value != "unchanged":
        raise RuntimeError(
            f"incompatible table was modified: data type={data_type}, value={value}"
        )
