"""Reusable creation and check hooks grouped by their lifecycle boundary.

Every hook takes HookContext first and checks preceding operation outcomes.
Only the expected-creation-error hook deliberately accepts an unsuccessful SQL.
Scenario fixture SQL and ordinary table-row assertions belong in the SLT.
"""

import json
import time

import oracledb

from oracle_test_utils import (
    TEST_ORACLE_PASSWORD,
    TEST_ORACLE_PDB,
    TEST_ORACLE_SOURCE_SCHEMA,
    TEST_ORACLE_USER,
    HookContext,
    _backfill_state_table,
    _connect_as_sys,
    _error_code,
    _load_checkpoint,
    _normalize_identifier,
    _query_one,
    _read_backfill_state,
    _read_source_split,
    _save_checkpoint,
    _source_state_table,
    _transaction_state,
)

# Contract: keep this test-owned fixture constant synchronized with
# TEST_ORACLE_HEARTBEAT_TABLE in oracle_test_env.slt.part.
TEST_ORACLE_HEARTBEAT_TABLE = "RW_HEARTBEAT"
CHECKPOINT_TIMEOUT_SECONDS = 90
CHECKPOINT_POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_STATE_TIMEOUT_SECONDS = 30
HEARTBEAT_POLL_INTERVAL_SECONDS = 3
HEARTBEAT_REQUIRED_ADVANCES = 3


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
            "source creation failed outside SQL validation"
        ) from outcome.exception
    result = outcome.result
    if result.returncode == 0:
        raise RuntimeError(f"CREATE SOURCE unexpectedly succeeded: {outcome.sql}")
    if expected_message not in result.stderr:
        raise RuntimeError(
            f"CREATE SOURCE did not report the expected error:\n{result.stderr}"
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
        raise RuntimeError(
            f"CREATE SOURCE did not include DBA setup SQL:\n{result.stderr}"
        )
    actual_setup_sql = result.stderr[setup_start:].strip()
    if actual_setup_sql != expected_setup_sql:
        raise RuntimeError(
            "CREATE SOURCE returned unexpected DBA setup SQL\n"
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
        time.sleep(CHECKPOINT_POLL_INTERVAL_SECONDS)


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


# Backfill prefix, paused checkpoints, and recovery/completion comparisons.
def check_backfill_prefix(
    context: HookContext,
    *,
    min_pk: int,
    timeout=CHECKPOINT_TIMEOUT_SECONDS,
) -> None:
    check_success(context)
    state_table = _backfill_state_table(context.table)
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = _read_backfill_state(state_table)
        if state and state["pk"] is not None and state["pk"] >= min_pk:
            return
        time.sleep(CHECKPOINT_POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"backfill did not reach scanned prefix {min_pk}: {state}")


def check_paused_backfill(
    context: HookContext,
    *,
    min_pk: int,
    max_pk: int,
    check_row_count=False,
    check_snapshot_offset=False,
    source=None,
    transaction=None,
) -> None:
    check_success(context)
    state = _read_backfill_state(_backfill_state_table(context.table))
    if not state or state["finished"] or not min_pk <= state["pk"] <= max_pk:
        raise RuntimeError(f"unexpected paused backfill frontier: {state}")
    if check_row_count and state["rows"] != state["pk"]:
        raise RuntimeError(f"unexpected pre-mutation snapshot progress: {state}")
    if (
        check_snapshot_offset
        and int(state["offset"]["Oracle"]["decoded_commit_scn"]) <= 0
    ):
        raise RuntimeError(f"missing snapshot comparison offset: {state}")
    if transaction is not None:
        operation_scn = _transaction_state(transaction)["operation_scn"]
        lower_scn = int(state["offset"]["Oracle"]["decoded_commit_scn"])
        if lower_scn <= operation_scn:
            raise RuntimeError(
                f"table lower bound {lower_scn} must follow operation SCN {operation_scn}"
            )
    checkpoint = {"state": state}
    if source is not None:
        split = _read_source_split(_source_state_table(source))
        if not split or not split.get("initial_mining_scn"):
            raise RuntimeError(f"missing checkpointed source mining boundary: {split}")
        checkpoint["initial_mining_scn"] = split["initial_mining_scn"]
    _save_checkpoint(context.table, checkpoint)
    print(f"paused backfill: {state}")


def check_backfill_unchanged(context: HookContext, *, table=None) -> None:
    check_success(context)
    table = table or context.table
    before = _load_checkpoint(table)["state"]
    after = _read_backfill_state(_backfill_state_table(table))
    if not after or after["pk"] != before["pk"] or after["finished"]:
        raise RuntimeError(f"backfill frontier changed: {before} -> {after}")


def check_backfill_finished(
    context: HookContext,
    *,
    table=None,
    timeout=CHECKPOINT_TIMEOUT_SECONDS,
) -> None:
    check_success(context)
    table = table or context.table
    state_table = _backfill_state_table(table)
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = _read_backfill_state(state_table)
        if state and state["finished"]:
            return
        time.sleep(CHECKPOINT_POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"backfill did not checkpoint completion: {state}")


# Source data-event positions and quiet heartbeat progress.
def check_source_offset(
    context: HookContext,
    *,
    source: str,
    paused_table=None,
    timeout=CHECKPOINT_TIMEOUT_SECONDS,
) -> None:
    check_success(context)
    before_scn = context.transaction["before_scn"]
    source_table = _source_state_table(source)
    checkpoint = _load_checkpoint(paused_table) if paused_table is not None else None
    deadline = time.monotonic() + timeout
    split = None
    while time.monotonic() < deadline:
        split = _read_source_split(source_table)
        if (
            checkpoint
            and split
            and split["initial_mining_scn"] != checkpoint["initial_mining_scn"]
        ):
            raise RuntimeError(f"mining boundary changed without recovery: {split}")
        raw = split["inner"].get("start_offset") if split else None
        offset = json.loads(raw) if raw else {}
        decoded = offset.get("sourceOffset", {}).get("decoded_commit_scn")
        if offset.get("isHeartbeat") is False and decoded is not None:
            if int(decoded) > before_scn:
                if paused_table is not None:
                    check_backfill_unchanged(context, table=paused_table)
                print(f"checkpointed mutation: decoded_commit_scn={decoded}")
                return
        time.sleep(CHECKPOINT_POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"source did not checkpoint the mutation: {split}")


def check_heartbeat_progress(
    context: HookContext,
    *,
    source: str,
    checkpoint=None,
    previous_checkpoint=None,
    timeout=CHECKPOINT_TIMEOUT_SECONDS,
    interval=HEARTBEAT_POLL_INTERVAL_SECONDS,
) -> None:
    check_success(context)
    table = _source_state_table(source)
    saved = _load_checkpoint(previous_checkpoint) if previous_checkpoint else None
    saved_offset = json.loads(saved["inner"]["start_offset"]) if saved else None
    deadline = time.monotonic() + timeout
    previous_scn = int(saved_offset["sourceOffset"]["scn"]) if saved else None
    initial_mining_scn = saved["initial_mining_scn"] if saved else None
    advances = 0
    last_split = None
    while time.monotonic() < deadline:
        last_split = _read_source_split(table)
        if last_split is not None:
            mining_scn = last_split.get("initial_mining_scn")
            raw_offset = last_split["inner"].get("start_offset")
            if mining_scn is not None and raw_offset:
                offset = json.loads(raw_offset)
                if int(mining_scn) <= 0:
                    raise RuntimeError(f"invalid mining boundary: {last_split}")
                if initial_mining_scn is None:
                    initial_mining_scn = mining_scn
                elif mining_scn != initial_mining_scn:
                    raise RuntimeError(
                        f"checkpointed mining boundary changed: {last_split}"
                    )
                if saved_offset is not None:
                    # A fresh initialization can advance SCN too; recovery must
                    # retain the original native snapshot boundary instead.
                    before = saved_offset["sourceOffset"]["snapshot_scn"]
                    after = offset["sourceOffset"]["snapshot_scn"]
                    if before != after:
                        raise RuntimeError(
                            f"snapshot boundary changed across recovery: {before} -> {after}"
                        )
                if offset.get("isHeartbeat") is not True:
                    raise RuntimeError(
                        f"expected a quiet-source heartbeat offset: {offset}"
                    )
                # Heartbeats carry native recovery SCN, not decoded_commit_scn.
                scn = int(offset["sourceOffset"]["scn"])
                if scn <= 0:
                    raise RuntimeError(f"invalid heartbeat SCN: {offset}")
                if previous_scn is not None:
                    if scn < previous_scn:
                        raise RuntimeError(
                            f"heartbeat SCN regressed: {previous_scn} -> {scn}"
                        )
                    if scn > previous_scn:
                        advances += 1
                previous_scn = scn
                # A startup forced heartbeat alone cannot establish periodic liveness.
                if advances >= HEARTBEAT_REQUIRED_ADVANCES:
                    if checkpoint is not None:
                        _save_checkpoint(checkpoint, last_split)
                    print(f"checkpointed heartbeat: scn={scn}, advances={advances}")
                    return
        time.sleep(interval)
    raise RuntimeError(
        f"timed out waiting for three checkpointed heartbeat advances for {source}; "
        f"advances={advances}, last_split={last_split}"
    )
