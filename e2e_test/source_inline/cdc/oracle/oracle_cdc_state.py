#!/usr/bin/env python3

"""Inspect Oracle CDC source state through committed SQL internal-table reads."""

import argparse
import json
import os
import re
import subprocess
import time


IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def query(sql: str) -> list[str]:
    process_env = os.environ.copy()
    process_env["PGOPTIONS"] = (
        process_env.get("PGOPTIONS", "") + " -c visibility_mode=checkpoint"
    ).strip()
    result = subprocess.run(
        [
            "psql", "-X", "-v", "ON_ERROR_STOP=1", "-t", "-A", "-q",
            "-h", os.environ["SLT_HOST"],
            "-p", os.environ["SLT_PORT"],
            "-d", os.environ["SLT_DB"], "-U", "root",
            "-c", sql,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=process_env,
        timeout=30,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def source_state_table(source: str) -> str:
    if not IDENTIFIER.fullmatch(source):
        raise RuntimeError(f"invalid source name: {source}")
    rows = query(
        "SELECT name FROM rw_catalog.rw_internal_table_info "
        f"WHERE job_name = '{source}' AND job_type = 'source' "
        "AND schema_name = 'public';"
    )
    if len(rows) != 1:
        raise RuntimeError(f"expected one source state table for {source}, got {rows}")
    return '"' + rows[0].replace('"', '""') + '"'


def read_source_split(table: str) -> dict | None:
    # Explicit checkpoint visibility reads durable state, not actor-local state
    # or rows merely made visible by a non-checkpoint barrier.
    rows = query(
        "SELECT offset_info->'split_info'->'oracle_split' "
        f"FROM {table} WHERE offset_info->'split_info'->'oracle_split' IS NOT NULL;"
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"expected one Oracle source split, got {rows}")
    return json.loads(rows[0])


def assert_heartbeat_progress(source: str, timeout: int, interval: float) -> None:
    table = source_state_table(source)
    deadline = time.monotonic() + timeout
    previous_scn = None
    initial_mining_scn = None
    advances = 0
    last_split = None
    while time.monotonic() < deadline:
        last_split = read_source_split(table)
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
                    raise RuntimeError(f"mining boundary changed without recovery: {last_split}")
                if offset.get("isHeartbeat") is not True:
                    raise RuntimeError(f"expected a quiet-source heartbeat offset: {offset}")
                # Heartbeats carry the native recovery SCN, not the data-event
                # decoded_commit_scn. Compare the numeric position, not JSON text.
                scn = int(offset["sourceOffset"]["scn"])
                if scn <= 0:
                    raise RuntimeError(f"invalid heartbeat SCN: {offset}")
                if previous_scn is not None:
                    if scn < previous_scn:
                        raise RuntimeError(f"heartbeat SCN regressed: {previous_scn} -> {scn}")
                    if scn > previous_scn:
                        advances += 1
                previous_scn = scn
                # Require three later committed positions. One startup forced
                # heartbeat alone cannot satisfy this periodic-liveness check.
                if advances >= 3:
                    return
        time.sleep(interval)
    raise RuntimeError(
        f"timed out waiting for three checkpointed heartbeat advances for {source}; "
        f"advances={advances}, last_split={last_split}"
    )


def backfill_state_table(table: str) -> str:
    if not IDENTIFIER.fullmatch(table):
        raise RuntimeError(f"invalid table name: {table}")
    rows = query(
        "SELECT name FROM rw_catalog.rw_internal_table_info "
        f"WHERE job_name = '{table}' AND job_type = 'table' "
        "AND schema_name = 'public' AND name LIKE '%_streamcdcscan_%';"
    )
    if len(rows) != 1:
        raise RuntimeError(f"expected one backfill state table for {table}, got {rows}")
    return '"' + rows[0].replace('"', '""') + '"'


def read_backfill_state(table: str) -> dict | None:
    rows = query(
        "SELECT jsonb_build_object('pk', \"ID\", 'finished', backfill_finished, "
        f"'rows', row_count, 'offset', cdc_offset) FROM {table};"
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"expected one backfill state row, got {rows}")
    return json.loads(rows[0])


def mutate_at_frontier(source: str, table: str, timeout: int) -> None:
    from oracle_test_utils import connect_as_sys, identifier, mutate_live_page, query_one

    state_table = backfill_state_table(table)
    source_table = source_state_table(source)
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = read_backfill_state(state_table)
        if state and state["pk"] is not None and state["pk"] >= 5:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"no checkpointed prefix before mutation: {state}")

    query(f"ALTER TABLE {table} SET backfill_rate_limit = 0;")
    query("FLUSH;")
    state = read_backfill_state(state_table)
    # Each query fetches at most five rows. Pausing stops further page requests.
    # Leave enough distance to ID 45 even for the page fetched before the pause.
    if not state or state["finished"] or not 5 <= state["pk"] <= 15:
        raise RuntimeError(f"expected a paused prefix safely below ID 45: {state}")
    if state["rows"] != state["pk"]:
        raise RuntimeError(f"unexpected pre-mutation snapshot progress: {state}")
    if int(state["offset"]["Oracle"]["decoded_commit_scn"]) <= 0:
        raise RuntimeError(f"missing snapshot comparison offset: {state}")
    split = read_source_split(source_table)
    if not split or not split.get("initial_mining_scn"):
        raise RuntimeError(f"missing checkpointed source mining boundary: {split}")
    initial_mining_scn = split["initial_mining_scn"]
    # Hold changes open while checking the already-scanned prefix, then roll them
    # back. Neither the update nor the insertion may become visible in RisingWave.
    with connect_as_sys(identifier("ORACLE_PDB")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE APP.RW_LIVE_PAGE SET VALUE = 'uncommitted' WHERE ID = 1")
            cursor.execute("INSERT INTO APP.RW_LIVE_PAGE VALUES (200, 'rolled-back')")
        if query(f'SELECT "VALUE" FROM {table} WHERE "ID" = 1;') != ["initial-1"]:
            raise RuntimeError("uncommitted update became visible")
        if query(f'SELECT count(*) FROM {table} WHERE "ID" = 200;') != ["0"]:
            raise RuntimeError("uncommitted insertion became visible")
        connection.rollback()
    before_mutation_scn = int(query_one("SELECT CURRENT_SCN FROM V$DATABASE"))
    print(f"paused backfill: {state}; initial_mining_scn={initial_mining_scn}")
    mutate_live_page()

    # Keep backfill paused until the source checkpoints the transaction's data
    # offset. This proves the CDC changes arrive while the suffix is unscanned.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        split = read_source_split(source_table)
        if split and split["initial_mining_scn"] != initial_mining_scn:
            raise RuntimeError(f"mining boundary changed without recovery: {split}")
        raw_offset = split["inner"].get("start_offset") if split else None
        offset = json.loads(raw_offset) if raw_offset else {}
        decoded = offset.get("sourceOffset", {}).get("decoded_commit_scn")
        if offset.get("isHeartbeat") is False and decoded is not None:
            if int(decoded) > before_mutation_scn:
                after = read_backfill_state(state_table)
                if after["pk"] != state["pk"] or after["finished"]:
                    raise RuntimeError(f"snapshot advanced while paused: {state} -> {after}")
                print(f"checkpointed mutation: decoded_commit_scn={decoded}; backfill={after}")
                query(f"ALTER TABLE {table} SET backfill_rate_limit = 1000;")
                return
        time.sleep(0.5)
    raise RuntimeError(f"source did not checkpoint the mutation: {split}")


def run_transaction_case(source: str, table: str, scenario: str, timeout: int) -> None:
    from oracle_test_utils import connect_as_sys, identifier

    if not IDENTIFIER.fullmatch(table):
        raise RuntimeError(f"invalid table name: {table}")
    source_table = source_state_table(source)
    initial = read_source_split(source_table)
    if not initial or not initial.get("initial_mining_scn"):
        raise RuntimeError(f"source mining boundary is not checkpointed: {initial}")
    minimum = 5 if scenario == "long" else 300
    maximum = 35 if scenario == "long" else 800
    rate = 2 if scenario == "long" else 100
    with connect_as_sys(identifier("ORACLE_PDB")) as connection:
        with connection.cursor() as cursor:
            # In the long case the operation precedes CREATE TABLE's lower-bound
            # query, but commit follows the scan of ID 1.
            if scenario == "long":
                cursor.execute("UPDATE APP.RW_LIVE_PAGE SET VALUE = 'long-committed' WHERE ID = 1")
                cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                operation_upper_scn = int(cursor.fetchone()[0])
            query(
                f"SET backfill_rate_limit = {rate}; CREATE TABLE {table} "
                '("ID" INT PRIMARY KEY, "VALUE" VARCHAR) WITH (snapshot.batch_size = 5) '
                f"FROM {source} TABLE 'APP.RW_LIVE_PAGE';"
            )
            state_table = backfill_state_table(table)
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                state = read_backfill_state(state_table)
                if state and state["pk"] is not None and state["pk"] >= minimum:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError(f"transaction case did not reach scanned prefix: {state}")
            query(f"ALTER TABLE {table} SET backfill_rate_limit = 0;")
            query("FLUSH;")
            state = read_backfill_state(state_table)
            if not state or state["finished"] or not minimum <= state["pk"] <= maximum:
                raise RuntimeError(f"unexpected paused transaction frontier: {state}")
            if scenario == "long":
                lower_scn = int(state["offset"]["Oracle"]["decoded_commit_scn"])
                if lower_scn <= operation_upper_scn:
                    raise RuntimeError(
                        f"table lower bound {lower_scn} must follow operation SCN {operation_upper_scn}"
                    )
                if query(f'SELECT "VALUE" FROM {table} WHERE "ID" = 1;') != ["initial-1"]:
                    raise RuntimeError("long transaction became visible before commit")
            else:
                # At least 300 already-scanned rows share one commit SCN, exceeding
                # the default 256-row stream chunk capacity. Include the suffix too.
                cursor.execute(
                    "UPDATE APP.RW_LIVE_PAGE SET VALUE = 'large-' || TO_CHAR(ID) WHERE ID <= 600"
                )
                cursor.executemany(
                    "INSERT INTO APP.RW_LIVE_PAGE VALUES (:1, :2)",
                    [(i, f"large-{i}") for i in range(1001, 1601)],
                )
            print(f"committing {scenario} transaction after checkpointed prefix: {state}")
        connection.commit()
    query(f"ALTER TABLE {table} SET backfill_rate_limit = 1000;")


def run_recovery_case(source: str, table: str, timeout: int) -> None:
    from oracle_test_utils import connect_as_sys, identifier, query_one
    from oracle_transaction_fixture import finish

    if not IDENTIFIER.fullmatch(source) or not IDENTIFIER.fullmatch(table):
        raise RuntimeError("invalid recovery source/table name")
    # The SLT starts the open transaction and creates the source before this
    # helper runs. Table creation and recovery remain here as in the original fixture.
    query(
        f"SET backfill_rate_limit = 2; CREATE TABLE {table} "
        '("ID" INT PRIMARY KEY, "VALUE" VARCHAR) WITH (snapshot.batch_size = 5) '
        f"FROM {source} TABLE 'APP.RW_LIVE_PAGE';"
    )
    state_table = backfill_state_table(table)
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = read_backfill_state(state_table)
        if state and state["pk"] is not None and state["pk"] >= 5:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"recovery backfill did not reach a prefix: {state}")
    query(f"ALTER TABLE {table} SET backfill_rate_limit = 0;")
    query("FLUSH;")
    before = read_backfill_state(state_table)
    if not before or before["finished"] or not 5 <= before["pk"] <= 35:
        raise RuntimeError(f"invalid partial-backfill checkpoint: {before}")

    before_other_commit = int(query_one("SELECT CURRENT_SCN FROM V$DATABASE"))
    with connect_as_sys(identifier("ORACLE_PDB")) as other:
        with other.cursor() as cursor:
            cursor.execute("UPDATE APP.RW_LIVE_PAGE SET VALUE = 'other-committed' WHERE ID = 50")
        other.commit()
    source_table = source_state_table(source)
    deadline = time.monotonic() + timeout
    split = None
    while time.monotonic() < deadline:
        split = read_source_split(source_table)
        raw = split["inner"].get("start_offset") if split else None
        offset = json.loads(raw) if raw else {}
        decoded = offset.get("sourceOffset", {}).get("decoded_commit_scn")
        if offset.get("isHeartbeat") is False and decoded is not None:
            if int(decoded) > before_other_commit:
                break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"no checkpointed data offset before recovery: {split}")
    print(f"recovering with open transaction: backfill={before}; source={split}")
    query("RECOVER;")
    after = read_backfill_state(state_table)
    if not after or after["pk"] != before["pk"] or after["finished"]:
        raise RuntimeError(f"partial-backfill frontier was not restored: {before} -> {after}")
    if query(f'SELECT "VALUE" FROM {table} WHERE "ID" = 1;') != ["initial-1"]:
        raise RuntimeError("open transaction became visible during recovery")
    finish(source, "commit")
    query(f"ALTER TABLE {table} SET backfill_rate_limit = 1000;")
    assert_backfill_finished(table, timeout)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if query(f'SELECT "VALUE" FROM {table} WHERE "ID" = 1;') == ["recovered-long"]:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("transaction spanning recovery was lost")

    query("FLUSH;")
    query("RECOVER;")
    with connect_as_sys(identifier("ORACLE_PDB")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE APP.RW_LIVE_PAGE SET VALUE = 'after-recovery' WHERE ID = 50")
            cursor.execute("DELETE FROM APP.RW_LIVE_PAGE WHERE ID = 2")
            cursor.execute("INSERT INTO APP.RW_LIVE_PAGE VALUES (1001, 'after-recovery-insert')")
        connection.commit()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if query(f'SELECT "VALUE" FROM {table} WHERE "ID" = 50;') == ["after-recovery"]:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("completed table did not resume CDC after recovery")
    query(
        f"SET backfill_rate_limit = 1000; CREATE TABLE {table}_new "
        f"(*) FROM {source} TABLE 'APP.RW_LIVE_PAGE';"
    )


def assert_backfill_finished(table: str, timeout: int) -> None:
    state_table = backfill_state_table(table)
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = read_backfill_state(state_table)
        if state and state["finished"]:
            return
        time.sleep(0.5)
    raise RuntimeError(f"backfill did not checkpoint completion: {state}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=[
        "assert_heartbeat_progress", "mutate_at_frontier", "assert_backfill_finished",
        "run_transaction_case", "run_recovery_case",
    ])
    parser.add_argument("--source")
    parser.add_argument("--table")
    parser.add_argument("--scenario", choices=["long", "large"])
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--interval", type=float, default=3)
    args = parser.parse_args()
    if args.timeout <= 0 or args.interval <= 0:
        parser.error("timeout and interval must be positive")
    if args.command == "assert_heartbeat_progress":
        if not args.source:
            parser.error("--source is required")
        assert_heartbeat_progress(args.source, args.timeout, args.interval)
    elif args.command == "mutate_at_frontier":
        if not args.source or not args.table:
            parser.error("--source and --table are required")
        mutate_at_frontier(args.source, args.table, args.timeout)
    elif args.command == "run_recovery_case":
        if not args.source or not args.table:
            parser.error("--source and --table are required")
        run_recovery_case(args.source, args.table, args.timeout)
    elif args.command == "run_transaction_case":
        if not args.source or not args.table or not args.scenario:
            parser.error("--source, --table and --scenario are required")
        run_transaction_case(args.source, args.table, args.scenario, args.timeout)
    else:
        if not args.table:
            parser.error("--table is required")
        assert_backfill_finished(args.table, args.timeout)


if __name__ == "__main__":
    main()
