#!/usr/bin/env python3

"""
Validate the relationship between `risectl hummock refill stats` and vnode
distribution of a streaming job's internal tables.

The script is intended to be used in sqllogictest `system` commands.

What this script validates:
1. The refill policy reported for the target job's internal tables matches
   `--expect-policy`, if provided.
2. For the target job's internal tables, `stats.streaming[table_id]` equals the
   union of vnode bitmaps from `stats.internal.streaming[table_id]`.

Important note:
`stats.internal.streaming` is worker-global `read_version_mapping` state, not a
job-scoped view. Therefore this script only filters and validates the target
job's internal tables. It does not require all entries in
`stats.internal.streaming` to belong to the target job.

Examples:
    psql_refill_validate.py --job-name s3 --job-type sink --mode streaming
    psql_refill_validate.py --job-name mv1 --job-type "materialized view" --mode both
"""

import argparse
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
from collections import defaultdict
from typing import Any

import psycopg2


logging.basicConfig(format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


JOB_TYPE_CHOICES = [
    "table",
    "materialized view",
    "sink",
    "index",
    "source",
]


def build_conn_string(dbname: str) -> str:
    host = (
        os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS")
        or os.environ.get("SLT_HOST")
        or "127.0.0.1"
    )
    port = (
        os.environ.get("RISEDEV_RW_FRONTEND_PORT")
        or os.environ.get("SLT_PORT")
        or "4566"
    )
    return (
        f"host='{host}' port='{port}' dbname='{dbname}' "
        "user='root' password=''"
    )


def describe_connection_target() -> str:
    host = (
        os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS")
        or os.environ.get("SLT_HOST")
        or "127.0.0.1"
    )
    port = (
        os.environ.get("RISEDEV_RW_FRONTEND_PORT")
        or os.environ.get("SLT_PORT")
        or "4566"
    )
    return f"{host}:{port}"


def resolve_meta_addr() -> str:
    return os.environ.get("RW_META_ADDR") or "http://127.0.0.1:5690"


def run_sql(conn, query: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        logger.debug("Executing SQL: %s ; params=%s", query, params)
        cur.execute(query, params)
        return cur.fetchall()


def run_command(command: list[str]) -> str:
    logger.info("Running command: %s", " ".join(command))
    env = os.environ.copy()
    env.setdefault("RW_META_ADDR", resolve_meta_addr())
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result.stdout


def resolve_risectl(explicit_risectl: str | None) -> str:
    if explicit_risectl:
        return explicit_risectl

    local_risectl = Path(__file__).resolve().with_name("risectl")
    if local_risectl.exists():
        return str(local_risectl)

    return "risectl"


def resolve_job(conn, job_name: str, job_type: str | None) -> tuple[int, str]:
    query = """
    WITH all_jobs AS (
        SELECT id, name, 'table' AS job_type FROM rw_catalog.rw_tables
        UNION ALL
        SELECT id, name, 'materialized view' AS job_type FROM rw_catalog.rw_materialized_views
        UNION ALL
        SELECT id, name, 'sink' AS job_type FROM rw_catalog.rw_sinks
        UNION ALL
        SELECT id, name, 'index' AS job_type FROM rw_catalog.rw_indexes
        UNION ALL
        SELECT id, name, 'source' AS job_type FROM rw_catalog.rw_sources WHERE is_shared = true
    )
    SELECT id, job_type
    FROM all_jobs
    WHERE name = %s
    """
    params: tuple[Any, ...]
    if job_type is None:
        params = (job_name,)
    else:
        query += " AND job_type = %s"
        params = (job_name, job_type)

    rows = run_sql(conn, query, params)
    if not rows:
        qualifier = f" of type {job_type}" if job_type else ""
        raise RuntimeError(f"Streaming job {job_name!r}{qualifier} not found")
    if len(rows) > 1:
        raise RuntimeError(
            f"Streaming job {job_name!r} is ambiguous; please specify --job-type. "
            f"Candidates: {rows}"
        )
    job_id, resolved_type = rows[0]
    return int(job_id), str(resolved_type)


def fetch_internal_tables(conn, job_id: int) -> dict[int, str]:
    rows = run_sql(
        conn,
        """
        SELECT id, name
        FROM rw_catalog.rw_internal_table_info
        WHERE job_id = %s
        ORDER BY id
        """,
        (job_id,),
    )
    return {int(table_id): str(name) for table_id, name in rows}


def fetch_fragments(conn, job_id: int) -> dict[int, list[int]]:
    rows = run_sql(
        conn,
        """
        SELECT fragment_id, state_table_id
        FROM (
            SELECT fragment_id, unnest(state_table_ids) AS state_table_id
            FROM rw_catalog.rw_fragments
            WHERE table_id = %s
        ) t
        ORDER BY fragment_id, state_table_id
        """,
        (job_id,),
    )
    fragments: dict[int, list[int]] = defaultdict(list)
    for fragment_id, state_table_id in rows:
        fragments[int(fragment_id)].append(int(state_table_id))
    return dict(fragments)


def fetch_streaming_worker_vnodes(
    conn, job_id: int, internal_table_ids: list[int]
) -> dict[int, dict[int, list[int]]]:
    rows = run_sql(
        conn,
        """
        SELECT
            frag.state_table_id,
            frag.fragment_id,
            a.actor_id,
            a.worker_id,
            rw_actor_vnodes(a.actor_id)::varchar
        FROM (
            SELECT unnest(f.state_table_ids) AS state_table_id, f.fragment_id
            FROM rw_catalog.rw_fragments f
            WHERE f.table_id = %s
        ) frag
        JOIN rw_catalog.rw_actors a
          ON a.fragment_id = frag.fragment_id
        WHERE frag.state_table_id = ANY(%s)
        ORDER BY frag.state_table_id, frag.fragment_id, a.actor_id, a.worker_id
        """,
        (job_id, internal_table_ids),
    )

    result: dict[int, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
    for state_table_id, _fragment_id, _actor_id, worker_id, vnode_json in rows:
        if vnode_json is None:
            continue
        for vnode in json.loads(vnode_json):
            result[int(worker_id)][int(state_table_id)].add(int(vnode))

    return normalize_nested_vnodes(result)


def parse_refill_stats(output: str) -> dict[int, dict[str, Any]]:
    payload = json.loads(output)
    stats_by_worker: dict[int, dict[str, Any]] = {}
    for entry in payload:
        worker_id = int(entry["worker_id"])
        stats_by_worker[worker_id] = entry["stats"]
    return stats_by_worker


def parse_serving_fragment_mapping(output: str) -> list[tuple[int, int, int, list[int]]]:
    rows: list[tuple[int, int, int, list[int]]] = []
    for line in output.splitlines():
        if "│" not in line:
            continue
        parts = [part.strip() for part in line.split("│")[1:-1]]
        if len(parts) != 4 or parts[0] == "Job Id":
            continue

        job_id = int(parts[0])
        fragment_id = int(parts[1])

        vnode_match = re.search(r"in total:\s*(.*)$", parts[2])
        if vnode_match and vnode_match.group(1):
            vnodes = [int(item) for item in vnode_match.group(1).split(",") if item]
        else:
            vnodes = []

        worker_match = re.search(r"id:\s*(\d+)", parts[3])
        if worker_match is None:
            raise RuntimeError(f"Unable to parse worker cell from serving mapping row: {line}")

        worker_id = int(worker_match.group(1))
        rows.append((job_id, fragment_id, worker_id, vnodes))
    return rows


def build_expected_serving_worker_vnodes(
    job_id: int,
    fragments: dict[int, list[int]],
    serving_rows: list[tuple[int, int, int, list[int]]],
    internal_table_ids: set[int],
) -> dict[int, dict[int, list[int]]]:
    result: dict[int, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
    for row_job_id, fragment_id, worker_id, vnodes in serving_rows:
        if row_job_id != job_id:
            continue
        for table_id in fragments.get(fragment_id, []):
            if table_id not in internal_table_ids:
                continue
            result[worker_id][table_id].update(vnodes)
    return normalize_nested_vnodes(result)


def normalize_nested_vnodes(
    data: dict[int, dict[int, set[int] | list[int]]]
) -> dict[int, dict[int, list[int]]]:
    normalized: dict[int, dict[int, list[int]]] = {}
    for worker_id, per_table in data.items():
        normalized[int(worker_id)] = {}
        for table_id, vnodes in per_table.items():
            normalized[int(worker_id)][int(table_id)] = sorted({int(v) for v in vnodes})
    return normalized


def project_mode_stats(
    stats_by_worker: dict[int, dict[str, Any]],
    mode: str,
    relevant_table_ids: set[int],
) -> dict[int, dict[int, list[int]]]:
    projected: dict[int, dict[int, list[int]]] = {}
    for worker_id, stats in stats_by_worker.items():
        per_table = {}
        raw_mode_stats = stats.get(mode, {})
        for table_id_str, vnodes in raw_mode_stats.items():
            table_id = int(table_id_str)
            if table_id not in relevant_table_ids:
                continue
            per_table[table_id] = sorted(int(vnode) for vnode in vnodes)
        projected[int(worker_id)] = per_table
    return projected


def project_internal_streaming_stats(
    stats_by_worker: dict[int, dict[str, Any]],
    relevant_table_ids: set[int],
) -> dict[int, dict[int, list[int]]]:
    projected: dict[int, dict[int, list[int]]] = {}
    for worker_id, stats in stats_by_worker.items():
        per_table = {}
        raw_mode_stats = stats.get("internal", {}).get("streaming", {})
        for table_id_str, vnode_versions in raw_mode_stats.items():
            table_id = int(table_id_str)
            if table_id not in relevant_table_ids:
                continue
            merged = set()
            for vnodes in vnode_versions:
                merged.update(int(vnode) for vnode in vnodes)
            per_table[table_id] = sorted(merged)
        projected[int(worker_id)] = per_table
    return projected


def check_policy(
    stats_by_worker: dict[int, dict[str, Any]],
    internal_table_ids: dict[int, str],
    expected_policy: str,
) -> list[str]:
    mismatches = []
    for worker_id, stats in stats_by_worker.items():
        policies = stats.get("policies", {})
        for table_id, table_name in internal_table_ids.items():
            actual_policy = policies.get(str(table_id))
            if actual_policy != expected_policy:
                mismatches.append(
                    f"worker {worker_id} table {table_id} ({table_name}) "
                    f"policy mismatch: expected {expected_policy!r}, got {actual_policy!r}"
                )
    return mismatches


def compare_mode(
    mode: str,
    expected: dict[int, dict[int, list[int]]],
    actual: dict[int, dict[int, list[int]]],
    internal_table_ids: dict[int, str],
) -> list[str]:
    mismatches = []
    workers = sorted(set(expected) | set(actual))
    table_ids = sorted(internal_table_ids)

    for worker_id in workers:
        expected_per_table = expected.get(worker_id, {})
        actual_per_table = actual.get(worker_id, {})
        for table_id in table_ids:
            expected_vnodes = expected_per_table.get(table_id, [])
            actual_vnodes = actual_per_table.get(table_id, [])
            if expected_vnodes != actual_vnodes:
                mismatches.append(
                    f"{mode} mismatch on worker {worker_id}, "
                    f"table {table_id} ({internal_table_ids[table_id]}): "
                    f"expected {expected_vnodes}, got {actual_vnodes}"
                )
    return mismatches


def dump_summary(
    mode: str,
    data: dict[int, dict[int, list[int]]],
    internal_table_ids: dict[int, str],
) -> str:
    summary = {}
    for worker_id, per_table in sorted(data.items()):
        summary[str(worker_id)] = {
            f"{table_id}:{internal_table_ids.get(table_id, '?')}": vnodes
            for table_id, vnodes in sorted(per_table.items())
        }
    return json.dumps({mode: summary}, indent=2, sort_keys=True)


def compact_summary(
    data: dict[int, dict[int, list[int]]],
    internal_table_ids: dict[int, str],
) -> str:
    lines = []
    for worker_id, per_table in sorted(data.items()):
        if not per_table:
            lines.append(f"worker {worker_id}: no matching tables")
            continue
        parts = []
        for table_id, vnodes in sorted(per_table.items()):
            parts.append(
                f"{table_id}({internal_table_ids.get(table_id, '?')}):"
                f"{len(vnodes)} vnodes"
            )
        lines.append(f"worker {worker_id}: " + ", ".join(parts))
    return "\n".join(lines) if lines else "(empty)"


def log_job_context(
    job_name: str,
    job_id: int,
    resolved_job_type: str,
    internal_tables: dict[int, str],
    fragments: dict[int, list[int]],
) -> None:
    logger.info(
        "Resolved job %r -> id=%s, type=%s",
        job_name,
        job_id,
        resolved_job_type,
    )
    logger.info(
        "Found %s internal tables: %s",
        len(internal_tables),
        ", ".join(
            f"{table_id}({table_name})"
            for table_id, table_name in sorted(internal_tables.items())
        ),
    )
    logger.info(
        "Found %s fragments referencing state tables: %s",
        len(fragments),
        ", ".join(
            f"{fragment_id}->{state_table_ids}"
            for fragment_id, state_table_ids in sorted(fragments.items())
        )
        or "(none)",
    )


def log_mode_summary(
    mode: str,
    label: str,
    data: dict[int, dict[int, list[int]]],
    internal_table_ids: dict[int, str],
) -> None:
    logger.info("%s %s summary:\n%s", label, mode, compact_summary(data, internal_table_ids))
    logger.debug("%s %s details:\n%s", label, mode, dump_summary(mode, data, internal_table_ids))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate that `risectl hummock refill stats` matches vnode distribution "
            "of a streaming job's internal tables."
        )
    )
    parser.add_argument("--job-name", required=True, help="Streaming job name.")
    parser.add_argument(
        "--job-type",
        choices=JOB_TYPE_CHOICES,
        help="Streaming job type. Required if job name is ambiguous.",
    )
    parser.add_argument(
        "--mode",
        choices=["streaming", "serving", "both"],
        default="streaming",
        help="Which refill modes to validate.",
    )
    parser.add_argument(
        "--db",
        default="dev",
        help="Database name to connect to (default: dev).",
    )
    parser.add_argument(
        "--risectl",
        help="Risectl command to execute. Defaults to e2e_test/commands/risectl if present.",
    )
    parser.add_argument(
        "--expect-policy",
        choices=["enabled", "disabled", "streaming", "serving", "both"],
        help="Additionally validate refill policy reported by stats.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )

    args = parser.parse_args()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    risectl = resolve_risectl(args.risectl)

    conn = None
    try:
        logger.info(
            "Starting refill validation: job_name=%s, job_type=%s, mode=%s, db=%s",
            args.job_name,
            args.job_type or "<auto>",
            args.mode,
            args.db,
        )
        logger.info("Using risectl command: %s", risectl)
        logger.info("Using meta address: %s", resolve_meta_addr())
        logger.info("Connecting to RisingWave frontend at %s", describe_connection_target())
        conn = psycopg2.connect(build_conn_string(args.db))
        conn.autocommit = True
        logger.info("Connected to RisingWave frontend")

        job_id, resolved_job_type = resolve_job(conn, args.job_name, args.job_type)
        internal_tables = fetch_internal_tables(conn, job_id)
        if not internal_tables:
            raise RuntimeError(
                f"Job {args.job_name!r} ({resolved_job_type}) has no internal tables to validate"
            )

        fragments = fetch_fragments(conn, job_id)
        log_job_context(args.job_name, job_id, resolved_job_type, internal_tables, fragments)

        relevant_table_ids = set(internal_tables)
        refill_output = run_command([risectl, "hummock", "refill", "stats"])
        stats_by_worker = parse_refill_stats(refill_output)
        logger.info("Loaded refill stats from %s workers", len(stats_by_worker))

        mismatches: list[str] = []

        if args.expect_policy:
            logger.info("Checking refill policy is %r for all matching internal tables", args.expect_policy)
            mismatches.extend(
                check_policy(stats_by_worker, internal_tables, args.expect_policy)
            )

        if args.mode in ("streaming", "both"):
            logger.info(
                "Comparing streaming refill stats against internal.read_version_mapping-derived vnode distribution"
            )
            expected_streaming = project_internal_streaming_stats(
                stats_by_worker, relevant_table_ids
            )
            actual_streaming = project_mode_stats(
                stats_by_worker, "streaming", relevant_table_ids
            )
            log_mode_summary("streaming", "Expected", expected_streaming, internal_tables)
            log_mode_summary("streaming", "Actual", actual_streaming, internal_tables)
            mismatches.extend(
                compare_mode(
                    "streaming", expected_streaming, actual_streaming, internal_tables
                )
            )

        if args.mode in ("serving", "both"):
            logger.info("Collecting expected serving vnode distribution from meta serving mapping")
            serving_output = run_command(
                [risectl, "meta", "list-serving-fragment-mapping"]
            )
            serving_rows = parse_serving_fragment_mapping(serving_output)
            logger.info("Loaded %s serving mapping rows", len(serving_rows))
            expected_serving = build_expected_serving_worker_vnodes(
                job_id, fragments, serving_rows, relevant_table_ids
            )
            log_mode_summary("serving", "Expected", expected_serving, internal_tables)
            actual_serving = project_mode_stats(
                stats_by_worker, "serving", relevant_table_ids
            )
            log_mode_summary("serving", "Actual", actual_serving, internal_tables)
            mismatches.extend(
                compare_mode("serving", expected_serving, actual_serving, internal_tables)
            )

        if mismatches:
            print("Result: MISMATCH")
            logger.error("Validation finished with %s mismatches", len(mismatches))
            for mismatch in mismatches:
                logger.error(mismatch)
            return 1

        logger.info("Validation finished successfully")
        print("Result: MATCH")
        return 0
    except Exception as err:
        logger.exception("Validation failed: %s", err)
        print("Result: MISMATCH")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
