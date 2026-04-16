from __future__ import annotations

import json
import time
from pathlib import Path

import psycopg2


ARTIFACT_FILE = Path("/workspace/artifacts/subscription/native_subscribe.json")
SUBSCRIPTION_NAME = "customer_change_audit_sub"
CURSOR_NAME = "customer_change_audit_cur"


def connect_rw():
    return psycopg2.connect(
        host="risingwave-standalone",
        port="4566",
        user="root",
        database="dev",
    )


def connect_source_postgres():
    return psycopg2.connect(
        host="source-postgres",
        port="5432",
        user="postgres",
        password="postgres",
        database="customerdb",
    )


def execute(conn, sql: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()


def fetch_rows(conn, sql: str) -> list[dict]:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [desc.name for desc in cursor.description]
        rows = cursor.fetchall()
    conn.commit()
    return [dict(zip(columns, row, strict=False)) for row in rows]


def main() -> None:
    action_id = int(time.time())
    initial_rows: list[dict] = []
    observed_rows: list[dict] = []
    ARTIFACT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with connect_rw() as rw_conn, connect_source_postgres() as pg_conn:
        execute(rw_conn, "SET search_path TO demo_core, public")
        execute(rw_conn, f"DROP SUBSCRIPTION IF EXISTS {SUBSCRIPTION_NAME}")
        execute(
            rw_conn,
            f"CREATE SUBSCRIPTION {SUBSCRIPTION_NAME} "
            "FROM customer_change_audit WITH (retention = '1 hour')",
        )
        execute(
            rw_conn,
            f"DECLARE {CURSOR_NAME} SUBSCRIPTION CURSOR FOR {SUBSCRIPTION_NAME} SINCE NOW()",
        )
        initial_rows = fetch_rows(rw_conn, f"FETCH NEXT FROM {CURSOR_NAME}")

        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    action_id,
                    "c-1001",
                    "subscription_demo",
                    "11.11",
                    "2026-04-07 09:18:00",
                    "subscription-demo",
                    "marketing",
                ),
            )
        pg_conn.commit()

        for _ in range(20):
            execute(rw_conn, "FLUSH")
            rows = fetch_rows(rw_conn, f"FETCH NEXT FROM {CURSOR_NAME}")
            if rows:
                observed_rows.extend(rows)
                if any(
                    row["customer_key"] == "c-1001" and row["channel"] == "subscription-demo"
                    for row in observed_rows
                ):
                    break
            time.sleep(1)

        execute(rw_conn, f"CLOSE {CURSOR_NAME}")
        execute(rw_conn, f"DROP SUBSCRIPTION IF EXISTS {SUBSCRIPTION_NAME}")

    if initial_rows:
        raise RuntimeError("Expected an empty subscription fetch before new CDC input")
    if not any(
        row["customer_key"] == "c-1001"
        and row["channel"] == "subscription-demo"
        and row["op"] == "Insert"
        for row in observed_rows
    ):
        raise RuntimeError("Expected the subscription cursor to receive the incremental CDC join row")

    ARTIFACT_FILE.write_text(
        json.dumps(
            {
                "subscription_name": SUBSCRIPTION_NAME,
                "cursor_name": CURSOR_NAME,
                "mode": "SINCE NOW()",
                "seed_action_id": action_id,
                "initial_rows": initial_rows,
                "observed_rows": observed_rows,
            },
            default=str,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
