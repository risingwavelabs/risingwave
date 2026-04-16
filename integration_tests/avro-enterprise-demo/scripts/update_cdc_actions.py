from __future__ import annotations

import argparse

import psycopg2


def connect():
    return psycopg2.connect(
        host="source-postgres",
        port="5432",
        user="postgres",
        password="postgres",
        database="customerdb",
    )


def execute_many(statements: list[tuple[str, tuple]]):
    with connect() as conn:
        with conn.cursor() as cursor:
            for sql, params in statements:
                cursor.execute(sql, params)


def scenario_valid() -> None:
    execute_many(
        [
            (
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (1, "c-1001", "purchase", "32.50", "2026-04-07 09:05:00", "web", "marketing"),
            ),
            (
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (2, "c-1001", "purchase", "48.00", "2026-04-07 09:06:00", "mobile", "marketing"),
            ),
            (
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (3, "c-1004", "case_opened", "0.00", "2026-04-07 09:12:00", "support", "ops"),
            ),
        ]
    )


def scenario_source_recovery() -> None:
    execute_many(
        [
            (
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (4, "c-1001", "refund", "5.00", "2026-04-07 09:14:00", "ops-console", "ops"),
            ),
        ]
    )


def scenario_sink_failure() -> None:
    execute_many(
        [
            (
                """
                INSERT INTO public.customer_actions
                (action_id, customer_key, action_type, amount, action_ts, channel, team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (5, "c-1004", "purchase", "19.99", "2026-04-07 09:13:00", "mobile", "marketing"),
            ),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=["valid", "source_recovery", "sink_failure"])
    args = parser.parse_args()

    if args.scenario == "valid":
        scenario_valid()
    elif args.scenario == "source_recovery":
        scenario_source_recovery()
    else:
        scenario_sink_failure()


if __name__ == "__main__":
    main()
