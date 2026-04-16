from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg2


ARTIFACTS = Path("/workspace/artifacts")
DOWNSTREAM_FILE = ARTIFACTS / "downstream/latest_state.json"
RECEIVED_FILE = ARTIFACTS / "http-receiver/received.jsonl"
RECEIVER_ERROR_FILE = ARTIFACTS / "http-receiver/error.log"
SUBSCRIPTION_FILE = ARTIFACTS / "subscription/native_subscribe.json"

OUTPUTS = {
    "backbone": ARTIFACTS / "backbone-status.md",
    "schema": ARTIFACTS / "schema-branch-status.md",
    "failures": ARTIFACTS / "failure-branch-status.md",
}


def connect(user: str = "root", password: str | None = None):
    return psycopg2.connect(
        host="risingwave-standalone",
        port="4566",
        user=user,
        password=password,
        database="dev",
    )


def query_one(sql: str, user: str = "root", password: str | None = None):
    with connect(user=user, password=password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()


def expect(condition: bool, message: str) -> tuple[bool, str]:
    return condition, message


def load_downstream() -> dict:
    if not DOWNSTREAM_FILE.exists():
        return {}
    return json.loads(DOWNSTREAM_FILE.read_text(encoding="utf-8"))


def load_subscription() -> dict:
    if not SUBSCRIPTION_FILE.exists():
        return {}
    return json.loads(SUBSCRIPTION_FILE.read_text(encoding="utf-8"))


def load_receiver_payloads() -> list[dict]:
    if not RECEIVED_FILE.exists():
        return []
    return [
        json.loads(line)
        for line in RECEIVED_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def backbone_checks() -> list[tuple[bool, str]]:
    checks: list[tuple[bool, str]] = []

    columns = query_one(
        """
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = 'demo_core' AND table_name = 'customer_profiles'
        """
    )[0]
    checks.append(expect(
        "profile" in columns and "preferred_contact" in columns and "balance" in columns,
        "Kafka Avro source auto-decodes schema registry fields",
    ))

    latest_rows = query_one("SELECT count(*) FROM demo_ops.customer_profile_current")[0]
    checks.append(expect(latest_rows == 1, "Latest-state materialized view preserves only active non-deleted keys"))

    deleted_row = query_one("SELECT count(*) FROM demo_ops.customer_profile_current WHERE customer_key = 'c-1002'")[0]
    checks.append(expect(deleted_row == 0, "Tombstone handling removes deleted keys from latest-state view"))

    joined_rows = query_one("SELECT count(*) FROM demo_marketing.customer_analytics_dev")[0]
    checks.append(expect(joined_rows >= 2, "Streaming join and tumble window materialized view produces joined rows"))

    udf_and_projection = query_one(
        """
        SELECT count(*)
        FROM demo_ops.customer_profile_current
        WHERE email_normalized = lower(email_normalized)
          AND segment IS NOT NULL
          AND city IS NOT NULL
          AND tag_count >= 1
        """
    )[0]
    checks.append(expect(udf_and_projection >= 1, "UDF output and complex-field projections are visible in the latest-state view"))

    complex_rows = query_one(
        """
        SELECT count(*)
        FROM demo_ops.customer_profile_complex
        WHERE attributes_map IS NOT NULL AND array_length(tags) >= 1
        """
    )[0]
    checks.append(expect(complex_rows >= 1, "Complex Avro fields remain queryable after decoding"))

    subscription = load_subscription()
    initial_rows = subscription.get("initial_rows", [])
    observed_rows = subscription.get("observed_rows", [])
    subscription_ok = not initial_rows and any(
        row.get("customer_key") == "c-1001"
        and row.get("channel") == "subscription-demo"
        and row.get("op") == "Insert"
        for row in observed_rows
    )
    checks.append(expect(subscription_ok, "Native SQL SUBSCRIBE receives incremental rows"))

    downstream = load_downstream()
    checks.append(expect(
        "c-1001" in downstream and downstream.get("c-1002") is None,
        "Processed output is visible in downstream JSON format and reflects latest state plus deletes",
    ))

    return checks


def schema_checks() -> list[tuple[bool, str]]:
    checks: list[tuple[bool, str]] = []

    vip_note_column = query_one(
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'demo_core' AND table_name = 'customer_profiles' AND column_name = 'vip_note'
        """
    )[0]
    checks.append(expect(vip_note_column == 1, "Optional field addition refreshes the source schema with vip_note"))

    latest_rows = query_one("SELECT count(*) FROM demo_ops.customer_profile_current")[0]
    joined_rows = query_one("SELECT count(*) FROM demo_marketing.customer_analytics_dev")[0]
    checks.append(expect(latest_rows >= 2 and joined_rows >= 2, "Source and materialized views remain queryable after add-field evolution"))

    downstream = load_downstream()
    checks.append(expect(
        "c-1001" in downstream,
        "Latest-state output remains readable after add-field evolution",
    ))

    return checks


def failure_checks() -> list[tuple[bool, str]]:
    checks: list[tuple[bool, str]] = []

    recovery_rows = query_one(
        "SELECT count(*) FROM demo_core.customer_change_audit WHERE channel = 'ops-console'"
    )[0]
    checks.append(expect(recovery_rows >= 1, "Source restart recovery resumes ingestion and preserves continuity"))

    receiver_error_log = RECEIVER_ERROR_FILE.read_text(encoding="utf-8") if RECEIVER_ERROR_FILE.exists() else ""
    received_payloads = load_receiver_payloads()
    recovered_keys = {payload.get("customer_key") for payload in received_payloads}
    checks.append(expect(
        '"customer_key": "c-1004"' in receiver_error_log and "c-1004" in recovered_keys,
        "Downstream delivery failure is surfaced and the same key is later delivered successfully after retry",
    ))

    return checks


def render(scope: str, checks: list[tuple[bool, str]]) -> str:
    headers = {
        "backbone": "# Backbone Status",
        "schema": "# Schema Evolution Branch Status",
        "failures": "# Failure Branch Status",
    }
    step_ids = {
        "backbone": ["B1-source-create", "B2-mv-create", "B3-updates-tombstones", "B4-join-subscribe", "B5-json-output"],
        "schema": ["S1-add-optional-field"],
        "failures": ["F1-source-recovery", "F2-downstream-delivery-retry"],
    }
    lines = [headers[scope], ""]
    lines.append("## STEP_IDS")
    for step_id in step_ids[scope]:
        lines.append(f"- {step_id}")
    lines.append("")
    for ok, message in checks:
        lines.append(f"- [{'x' if ok else ' '}] {message}")

    lines.extend([
        "",
        "## Scope boundaries",
        "",
        "- Scope boundary: remove optional field is not part of this demo scope.",
        "- Scope boundary: AD group-to-role mapping is not part of this demo scope.",
        "- Scope boundary: real higher-environment promotion is not part of this demo scope.",
        "- Scope boundary: CSV/XML/fixed-width outputs are not part of this demo scope; fixed-width remains deferred with a remark only.",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", choices=sorted(OUTPUTS), required=True)
    args = parser.parse_args()

    if args.scope == "backbone":
        checks = backbone_checks()
    elif args.scope == "schema":
        checks = schema_checks()
    else:
        checks = failure_checks()

    output = OUTPUTS[args.scope]
    output.write_text(render(args.scope, checks), encoding="utf-8")

    if not all(ok for ok, _ in checks):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
