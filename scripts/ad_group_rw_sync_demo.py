#!/usr/bin/env python3
"""
Demo controller for syncing external group membership into RisingWave grants.

This script is intentionally narrow:
- It demonstrates a customer-facing workaround / integration pattern.
- It does NOT claim native AD-group RBAC in RisingWave.
- It uses a mock group snapshot as the primary source of truth.

Main demo claim:
    external group membership change -> effective RisingWave permissions change

Example:
    python scripts/ad_group_rw_sync_demo.py \
        --source scripts/groups.demo.json \
        --managed-users alice \
        --mode plan

    python scripts/ad_group_rw_sync_demo.py \
        --source scripts/groups.demo.json \
        --managed-users alice \
        --mode apply
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


GROUP_POLICIES: dict[str, set[str]] = {
    "finance_ro": {"CONNECT", "USAGE", "SELECT"},
    "finance_rw": {"CONNECT", "USAGE", "SELECT", "INSERT", "UPDATE"},
}

PERMISSION_ORDER = ["CONNECT", "USAGE", "SELECT", "INSERT", "UPDATE"]


@dataclass(frozen=True)
class Scope:
    database: str
    schema: str
    table: str

    @property
    def qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass(frozen=True)
class EffectiveState:
    user: str
    exists: bool
    connect: bool
    usage: bool
    select: bool
    insert: bool
    update: bool

    @classmethod
    def missing(cls, user: str) -> "EffectiveState":
        return cls(
            user=user,
            exists=False,
            connect=False,
            usage=False,
            select=False,
            insert=False,
            update=False,
        )

    @classmethod
    def from_permissions(cls, user: str, permissions: set[str]) -> "EffectiveState":
        return cls(
            user=user,
            exists=True,
            connect="CONNECT" in permissions,
            usage="USAGE" in permissions,
            select="SELECT" in permissions,
            insert="INSERT" in permissions,
            update="UPDATE" in permissions,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync a mock AD-group snapshot into RisingWave grants for demo purposes."
    )
    parser.add_argument("--source", required=True, help="Path to the group snapshot JSON file.")
    parser.add_argument(
        "--mode",
        choices=("plan", "apply"),
        default="plan",
        help="Plan only or apply the changes.",
    )
    parser.add_argument(
        "--managed-users",
        required=True,
        help="Comma-separated list of users owned by this demo controller. "
        "Users removed from all groups still need to remain in this managed scope.",
    )
    parser.add_argument("--database", default="demo", help="Managed database name.")
    parser.add_argument("--schema", default="finance", help="Managed schema name.")
    parser.add_argument("--table", default="orders", help="Managed table name.")
    parser.add_argument(
        "--default-password",
        default="demo123",
        help="Password assigned when the controller bootstraps a missing RisingWave user.",
    )
    parser.add_argument(
        "--psql-bin",
        default="psql",
        help="psql binary used to talk to RisingWave.",
    )
    parser.add_argument("--host", default="localhost", help="RisingWave host.")
    parser.add_argument("--port", default="4566", help="RisingWave SQL port.")
    parser.add_argument(
        "--admin-user",
        default="root",
        help="Admin user used by the controller to query and apply grants.",
    )
    parser.add_argument(
        "--admin-database",
        default=None,
        help="Database used by the admin connection. Defaults to --database.",
    )
    return parser.parse_args()


def sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_snapshot(path: Path) -> dict[str, list[str]]:
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("Snapshot must be a JSON object of group -> [users].")

    normalized: dict[str, list[str]] = {}
    for group, users in data.items():
        if group not in GROUP_POLICIES:
            raise ValueError(
                f"Unsupported group {group!r}. Supported groups: {', '.join(GROUP_POLICIES)}"
            )
        if not isinstance(users, list) or any(not isinstance(u, str) for u in users):
            raise ValueError(f"Group {group!r} must map to a list of user names.")
        normalized[group] = users
    return normalized


def managed_users_from_arg(value: str) -> list[str]:
    users = [item.strip() for item in value.split(",") if item.strip()]
    if not users:
        raise ValueError("At least one managed user is required.")
    return users


def run_psql(
    args: argparse.Namespace,
    sql: str,
    *,
    capture_output: bool = True,
) -> str:
    cmd = [
        args.psql_bin,
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-U",
        args.admin_user,
        "-d",
        args.admin_database or args.database,
        "-At",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"psql failed with exit code {result.returncode}\n"
            f"SQL:\n{sql}\n\nSTDERR:\n{result.stderr}"
        )
    return result.stdout if capture_output else ""


def fetch_existing_users(args: argparse.Namespace, users: Iterable[str]) -> set[str]:
    user_list = list(users)
    if not user_list:
        return set()
    literals = ", ".join(sql_literal(user) for user in user_list)
    sql = f"""
    SELECT name
    FROM rw_catalog.rw_users
    WHERE name IN ({literals})
    ORDER BY name;
    """
    output = run_psql(args, sql)
    return {line.strip() for line in output.splitlines() if line.strip()}


def fetch_effective_state(
    args: argparse.Namespace,
    scope: Scope,
    users: list[str],
) -> list[EffectiveState]:
    existing_users = fetch_existing_users(args, users)
    states: list[EffectiveState] = []
    for user in users:
        if user not in existing_users:
            states.append(EffectiveState.missing(user))
            continue

        sql = f"""
        SELECT
            has_database_privilege({sql_literal(user)}, {sql_literal(scope.database)}, 'CONNECT'),
            has_schema_privilege({sql_literal(user)}, {sql_literal(scope.schema)}, 'USAGE'),
            has_table_privilege({sql_literal(user)}, {sql_literal(scope.qualified_table)}, 'SELECT'),
            has_table_privilege({sql_literal(user)}, {sql_literal(scope.qualified_table)}, 'INSERT'),
            has_table_privilege({sql_literal(user)}, {sql_literal(scope.qualified_table)}, 'UPDATE');
        """
        output = run_psql(args, sql).strip()
        connect, usage, select, insert, update = [item == "t" for item in output.split("\t")]
        states.append(
            EffectiveState(
                user=user,
                exists=True,
                connect=connect,
                usage=usage,
                select=select,
                insert=insert,
                update=update,
            )
        )
    return states


def desired_permissions_for_user(snapshot: dict[str, list[str]], user: str) -> set[str]:
    permissions: set[str] = set()
    for group, members in snapshot.items():
        if user in members:
            permissions.update(GROUP_POLICIES[group])
    return permissions


def planned_statements(
    scope: Scope,
    users: list[str],
    snapshot: dict[str, list[str]],
    existing_users: set[str],
    default_password: str,
) -> list[str]:
    statements: list[str] = []
    for user in users:
        user_ident = sql_ident(user)
        if user not in existing_users:
            statements.append(
                f"CREATE USER {user_ident} WITH PASSWORD {sql_literal(default_password)};"
            )

        # Managed-scope desired-state overwrite.
        statements.append(f"REVOKE CONNECT ON DATABASE {sql_ident(scope.database)} FROM {user_ident};")
        statements.append(f"REVOKE USAGE ON SCHEMA {sql_ident(scope.schema)} FROM {user_ident};")
        statements.append(
            f"REVOKE SELECT, INSERT, UPDATE ON TABLE "
            f"{sql_ident(scope.schema)}.{sql_ident(scope.table)} FROM {user_ident};"
        )

        desired = desired_permissions_for_user(snapshot, user)
        if "CONNECT" in desired:
            statements.append(
                f"GRANT CONNECT ON DATABASE {sql_ident(scope.database)} TO {user_ident};"
            )
        if "USAGE" in desired:
            statements.append(
                f"GRANT USAGE ON SCHEMA {sql_ident(scope.schema)} TO {user_ident};"
            )

        table_privileges = [perm for perm in ("SELECT", "INSERT", "UPDATE") if perm in desired]
        if table_privileges:
            statements.append(
                f"GRANT {', '.join(table_privileges)} ON TABLE "
                f"{sql_ident(scope.schema)}.{sql_ident(scope.table)} TO {user_ident};"
            )
    return statements


def print_snapshot(snapshot: dict[str, list[str]]) -> None:
    print("=== Group snapshot ===")
    for group in sorted(GROUP_POLICIES):
        members = ", ".join(snapshot.get(group, [])) or "-"
        print(f"{group:12} -> {members}")
    print()


def print_policy_summary() -> None:
    print("=== Policy mapping ===")
    for group in sorted(GROUP_POLICIES):
        ordered = ", ".join(permission for permission in PERMISSION_ORDER if permission in GROUP_POLICIES[group])
        print(f"{group:12} -> {ordered}")
    print()


def print_state_table(title: str, states: list[EffectiveState]) -> None:
    print(f"=== {title} ===")
    header = f"{'user':12} {'exists':6} {'connect':7} {'usage':5} {'select':6} {'insert':6} {'update':6}"
    print(header)
    print("-" * len(header))
    for state in states:
        print(
            f"{state.user:12} "
            f"{str(state.exists):6} "
            f"{str(state.connect):7} "
            f"{str(state.usage):5} "
            f"{str(state.select):6} "
            f"{str(state.insert):6} "
            f"{str(state.update):6}"
        )
    print()


def expected_after_state(users: list[str], snapshot: dict[str, list[str]]) -> list[EffectiveState]:
    return [
        EffectiveState.from_permissions(user, desired_permissions_for_user(snapshot, user))
        for user in users
    ]


def print_statements(statements: list[str]) -> None:
    print("=== Planned SQL ===")
    for index, statement in enumerate(statements, start=1):
        print(f"{index:02d}. {statement}")
    print()


def apply_statements(args: argparse.Namespace, statements: list[str]) -> None:
    for statement in statements:
        run_psql(args, statement, capture_output=False)


def main() -> int:
    try:
        args = parse_args()
        scope = Scope(database=args.database, schema=args.schema, table=args.table)
        snapshot = parse_snapshot(Path(args.source))
        users = managed_users_from_arg(args.managed_users)
        existing_users = fetch_existing_users(args, users)
        before = fetch_effective_state(args, scope, users)
        statements = planned_statements(
            scope=scope,
            users=users,
            snapshot=snapshot,
            existing_users=existing_users,
            default_password=args.default_password,
        )

        print("Demo claim: external group membership change -> effective RisingWave permissions change")
        print("Mode:", args.mode)
        print(f"Managed scope: database={scope.database}, schema={scope.schema}, table={scope.qualified_table}")
        print("Managed users:", ", ".join(users))
        print("Managed groups:", ", ".join(sorted(GROUP_POLICIES)))
        print()
        print_snapshot(snapshot)
        print_policy_summary()
        print_state_table("Admin-side effective state (before)", before)
        print_statements(statements)
        print_state_table("Expected effective state (after)", expected_after_state(users, snapshot))

        if args.mode == "apply":
            apply_statements(args, statements)
            after = fetch_effective_state(args, scope, users)
            print_state_table("Admin-side effective state (after apply)", after)
            print("Apply completed successfully.")
        else:
            print("Plan only. Re-run with --mode apply to execute the planned SQL.")

        print("Suggested user-side proof:")
        print(f"  1) Connect as a managed user (for example, alice) to database {scope.database}")
        print(f"  2) Run: SELECT * FROM {scope.qualified_table};")
        print(f"  3) Run: INSERT INTO {scope.qualified_table} VALUES (...);")
        return 0
    except Exception as error:  # noqa: BLE001 - demo script should fail loudly and simply.
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
