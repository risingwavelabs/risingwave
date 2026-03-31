"""Shared utilities for integration test sink_check.py scripts.

Provides common functions for checking sink destinations in CI tests.
Each sink_check.py can import what it needs - no mandatory base class.
"""
import subprocess
import sys


def docker_compose_exec(container, command, use_bash=True):
    """Execute a command in a docker compose container and return decoded output.

    Args:
        container: docker compose service name
        command: command string to execute
        use_bash: if True, wraps command in bash -c (default: True)
    Returns:
        decoded UTF-8 string output
    """
    if use_bash:
        args = ["docker", "compose", "exec", container, "bash", "-c", command]
    else:
        args = ["docker", "compose", "exec", container] + command.split()
    return subprocess.check_output(args).decode("utf-8")


def check_row_counts(relations, query_fn, db_name="DB"):
    """Check that all relations have at least 1 row.

    Args:
        relations: list of relation names to check
        query_fn: callable(relation) -> int, returns row count for a relation
        db_name: display name for logging
    Returns:
        list of failed relation names (empty if all passed)
    """
    failed = []
    for rel in relations:
        print(f"Running count check on {rel} ON {db_name}")
        rows = query_fn(rel)
        print(f"{rows} rows in {rel}")
        if rows < 1:
            failed.append(rel)
    return failed


def report_failures(failed_cases):
    """Report failures and exit with code 1 if any exist."""
    if failed_cases:
        print(f"Data check failed for case {failed_cases}")
        sys.exit(1)
