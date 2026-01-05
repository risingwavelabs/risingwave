#!/usr/bin/env python3
"""
Validate tools/docs-code-compare/slices.yml for drift.

What it checks:
- YAML schema sanity (required keys and types)
- Referenced paths exist in code repo and docs repo (best-effort)
- Allows glob-like scopes containing '*' or '?' without existence checks

This is intended to be run periodically (CI/cron) to catch stale scopes early.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Issue:
    level: str  # "ERROR" or "WARN"
    message: str


def _is_glob(s: str) -> bool:
    return "*" in s or "?" in s or ("[" in s and "]" in s)


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Top-level YAML must be a mapping/object")
    return raw


def _expect_list_of_str(x: Any) -> bool:
    return isinstance(x, list) and all(isinstance(i, str) for i in x)


def _check_scope_paths(
    issues: list[Issue],
    *,
    label: str,
    repo_root: Path,
    slice_id: str,
    scope_list: list[str],
) -> None:
    for scope in scope_list:
        if _is_glob(scope):
            continue
        p = repo_root / scope
        if not p.exists():
            issues.append(
                Issue(
                    "WARN",
                    f"{slice_id}: {label} path not found: {scope} (resolved: {p})",
                )
            )


def validate(
    slices_yml: Path,
    *,
    code_repo_root: Path | None,
    docs_repo_root: Path | None,
) -> tuple[list[Issue], dict[str, Any]]:
    issues: list[Issue] = []
    raw = _load_yaml(slices_yml)

    schema_version = raw.get("schema_version")
    if schema_version != 1:
        issues.append(Issue("ERROR", f"Unsupported schema_version: {schema_version!r} (expected 1)"))

    defaults = raw.get("defaults", {})
    if not isinstance(defaults, dict):
        issues.append(Issue("ERROR", "defaults must be a mapping/object"))
        defaults = {}

    # Roots: CLI args override YAML defaults.
    code_root = code_repo_root or Path(str(defaults.get("code_repo_root", "")))
    docs_root = docs_repo_root or Path(str(defaults.get("docs_repo_root", "")))

    if not str(code_root) or not code_root.exists():
        issues.append(Issue("ERROR", f"code repo root does not exist: {code_root}"))
    if docs_repo_root is not None and (not str(docs_root) or not docs_root.exists()):
        issues.append(Issue("ERROR", f"docs repo root does not exist: {docs_root}"))

    slices = raw.get("slices", [])
    if not isinstance(slices, list) or not slices:
        issues.append(Issue("ERROR", "slices must be a non-empty list"))
        return issues, raw

    seen_ids: set[str] = set()
    for s in slices:
        if not isinstance(s, dict):
            issues.append(Issue("ERROR", f"slice entry must be a mapping/object, got: {type(s)}"))
            continue

        slice_id = s.get("id")
        name = s.get("name")
        if not isinstance(slice_id, str) or not slice_id:
            issues.append(Issue("ERROR", f"slice has invalid id: {slice_id!r}"))
            continue
        if slice_id in seen_ids:
            issues.append(Issue("ERROR", f"duplicate slice id: {slice_id}"))
        seen_ids.add(slice_id)

        if not isinstance(name, str) or not name:
            issues.append(Issue("ERROR", f"{slice_id}: invalid name: {name!r}"))

        for k in ("docs_scopes", "code_scopes", "test_signals"):
            v = s.get(k)
            if not _expect_list_of_str(v):
                issues.append(Issue("ERROR", f"{slice_id}: {k} must be a list of strings"))

        key_questions = s.get("key_questions", [])
        if key_questions is not None and not _expect_list_of_str(key_questions):
            issues.append(Issue("ERROR", f"{slice_id}: key_questions must be a list of strings (or omitted)"))

        # Best-effort path checks.
        if isinstance(s.get("code_scopes"), list) and code_root.exists():
            _check_scope_paths(
                issues,
                label="code_scopes",
                repo_root=code_root,
                slice_id=slice_id,
                scope_list=s["code_scopes"],
            )
        if isinstance(s.get("test_signals"), list) and code_root.exists():
            _check_scope_paths(
                issues,
                label="test_signals",
                repo_root=code_root,
                slice_id=slice_id,
                scope_list=s["test_signals"],
            )

        # docs_scopes checks are optional because you might not have docs checkout in this workspace.
        if docs_repo_root is not None and isinstance(s.get("docs_scopes"), list) and docs_root.exists():
            _check_scope_paths(
                issues,
                label="docs_scopes",
                repo_root=docs_root,
                slice_id=slice_id,
                scope_list=s["docs_scopes"],
            )

    return issues, raw


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Validate docs-code-compare slices.yml for drift.")
    ap.add_argument(
        "--slices-yml",
        default=str(Path(__file__).with_name("slices.yml")),
        help="Path to slices.yml",
    )
    ap.add_argument(
        "--code-repo-root",
        default="",
        help="Override code repo root (defaults to slices.yml defaults.code_repo_root)",
    )
    ap.add_argument(
        "--docs-repo-root",
        default="",
        help="If provided, also validate docs_scopes against this docs checkout",
    )
    ap.add_argument("--fail-on-warn", action="store_true", help="Exit non-zero if warnings exist")
    args = ap.parse_args(argv)

    slices_yml = Path(args.slices_yml)
    if not slices_yml.exists():
        print(f"ERROR: slices.yml not found: {slices_yml}", file=sys.stderr)
        return 2

    code_root = Path(args.code_repo_root) if args.code_repo_root else None
    docs_root = Path(args.docs_repo_root) if args.docs_repo_root else None
    if docs_root is not None and not args.docs_repo_root:
        docs_root = None

    issues, _ = validate(slices_yml, code_repo_root=code_root, docs_repo_root=docs_root)

    errors = [i for i in issues if i.level == "ERROR"]
    warns = [i for i in issues if i.level == "WARN"]

    for i in issues:
        print(f"{i.level}: {i.message}")

    if errors:
        return 2
    if args.fail_on_warn and warns:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
