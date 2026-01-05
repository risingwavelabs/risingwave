#!/usr/bin/env python3
"""
Run docs-vs-code comparison slices using a configurable Claude Code CLI command.

Design goals:
- Keep the runner generic: it pipes the rendered prompt to the command's stdin and captures stdout.
- Parallelizable: one subprocess per slice, controlled by --jobs.
- Runner-script-configurable: docs/code roots, docs ref, product version, and Claude command/args are all parameters.

Typical usage:
  python3 tools/docs-code-compare/run_claude_slices.py \
    --docs-repo-root /path/to/risingwave-docs \
    --docs-repo-ref main \
    --jobs 6

If your Claude Code CLI needs flags, pass them via --claude-args:
  --claude-cmd claude --claude-args "--some-flag --another-flag"
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shlex
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml


@dataclass(frozen=True)
class SliceDef:
    id: str
    name: str
    docs_scopes: list[str]
    code_scopes: list[str]
    test_signals: list[str]
    key_questions: list[str]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bullet_list(title: str, items: Iterable[str]) -> str:
    lines = [f"- `{x}`" for x in items]
    if not lines:
        lines = ["- (none)"]
    return f"{title}\n\n" + "\n".join(lines) + "\n"


def _resolve_scopes(root: Path, scopes: list[str], *, keep_relative: bool) -> list[str]:
    if keep_relative:
        return scopes
    out: list[str] = []
    for s in scopes:
        # Preserve globs (like integration_tests/*-sink/) as literal strings.
        # Path joining is still fine; it results in "/root/integration_tests/*-sink/".
        out.append(str((root / s).resolve()))
    return out


def _render_prompt(
    template: str,
    *,
    slice_id: str,
    slice_name: str,
    code_repo_root: str,
    docs_repo_root: str,
    docs_repo_ref: str,
    product_version: str,
    docs_scopes: list[str],
    code_scopes: list[str],
    test_signals: list[str],
) -> str:
    # Render scopes as markdown bullet lists (as recommended by README).
    docs_scopes_md = "\n".join([f"- `{x}`" for x in docs_scopes]) or "- (none)"
    code_scopes_md = "\n".join([f"- `{x}`" for x in code_scopes]) or "- (none)"
    test_signals_md = "\n".join([f"- `{x}`" for x in test_signals]) or "- (none)"

    replacements = {
        "{{slice_id}}": slice_id,
        "{{slice_name}}": slice_name,
        "{{code_repo_root}}": code_repo_root,
        "{{docs_repo_root}}": docs_repo_root,
        "{{docs_repo_ref}}": docs_repo_ref,
        "{{product_version}}": product_version,
        "{{docs_scopes}}": docs_scopes_md,
        "{{code_scopes}}": code_scopes_md,
        "{{test_signals}}": test_signals_md,
    }

    out = template
    for k, v in replacements.items():
        out = out.replace(k, v)

    # Light sanity check: templates should not contain unreplaced vars.
    if "{{" in out and "}}" in out:
        # Don't fail hard; just append a warning banner to make it obvious in the prompt.
        out = (
            "WARNING: Unreplaced template variables detected. "
            "Check your template and runner arguments.\n\n"
            + out
        )
    return out


def _load_slices(yaml_path: Path) -> tuple[dict[str, Any], list[SliceDef]]:
    raw = yaml.safe_load(_read_text(yaml_path))
    defaults = raw.get("defaults", {}) if isinstance(raw, dict) else {}
    slices_raw = raw.get("slices", []) if isinstance(raw, dict) else []

    slices: list[SliceDef] = []
    for s in slices_raw:
        slices.append(
            SliceDef(
                id=str(s["id"]),
                name=str(s["name"]),
                docs_scopes=list(s.get("docs_scopes", [])),
                code_scopes=list(s.get("code_scopes", [])),
                test_signals=list(s.get("test_signals", [])),
                key_questions=list(s.get("key_questions", [])),
            )
        )

    return defaults, slices


def _run_one_slice(
    *,
    slice_def: SliceDef,
    prompt: str,
    out_dir: Path,
    claude_cmd: str,
    claude_args: str,
    cwd: Path,
    timeout_s: int,
    write_key_questions: bool,
) -> dict[str, Any]:
    slice_dir = out_dir / slice_def.id
    slice_dir.mkdir(parents=True, exist_ok=True)

    _write_text(slice_dir / "prompt.txt", prompt)
    if write_key_questions:
        _write_text(
            slice_dir / "key_questions.txt",
            "\n".join(slice_def.key_questions).strip() + "\n",
        )

    cmd = [claude_cmd] + shlex.split(claude_args)

    started_at = time.time()
    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            cwd=str(cwd),
            timeout=timeout_s,
            check=False,
        )
    except FileNotFoundError as e:
        return {
            "slice_id": slice_def.id,
            "ok": False,
            "error": f"Claude command not found: {claude_cmd!r} ({e})",
        }
    except subprocess.TimeoutExpired:
        return {
            "slice_id": slice_def.id,
            "ok": False,
            "error": f"Timed out after {timeout_s}s",
        }

    duration_ms = int((time.time() - started_at) * 1000)

    # Persist raw outputs for debugging.
    _write_text(slice_dir / "stdout.txt", proc.stdout or "")
    _write_text(slice_dir / "stderr.txt", proc.stderr or "")

    # Best-effort: treat stdout as the markdown report. Users can override via their CLI args.
    report_path = slice_dir / "slice_report.md"
    _write_text(report_path, proc.stdout or "")

    return {
        "slice_id": slice_def.id,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "duration_ms": duration_ms,
        "report_path": str(report_path),
        "stderr_path": str(slice_dir / "stderr.txt"),
    }

def _looks_like_auth_error(stdout: str, stderr: str) -> bool:
    s = (stdout or "") + "\n" + (stderr or "")
    s_low = s.lower()
    # Claude Code currently prints this on stdout:
    # "Invalid API key · Please run /login"
    if "invalid api key" in s_low and "/login" in s_low:
        return True
    # Defensive: common auth errors
    if "unauthorized" in s_low or "not authenticated" in s_low:
        return True
    return False


def _preflight_claude_auth(claude_cmd: str, claude_args: str, cwd: Path, timeout_s: int) -> tuple[bool, str]:
    """
    Run a tiny request to check Claude Code auth before launching many slices.
    Returns (ok, message).
    """
    cmd = [claude_cmd] + shlex.split(claude_args)
    prompt = "Return exactly: OK"
    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            cwd=str(cwd),
            timeout=max(10, min(timeout_s, 60)),
            check=False,
        )
    except FileNotFoundError as e:
        return False, f"Claude command not found: {claude_cmd!r} ({e})"
    except subprocess.TimeoutExpired:
        return False, "Claude preflight timed out"

    if proc.returncode == 0:
        return True, "ok"
    if _looks_like_auth_error(proc.stdout or "", proc.stderr or ""):
        return (
            False,
            "Claude Code is not authenticated (or API key is invalid). "
            "Run `claude` and execute `/login`, or use `claude setup-token` to set a long-lived token.",
        )
    return False, f"Claude preflight failed (returncode={proc.returncode}). Check stdout/stderr."


def _ensure_claude_non_interactive_args(
    base_args: str,
    *,
    code_repo_root: Path,
    docs_repo_root: Path,
) -> str:
    """
    Make Claude Code automation robust:
    - enforce --print (non-interactive)
    - restrict tools to Read,Bash unless user overrides
    - add --add-dir for docs repo so Read tool can access it
    - default to bypassPermissions to avoid hanging on permission prompts
    """
    toks = shlex.split(base_args)
    toks_set = set(toks)

    def has_any(flags: list[str]) -> bool:
        return any(f in toks_set for f in flags)

    if not has_any(["-p", "--print"]):
        toks.append("--print")

    if "--output-format" not in toks_set:
        toks.extend(["--output-format", "text"])

    if "--tools" not in toks_set:
        toks.extend(["--tools", "Read,Bash"])

    if "--permission-mode" not in toks_set:
        toks.extend(["--permission-mode", "bypassPermissions"])

    # Ensure the docs repo is accessible to Read tool.
    # Add both roots for safety (even if cwd is already code root).
    toks.extend(["--add-dir", str(code_repo_root.resolve())])
    toks.extend(["--add-dir", str(docs_repo_root.resolve())])

    return " ".join(shlex.quote(t) for t in toks)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Run RisingWave docs-vs-code comparison slices via Claude Code CLI.",
    )
    ap.add_argument(
        "--slices-yml",
        default=str(Path(__file__).with_name("slices.yml")),
        help="Path to slices.yml manifest",
    )
    ap.add_argument(
        "--template",
        default=str(Path(__file__).with_name("prompt-claude-slice-template.md")),
        help="Path to prompt template",
    )
    ap.add_argument(
        "--out-dir",
        default=str(Path(__file__).with_name("reports")),
        help="Output directory (per-slice subdirs will be created)",
    )

    ap.add_argument("--docs-repo-root", required=True, help="Local path to risingwave-docs repo")
    ap.add_argument("--docs-repo-ref", default="main", help="Docs branch/commit label to embed in prompts")
    ap.add_argument("--product-version", default="latest", help="Version label to embed in prompts")

    ap.add_argument(
        "--code-repo-root",
        default="",
        help="Override code repo root (defaults to manifest defaults.code_repo_root)",
    )

    ap.add_argument(
        "--slice-ids",
        default="",
        help="Comma-separated slice IDs to run (default: run all)",
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=max(1, (os.cpu_count() or 4) // 2),
        help="Max parallel jobs",
    )
    ap.add_argument(
        "--timeout-s",
        type=int,
        default=1800,
        help="Timeout per slice (seconds)",
    )
    ap.add_argument(
        "--keep-scopes-relative",
        action="store_true",
        help="Do not resolve scopes into absolute paths in the prompt",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Only render prompts and write them to out-dir; do not invoke Claude",
    )

    ap.add_argument(
        "--claude-cmd",
        default=os.environ.get("CLAUDE_CMD", "claude"),
        help="Claude Code CLI command (default: env CLAUDE_CMD or 'claude')",
    )
    ap.add_argument(
        "--claude-args",
        default=os.environ.get("CLAUDE_ARGS", ""),
        help="Extra args passed to Claude command (default: env CLAUDE_ARGS)",
    )
    ap.add_argument(
        "--write-key-questions",
        action="store_true",
        help="Write key_questions.txt per slice (in addition to prompt/report)",
    )
    ap.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip Claude auth preflight check (not recommended)",
    )

    args = ap.parse_args(argv)

    yaml_path = Path(args.slices_yml)
    template_path = Path(args.template)
    out_dir = Path(args.out_dir)

    defaults, slices = _load_slices(yaml_path)

    code_repo_root = args.code_repo_root or str(defaults.get("code_repo_root", ""))
    if not code_repo_root:
        raise SystemExit("Missing code_repo_root (pass --code-repo-root or set defaults.code_repo_root in slices.yml)")

    docs_repo_root = args.docs_repo_root
    docs_root_path = Path(docs_repo_root)
    if not docs_root_path.exists():
        raise SystemExit(f"--docs-repo-root does not exist: {docs_repo_root}")

    template = _read_text(template_path)

    selected_ids = set(x.strip() for x in args.slice_ids.split(",") if x.strip())
    if selected_ids:
        slices = [s for s in slices if s.id in selected_ids]
        missing = sorted(selected_ids - {s.id for s in slices})
        if missing:
            raise SystemExit(f"Unknown slice ids: {', '.join(missing)}")

    code_root_path = Path(code_repo_root)
    if not code_root_path.exists():
        raise SystemExit(f"code repo root does not exist: {code_repo_root}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Build an "effective" claude args string that is safe for automation.
    effective_claude_args = _ensure_claude_non_interactive_args(
        args.claude_args,
        code_repo_root=code_root_path,
        docs_repo_root=docs_root_path,
    )

    run_meta = {
        "started_at": int(time.time()),
        "slices_yml": str(yaml_path.resolve()),
        "template": str(template_path.resolve()),
        "docs_repo_root": str(docs_root_path.resolve()),
        "docs_repo_ref": args.docs_repo_ref,
        "code_repo_root": str(code_root_path.resolve()),
        "product_version": args.product_version,
        "claude_cmd": args.claude_cmd,
        "claude_args": effective_claude_args,
        "jobs": args.jobs,
        "timeout_s": args.timeout_s,
        "dry_run": args.dry_run,
        "slice_ids": [s.id for s in slices],
    }
    _write_text(out_dir / "run.json", json.dumps(run_meta, indent=2) + "\n")

    # Render prompts upfront.
    rendered: dict[str, str] = {}
    for s in slices:
        docs_scopes = _resolve_scopes(docs_root_path, s.docs_scopes, keep_relative=args.keep_scopes_relative)
        code_scopes = _resolve_scopes(code_root_path, s.code_scopes, keep_relative=args.keep_scopes_relative)
        test_signals = _resolve_scopes(code_root_path, s.test_signals, keep_relative=args.keep_scopes_relative)

        rendered[s.id] = _render_prompt(
            template,
            slice_id=s.id,
            slice_name=s.name,
            code_repo_root=str(code_root_path.resolve()),
            docs_repo_root=str(docs_root_path.resolve()),
            docs_repo_ref=args.docs_repo_ref,
            product_version=args.product_version,
            docs_scopes=docs_scopes,
            code_scopes=code_scopes,
            test_signals=test_signals,
        )

        # Also write prompts in dry-run mode (and even in normal mode for reproducibility).
        _write_text(out_dir / s.id / "prompt.txt", rendered[s.id])
        if args.write_key_questions:
            _write_text(out_dir / s.id / "key_questions.txt", "\n".join(s.key_questions).strip() + "\n")

    if args.dry_run:
        print(f"[dry-run] rendered {len(slices)} prompts under: {out_dir}")
        return 0

    if not args.skip_preflight:
        ok, msg = _preflight_claude_auth(args.claude_cmd, effective_claude_args, code_root_path, args.timeout_s)
        if not ok:
            print(f"ERROR: {msg}", file=sys.stderr)
            print(
                "Tip: `claude --print` is required for non-interactive mode; "
                "if your CLI needs extra flags, pass them via --claude-args.",
                file=sys.stderr,
            )
            return 2

    # Execute slices in parallel.
    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = []
        for s in slices:
            futs.append(
                ex.submit(
                    _run_one_slice,
                    slice_def=s,
                    prompt=rendered[s.id],
                    out_dir=out_dir,
                    claude_cmd=args.claude_cmd,
                    claude_args=effective_claude_args,
                    cwd=code_root_path,
                    timeout_s=args.timeout_s,
                    write_key_questions=args.write_key_questions,
                )
            )
        for f in concurrent.futures.as_completed(futs):
            results.append(f.result())

    results_sorted = sorted(results, key=lambda r: r.get("slice_id", ""))
    _write_text(out_dir / "results.json", json.dumps(results_sorted, indent=2) + "\n")

    ok = [r for r in results_sorted if r.get("ok")]
    bad = [r for r in results_sorted if not r.get("ok")]
    print(f"done: ok={len(ok)} failed={len(bad)} out_dir={out_dir}")
    if bad:
        print("failed slices:")
        for r in bad:
            print(f"  - {r.get('slice_id')}: {r.get('error') or ('returncode=' + str(r.get('returncode')))}")
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130)
