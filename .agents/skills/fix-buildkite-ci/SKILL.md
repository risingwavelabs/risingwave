---
name: fix-buildkite-ci
description: Triage and fix failing Buildkite CI checks for RisingWave pull requests by extracting concrete failing tests, diffs, and error messages from Buildkite build/job logs and artifacts, then applying focused code or test fixes. Use when users ask to diagnose red Buildkite checks, investigate flaky/failing CI jobs, understand why a Buildkite build failed, or provide Buildkite/GitHub check URLs and want a concrete fix.
---

# Fix Buildkite CI

## Overview

Diagnose Buildkite failures programmatically and avoid guessing from UI screenshots. Prefer structured build/job JSON plus artifact inspection to find the exact failing test case and mismatch, then implement the smallest correct fix.

## Workflow

1. Identify the failing Buildkite build(s).
2. Retrieve build JSON and list failed jobs.
3. Pull job logs and extract the first concrete failure signal.
4. Inspect artifacts when top-level logs are truncated.
5. Map failure to root cause and apply a focused fix.
6. Verify locally where feasible and summarize evidence.

Use `bk` CLI first. If auth is unavailable, use public Buildkite JSON/log/artifact endpoints via `curl`.

For exact commands and endpoint patterns, read `references/buildkite-ci-triage.md`.

## Step 1: Identify Failing Buildkite Checks

Use `gh pr checks <PR_NUMBER>` to find failing checks and capture Buildkite URLs (`.../builds/<N>`).

If only branch info is available, list recent branch builds with `bk build list`.

## Step 2: Pull Build JSON and Failed Jobs

Fetch `builds/<N>.json`, then list failed jobs by non-zero `exit_status`.

Capture at least:
- pipeline
- build number
- job id
- job name
- exit status

## Step 3: Extract the Concrete Failure

Fetch each failed job log and search for high-signal patterns:
- `query result mismatch`
- `[Diff] (-expected|+actual)`
- `query is expected to fail with error:`
- panic/assertion lines
- deterministic simulation error markers
- OOM/timeout/cancellation markers

Stop once you have one concrete failing file/case and mismatch.

## Step 4: Fall Back to Artifacts

If logs only show wrapper errors (for example, command exited with status), inspect artifacts from the same job, especially:
- `risedev-logs.zip`
- `risedev-logs/nodetype-*.log`

Extract and search artifact logs for the exact mismatch.

## Step 5: Apply Focused Fixes

Prefer minimal fixes tied to evidence:
- SQLLogicTest mismatch: update expected sections in the correct `.slt`/`.slt.part` file only when query output change is intentional.
- Wrong runtime behavior: fix source code and keep tests as-is.
- Flaky/cancellation-only signal (`143`): treat as infra/cancel unless corroborated by product errors.

Avoid broad "retry and hope" actions without root-cause evidence.

## Step 6: Verify and Report

Run the narrowest local check that validates the fix when possible. If full validation is not feasible, state it explicitly.

Always report:
- failing check/build/job identifiers
- failing file/test/case
- exact mismatch/error evidence
- applied fix (files changed)
- verification status and remaining risk

## Buildkite-Specific Heuristics

- Exit code `105`: often wrapper failure from docker-compose/plugin; inspect SLT/e2e logs for true mismatch.
- Exit code `4`: common in simulation/recovery steps; inspect uploaded simulation logs.
- Exit code `143`: usually cancellation/termination, not a deterministic product regression.
- `raw_log_url` may be null in JSON; use explicit job log endpoints by job id.
- Prefer JSON endpoints plus `jq`; avoid scraping large HTML pages.
