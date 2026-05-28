---
name: risingwave-slt-authoring
description: Use when creating, reviewing, or fixing RisingWave SQLLogicTest (.slt/.slt.serial) tests, especially tests with system commands, connector fixtures, CI lane coverage, madsim compatibility, environment variables, or flaky/idempotent e2e behavior.
---

# RisingWave SLT Authoring

Use this skill before adding or changing RisingWave SQLLogicTest files under `e2e_test/**`, generated DocSlt tests, or SLT snippets embedded in Rust docs.

## Grounding sources

Check the local source of truth before inventing conventions:

- `docs/dev/src/tests/intro.md` — general SLT, DocSlt, and deterministic simulation commands.
- `docs/dev/src/connector/intro.md` — connector e2e patterns, `system ok`, substitution, and external-service environment variables.
- `e2e_test/README.md` — e2e test ownership, `SLT_HOST`/`SLT_PORT`/`SLT_DB`, and coverage check.
- `e2e_test/source_inline/README.md` — source-inline parallel vs serial policy and local dependencies.
- `e2e_test/commands/README.md` — reusable commands available on `PATH` during `risedev slt`.
- `Makefile.toml` task `slt` — exact `risedev slt` environment and sqllogictest invocation.

## Authoring workflow

1. Pick the narrowest test surface:
   - Put normal e2e SQLLogicTests under `e2e_test/**`.
   - Use `e2e_test/source_inline/**/*.slt` for locally runnable source tests.
   - Use `.slt.serial` only when the test cannot run in parallel, such as `RECOVER`, global service mutation, or stateful external-system assumptions.
   - For Rust doc SQL examples, update DocSlt and run the generated SLT path.
2. Keep the full lifecycle in the test file:
   - Create external topics/tables/data before the SQL assertion.
   - Drop RisingWave objects and external resources at the end.
   - Make fixed external resource names idempotent: pre-drop or use unique names before producing data.
3. Use `risedev` wrappers, not raw `sqllogictest`, for normal validation:
   - Start services with `./risedev d` or the relevant profile.
   - Run targeted files with `./risedev slt './path/to/file.slt'`.
   - Use `./risedev slt-clean './path/to/file.slt'` after failed runs that may leave state.
4. If the test invokes shell commands, follow the `system` command rules below.
5. Confirm the test is selected by CI:
   - Inspect the relevant `ci/scripts/e2e-*.sh` glob or command.
   - Run `python3 e2e_test/check_slt_coverage.py -d` when adding a new file and coverage is uncertain.
   - Add/request the narrow CI label for connector-specific lanes when opening the PR.

## `system ok` and substitution rules

- Use `control substitution on` when the SLT needs environment variables such as `${RISEDEV_KAFKA_WITH_OPTIONS_COMMON}` or `${PULSAR_BROKER_URL}`.
- `risedev slt` sets `SLT_HOST`, `SLT_PORT`, `SLT_DB`, and prepends `e2e_test/commands` to `PATH`.
- For raw `psql` probes inside `system ok`, pass the SLT connection variables explicitly:
  `psql -h "$SLT_HOST" -p "$SLT_PORT" -d "$SLT_DB" -U root ...`.
- Prefer reusable helper commands in `e2e_test/commands/` for common external-system operations; put one-off scripts next to the SLT.
- In madsim/`sslt` paths, avoid process-spawning `system ok`; if a mixed-mode SLT must keep a shell command, guard it with `skipif madsim` and preserve a non-madsim validation path.

### SLT blank-line syntax

Think in **records**. A blank line ends the current command or expected-output block; the next non-empty line starts the next SLT directive.

The termination rule differs by record type — verified against the `sqllogictest` 0.29.x parser (`parse_lines`, `parse_multiple_result`):

- **Normal directive without expected output** (`statement ok`, `system ok` without `----`, etc.): use **one visible empty line** after the SQL or shell command. The body parser (`parse_lines`) stops at the first empty line.
- **`query <type>` results after `----`**: use **one visible empty line** after the final expected row. The results loop breaks on the first empty line.
- **`statement error` or `query error` with multiline `----`**: use **two consecutive empty lines** after the final error line. These blocks use `parse_multiple_result`, which requires two consecutive empty lines to terminate — the same rule as `system ok` stdout. Always add the two empty lines whenever another directive follows in the file.
- **`system ok` with stdout assertion (`----`)**: use **two consecutive empty lines** after the final expected stdout line. `parse_multiple_result` consumes both empty lines before the next directive, preventing subsequent `system ok` / `statement ok` blocks from being misread as stdout content.
- Inside a multi-line `system ok` shell command, a blank line terminates the command body. Do not insert blank lines inside heredocs or shell snippets unless the shell input really needs them.

Examples:

```slt
statement ok
create table t(v int);

query I
select v from t order by v;
----
1

system ok
python3 e2e_test/commands/some_helper.py --flag
```

When a `system ok` command asserts stdout with `----`, or when `statement error` / `query error` uses a multiline error block under `----`, use the two-empty-line form:

```slt
# system ok with stdout assertion — two empty lines required
system ok
curl "$PULSAR_HTTP_URL/admin/v2/brokers/ready"
----
ok


system ok
python3 e2e_test/commands/pulsar_util.py create-topic --topic 'persistent://public/default/topic' --partitions 0

# statement error with multiline error — also two empty lines required
statement error
ALTER TABLE t REFRESH SCHEMA;
----
db error: ERROR: Failed to run the query

Caused by:
  some multi-line error detail


statement ok
drop table t;
```

RisingWave CI has failed when a new SLT did not clearly terminate the first stdout block: sqllogictest treated the following directives as expected stdout for the shell command. The same two-empty-line rule applies to multiline `statement error` / `query error` blocks (those that use `----` for the error text rather than an inline regex). Locally smoke-test the file if you add `----` after `system ok` or after `statement error` / `query error` without an inline error pattern.

## Query assertion rules

- Prefer deterministic output:
  - add `order by` or use `rowsort` when row order is not part of the contract;
  - avoid asserting wall-clock timing, random values, unstable IDs, or external-service noise;
  - use `retry`/`backoff` for eventually consistent streaming or connector visibility.
- Keep expected output meaningful:
  - assert the user-visible behavior, not incidental implementation details;
  - do not update expected rows just to make CI green unless the behavior change is intentional and reviewed.
- For connector features, compare the closest existing connector test for semantic parity, especially Kafka/source-inline tests.

## Validation checklist

Before committing an SLT change, collect the narrowest evidence available:

- `git diff --check` for whitespace and patch hygiene.
- If helper scripts changed: syntax-check them, for example `python3 -m py_compile e2e_test/commands/<script>.py`.
- For parser/planner expected-output tests, use the relevant `risedev` update command instead of hand-editing only the golden file.
- For one SLT file with services already running: `./risedev slt './path/to/file.slt'`.
- After a failed run or external-state test: `./risedev slt-clean './path/to/file.slt'` before re-running.
- If local services or clients are unavailable, state the limitation and rely on the focused CI lane, but still run static checks.

## Common failure signatures

- `system command stdout mismatch` at the first command and the diff contains later `system ok`/`statement ok` blocks: the `system` expected-output block was not delimited correctly.
- `/var/run/postgresql/.s.PGSQL.5432`, connection refused to the wrong port, or literal `${SLT_DB}`: a raw shell probe ignored `SLT_HOST`/`SLT_PORT`/`SLT_DB` or used quotes that prevented substitution.
- `not implemented: spawning process is not supported in simulation mode`: `system ok` ran under madsim; add `skipif madsim` or split the test.
- Exact counts increase after retrying locally: fixed external resources kept stale data; add pre-cleanup or make the resource unique.
- Buildkite exit `105`: inspect the inner SLT output or service logs; it is usually a wrapper status, not the root cause.
