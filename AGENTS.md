# AGENTS.md

Single source of truth for agent/tool instructions in this repository. Other instruction files (Copilot/Claude/Cursor) should only point here and keep minimal tool-specific notes. Updates to instructions should be made **only here**.

## Quick context & links
- Project: RisingWave — Postgres-compatible streaming database.
- Architecture (crates overview):
	1. `config` — Default configurations for servers
	2. `prost` — Generated protobuf Rust code (gRPC/messages)
	3. `stream` — Stream compute engine
	4. `batch` — Batch compute engine for queries against materialized views
	5. `frontend` — SQL query planner and scheduler
	6. `storage` — Cloud native storage engine
	7. `meta` — Meta engine
	8. `utils` — Independent util crates
	9. `cmd` — All binaries; `cmd_all` contains the all-in-one binary `risingwave`
	10. `risedevtool` — Developer tool for RisingWave
- Docs: `README.md`, `src/README.md`, `docs/dev/src/connector/intro.md`.

## Build / Run / Test (canonical)
- Build & lint: `./risedev b`, `./risedev c`.
- Run & stop RW: `./risedev d` (tmux; kills previous; optional profiles in `risedev.yml`), `./risedev k`.
- SQL shell: `./risedev psql -c "<query>"`.
- End-to-end SLT: `./risedev slt './path/to/e2e-test-file.slt'` (globs allowed). If a run leaves residue, `./risedev slt-clean ./path/to/e2e-test-file.slt`.
- Tests live in `./e2e_test`; preferred format: SQLLogicTest (SLT).
- Logs: `.risingwave/log`.
- Unit/Parser/Planner tests: standard Rust/Tokio; parser `./risedev update-parser-test`; planner `./risedev run-planner-test [name]` and `./risedev do-apply-planner-test` (`./risedev dapt`).
- `./risedev` is safe to run automatically.

## Coding Style (canonical)
- Comments in English; keep code simple, easy to read/maintain.
- Follow existing repository patterns/conventions; prefer minimal, incremental changes.
- Format with `cargo fmt` when needed; clippy clean is expected (add `#[allow]` only with rationale).
- Prefer small, single-purpose functions; early returns over deep nesting; keep `mut` localized.
- Use explicit types on public APIs / non-obvious iterators to aid readability; avoid making callers guess.
- Error handling: prefer semantic errors (e.g., `thiserror`) and avoid exposing `anyhow::Result` across crate boundaries.
- Unsafe is exceptional; when needed, document Safety invariants and keep the surface minimal.
- Concurrency/async: avoid holding locks across `.await`; isolate blocking work with `spawn_blocking` when necessary.

## Documentation (concise)
- Public APIs/types: add `///` describing semantics, inputs/outputs, and error conditions.
- Comments explain *why/invariants*, not restating code; `unsafe`/`#[allow]` must include rationale.
- User-facing behavior/config changes should be reflected in relevant docs/README; note defaults and compatibility.
- Keep docs human-readable and consistent with the repo voice; avoid template/AI-sounding phrasing and code restatements.

## Testing (concise)
- Bug fixes come with a minimal repro test; cover error paths and boundaries.
- Prefer unit tests for pure logic/parse/transform; use integration/e2e only for cross-component behavior.
- Keep tests deterministic (control time/randomness; inject clock/seed as needed).
- Avoid over-mocking; prefer real or minimal viable components when practical.
- Assertions must check real behavior/state/errors (no empty assertions); validate outputs, state transitions, side effects, and expected error types/messages.
- Add tests where they cover new/previously uncovered branches or regressions; avoid duplicate coverage.

## Safety & boundaries
- Minimal changes; validate with relevant checks/tests before/after.
- Do **not** run git mutation commands (`git add/commit/rebase/push`, etc.) unless explicitly requested by the user.
- Avoid hard-coded endpoints for connectors; use profiles/env vars instead.

## Macros & debugging
- Use `cargo expand` (e.g., `cargo expand -p risingwave_meta`, `cargo expand -p risingwave_expr`) to inspect macro output.
- Rust analyzer macro expansion is helpful for development.

## Connector principles (short)
- Environment-independent; self-contained tests.
- No hard-coded `localhost:...`; use `risedev.yml` profiles and env vars (e.g., `${RISEDEV_KAFKA_WITH_OPTIONS_COMMON}`).
- Keep CI scripts thin; leverage RiseDev to manage external systems.

## Cursor sandbox escalation
Cursor users: when sandboxing is enabled, commands that bind/connect to local TCP need `require_escalated`. See `.cursor/rules/build-run-test.mdc` for the authoritative list.
