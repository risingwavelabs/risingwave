# PostgreSQL-style RBAC parser surface

This note documents the staged parser and dispatch surface that was added for
PostgreSQL-style RBAC compatibility work.

## Scope

The SQL parser and frontend statement dispatcher now recognize these statements:

- `CREATE ROLE`
- `ALTER ROLE`
- role-membership `GRANT ... TO ... [WITH ADMIN OPTION]`
- role-membership `REVOKE [ADMIN OPTION FOR] ... FROM ...`
- `SET ROLE`
- `RESET ROLE`

The parser also accepts the PostgreSQL-style role options currently needed by the
test matrix for this lane, including:

- `CREATEDB` / `NOCREATEDB`
- `CREATEROLE` / `NOCREATEROLE`
- `INHERIT` / `NOINHERIT`
- `LOGIN` / `NOLOGIN`
- legacy RisingWave user aliases that already existed

## Current implementation boundary

This lane intentionally lands parser and dispatcher support before the meta and
session lanes are finished.

That means some statements are **accepted and dispatched** but still terminate in
explicit `not implemented` frontend handlers until the downstream lanes land:

- membership grant/revoke execution
- `SET ROLE`
- `RESET ROLE`

This staged boundary is deliberate so that:

1. parser and AST contracts stabilize early,
2. regression tests can be written against a fixed SQL surface, and
3. meta/session work can plug into the already-routed statement variants.

## Verification coverage

The worker verification for this lane covers both unit-style parser assertions and
parser-test YAML contracts:

- `sqlparser_common` role-focused tests
- `create.yaml`
- `privilege.yaml`
- `set.yaml`

These suites are the source of truth for the accepted SQL spellings and formatter
output of the parser surface described above.
