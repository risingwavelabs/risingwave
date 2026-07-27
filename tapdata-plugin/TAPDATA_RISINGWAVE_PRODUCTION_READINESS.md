# TapData RisingWave Connector Release Checklist

This is a stable release checklist. Put commit-specific checksums, task names, row counts, and
approval links in the external release record or pull request.

## Supported scope

The connector is target-only and supports:

- WebSocket streaming for keyed insert, update, and delete;
- WebSocket JSONB append-only for keyless insert streams;
- JDBC as a compatibility fallback.

WebSocket modes require RisingWave 3.0+. JDBC also provides version, schema, metadata, DDL, and
privilege checks for every mode.

The qualified source matrix is:

| Source | Requirement |
|---|---|
| PostgreSQL | Reconstructable complete row images |
| MySQL 8.4 | `binlog_row_image=FULL` |
| MongoDB 7 | `enableFillingModifiedData=true` |
| Kafka 3.9.1 | JSONB append-only for keyless JSON |
| SQL Server 2022 | Change Tracking |
| Oracle 26ai | LogMiner with `autoLog=false` and PK supplemental logging |

## Correctness boundaries

Before release, confirm the documentation states:

- WebSocket streaming requires a primary key and old identity for update/delete;
- automatic relational schema evolution is unsupported;
- unknown or incomplete typed rows fail;
- JSONB append-only rejects update/delete;
- JSONB is at-least-once and can duplicate after ambiguous ACK loss;
- a single serialized WebSocket record over 8 MiB fails;
- SQL Server and Oracle tasks use the real source primary key.

## Required evidence

- [ ] Unit tests pass.
- [ ] Live RisingWave integration tests pass.
- [ ] The exact JAR installs in a clean TapData environment.
- [ ] TapData-stored JAR checksum matches the candidate.
- [ ] Connection Test passes for WebSocket PK, JSONB, and JDBC.
- [ ] A real Data Replication WebSocket task passes snapshot and CDC.
- [ ] JSONB and JDBC smoke tasks pass.
- [ ] Retained-log stop/restart recovery passes.
- [ ] Supported-source matrix passes.
- [ ] Lost-ACK, reconnect, payload, and TLS gates pass.
- [ ] One-hour representative soak passes.
- [ ] Repository CI is green.
- [ ] Code-owner and release-owner approvals are complete.

Use [`TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md`](TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md) for the
commands and pass criteria.

## Artifact gate

Build from a normal clean clone with frozen dependencies. Record:

```text
Git commit
JAR SHA-256
Manifest Git-Commit and Git-Dirty
JDK and Maven versions
Dependency-bundle SHA-256
```

Reject the artifact if:

- `Git-Dirty` is true;
- manifest commit does not match the source commit;
- the TapData-stored JAR differs from the candidate;
- a released version was rebuilt and replaced without a version change.

## Go/no-go

**Go** only when every required-evidence item is checked and the release statement is limited to
the qualified scope.

**No-go** if any data-correctness, exact-artifact, CI, or approval gate is open.
