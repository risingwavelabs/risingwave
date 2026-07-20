# TapData RisingWave Connector Release Readiness

This is the current release checklist and evidence summary for connector `1.0.0`. It intentionally
omits the chronological local execution log. A release is identified by its exact Git revision and
JAR SHA-256, never by the version string alone.

## Decision

**Code status:** release-candidate quality within the qualified source and deployment matrix.

**Distribution status:** not ready until the canonical repository, CI owner, final clean artifact,
and publication channel are confirmed.

The dependency upgrade to pgjdbc `42.7.12` and Jackson `2.18.9` invalidated the previously recorded
JAR checksum. No existing local JAR or historical checksum is a current release artifact.

## Supported scope

| Area | 1.0.0 position |
|---|---|
| Connector direction | RisingWave target only |
| Default transport | WebSocket streaming with a primary key |
| Keyless transport | WebSocket JSONB append-only or JDBC fallback |
| RisingWave version | 3.0.0+ for WebSocket; JDBC fallback supports earlier versions |
| PostgreSQL source | Qualified for snapshot and CDC |
| MySQL source | Qualified with `binlog_row_image=FULL` |
| MongoDB source | Qualified with TapData Update Field Completion enabled |
| Kafka source | Qualified through JSONB append-only mode |
| SQL Server / Oracle | Not qualified for 1.0.0 |
| RisingWave Cloud | JDBC TLS and WSS were exercised on a development cluster |

## Qualification evidence

The implemented behavior has passed the following local qualification work. These results establish
feature confidence but do not replace the final exact-artifact run after repository integration.

| Test area | Result |
|---|---|
| Maven unit suite | 48 tests passed |
| Local RisingWave live suite | 37 tests passed |
| Connection pre-checks | Version, schema, DDL privilege, webhook table, signed init, DML, and ACK |
| PostgreSQL replication | Snapshot plus keyed insert, update, delete, stop, and restart |
| MySQL replication | FULL row-image update preserved unchanged fields |
| MongoDB replication | Filled update preserved fields; `$unset` mapped to SQL `NULL` |
| Kafka replication | Continuous keyless inserts through JSONB append-only mode |
| CDC semantics | Replace, removed fields, primary-key changes, missing identity, and unknown fields |
| Scalar values | Numeric, decimal, temporal, JSONB, and binary round trips |
| JDBC JSONB | Uses PostgreSQL `PGobject(type=jsonb)` and writes successfully |
| Failure recovery | Disconnect/reconnect and repeated reconnect stress passed |
| Lost ACK | Keyed replay remained one row; JSONB replay duplicated as documented |
| Payload boundary | 8 MiB connector split limit stayed below tested self-hosted and Cloud limits |
| TLS | JDBC TLS and WSS success; invalid TLS configuration failed closed |
| Packaging | Shaded JDBC/Jackson classes load without TapData classloader conflicts |

## Correctness boundaries

- Success is reported only after a durable RisingWave ACK.
- Keyed WebSocket replay is idempotent; JSONB append-only replay is at-least-once and may duplicate.
- Typed WebSocket and JDBC updates share one CDC normalizer and require a reconstructable complete
  target row.
- Primary-key changes require both old and new identities and become delete plus upsert.
- Unknown relational fields fail because automatic DDL/schema evolution is not implemented.
- Typed WebSocket assumes the connector owns the complete target table schema. Manually added
  target-only columns are outside the supported contract.
- A single serialized source record larger than 8 MiB is rejected; multi-record batches are split.
- Webhook secrets use RisingWave Secret references and are not persisted as DDL string literals.

## Source and deployment requirements

- Keep MySQL `binlog_row_image=FULL`.
- Keep MongoDB `enableFillingModifiedData=true`; the target cannot distinguish an unsafe partial
  patch from a valid sparse full document when both arrive as after-only `_id` events.
- Treat SQL Server and Oracle as unqualified until tested with supported TapData connectors.
- Do not claim exactly-once delivery for keyless JSONB.
- Use an explicitly approved TapData runtime version and pin production container images by digest.
- Use WSS and an appropriate JDBC SSL mode for remote deployments.

## Remaining release gates

1. **Repository ownership:** choose one canonical source repository and publication owner.
2. **Integration:** rebase or migrate onto the destination repository's current main branch.
3. **CI:** ensure connector changes trigger at least Maven unit tests and packaging.
4. **Clean build:** build from a committed worktree with `Git-Dirty: false`.
5. **Exact-artifact tests:** install the generated JAR in a clean TapData environment and run:
   - connection pre-checks;
   - PostgreSQL to keyed WebSocket snapshot and CDC;
   - JSONB append-only smoke;
   - JDBC smoke.
6. **Artifact audit:** inspect manifest provenance and shaded dependency versions.
7. **Archive:** store the exact JAR, SHA-256, and frozen TapData dependency subset immutably.
8. **Review:** open a PR and obtain both code-owner and release-owner approval.

## Final artifact record

Complete this section only after all source changes and repository integration are finished:

```text
Source repository: <pending>
Source commit: <pending>
Artifact: risingwave-connector-1.0.0.jar
Size: <pending>
SHA-256: <pending>
Manifest Git-Commit: <pending>
Manifest Git-Dirty: false
Build JDK: <pending>
Maven: <pending>
TapData PDK/API: 2.0.8-SNAPSHOT, frozen dependency checksums verified
pgjdbc: 42.7.12
Jackson: 2.18.9
Unit tests: <pending>
Live tests: <pending>
TapData black-box smoke: <pending>
CI/PR: <pending>
```

## Go/no-go rule

Release only when every remaining gate is closed and the archived JAR matches the completed artifact
record. A green source build or an older qualified JAR is not sufficient.
