# TapData RisingWave Connector 1.0.0 Acceptance Test

This document is the end-to-end acceptance procedure for the RisingWave target connector.
It separates developer tests, release qualification, and the smaller set of checks that a user
must perform before accepting one exact connector JAR.

Do not accept a connector by version string alone. Record the exact Git commit and JAR SHA-256.

## 1. Acceptance levels

### Level A: developer regression

Run this after changing connector code:

```bash
cd tapdata-plugin
mvn clean test
```

Expected result:

```text
Tests run: 48, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

This suite covers configuration validation, version parsing, SQL quoting, type conversion, CDC
normalization, payload splitting, ACK failure handling, WebSocket client lifecycle, and JDBC
serialization. It does not prove that a packaged JAR works inside TapData.

### Level B: live RisingWave regression

Run this against a disposable local RisingWave with SQL on `127.0.0.1:4566`, WebSocket ingest on
`127.0.0.1:4560`, database `dev`, schema `public`, and user `root`:

```bash
cd tapdata-plugin
mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test
```

The suite creates and drops temporary objects. Run it only against a disposable database.

Expected result: all 37 live integration tests pass. These tests cover connection pre-checks,
signed webhook DDL, existing-table validation, typed WebSocket writes, JSONB append-only writes,
JDBC writes, CDC operations, concurrency, payload splitting, and scalar values.

### Level C: exact-JAR user acceptance

This is the minimum user-facing acceptance gate. It must use:

- a clean TapData installation;
- the exact JAR intended for distribution;
- a retained-log source such as PostgreSQL, MySQL, SQL Server, or Oracle;
- the TapData **Data Replication** UI, not only a direct Java test or TapFlow transformation;
- direct SQL inspection of the resulting RisingWave tables.

Level C is complete only after Sections 4 through 10 pass.

### Level D: production release qualification

In addition to Level C:

- run the source matrix in Section 12 for every source claimed as supported;
- run lost-ACK and reconnect fault injection;
- run a one-hour or longer soak at a representative rate;
- obtain repository CI, code-owner review, and release-owner approval;
- archive the final JAR and frozen TapData dependency bundle.

## 2. Tests already performed for 1.0.0 RC

The current RC has passed the following tests. These results are supporting evidence; after the
branch is integrated, the final JAR still needs the exact-artifact checks in this document.

| Area | Result |
|---|---|
| Maven unit suite | 48/48 passed |
| Local RisingWave live suite | 37/37 passed |
| Connection Test | WebSocket PK, WebSocket JSONB, and JDBC passed |
| Clean TapData installation | Connector installed and three modes loaded |
| Data Replication UI path | `syncType=migrate` task running and continuously writing |
| WebSocket PK transport | Continuous typed upserts with durable ACK |
| JSONB append-only transport | Continuous keyless JSON documents |
| JDBC transport | Snapshot and CDC writes |
| PostgreSQL | Snapshot, insert, update, delete, stop/restart |
| MySQL 8.4 | FULL row-image update behavior |
| MongoDB 7 | Filled update, `$unset`, and replace semantics |
| Kafka 3.9.1 | Continuous JSONB append-only inserts |
| SQL Server 2022 | Change Tracking through WebSocket and JDBC |
| Oracle 26ai | LogMiner through WebSocket and JDBC |
| JDBC restart recovery | SQL Server update, delete, and insert while stopped were recovered |
| Lost ACK | Keyed replay stayed at one row; JSONB replay appended a duplicate |
| Large batches | Multi-record batches split below 8 MiB |
| TLS | JDBC TLS and WSS success; invalid TLS failed closed |
| Secrets | Catalog Secret reference used; plaintext secret absent from table DDL |
| Packaging | Shaded pgjdbc/Jackson loaded in TapData |

The one-hour three-mode soak is a release gate. Record its final start/end counts, all task states,
TapData memory range, and error count in the acceptance record in Section 14.

## 3. Important scope and boundary conditions

### WebSocket PK mode

- Requires RisingWave 3.0.0 or later.
- Requires a primary key.
- The connector creates a webhook-backed target table.
- Insert and update become complete-row upserts.
- A primary-key change becomes delete-old plus upsert-new.
- A single serialized record larger than 8 MiB fails explicitly.
- Unknown relational columns fail because automatic schema evolution is not supported.

### WebSocket JSONB append-only mode

- Does not require a primary key.
- Creates one `data JSONB` column.
- Accepts inserts only.
- Update and delete must fail explicitly.
- Delivery is at-least-once. A retry after a lost ACK may append a duplicate.
- Arbitrary-precision integer and decimal values are stored as JSON strings.

### JDBC mode

- Uses the PostgreSQL wire protocol for `INSERT`, `UPDATE`, and `DELETE`.
- Can support keyless rows when TapData supplies a complete before image.
- JSONB columns are bound as PostgreSQL `jsonb`, not as `varchar`.
- JDBC is also used in every mode for version, schema, metadata, DDL, and privilege checks.

### Source requirements

| Source | Requirement |
|---|---|
| PostgreSQL | Row images must reconstruct the complete typed row; unavailable TOAST values fail |
| MySQL | `binlog_row_image=FULL` |
| MongoDB | TapData Update Field Completion, `enableFillingModifiedData=true` |
| Kafka | Use JSONB append-only unless a typed CDC contract has been qualified |
| SQL Server | Enable database and table Change Tracking |
| Oracle | ARCHIVELOG, PK supplemental logging, manual LogMiner, `autoLog=false` |

SQL Server and Oracle tasks must use the real source primary key as the target update condition.
Do not leave TapFlow's generic `_id` default.

## 4. Prepare a clean environment

### 4.1 Verify RisingWave

```bash
psql -h 127.0.0.1 -p 4566 -U root -d dev \
  -c "select version();"
```

For local WebSocket tests, verify both ports from the TapData container:

```bash
docker exec <tapdata-container> bash -lc \
  'timeout 3 bash -c "</dev/tcp/host.docker.internal/4566"'

docker exec <tapdata-container> bash -lc \
  'timeout 3 bash -c "</dev/tcp/host.docker.internal/4560"'
```

Both commands must return exit code zero. When TapData runs in Docker, do not configure the target
as `localhost`; use `host.docker.internal` or another address reachable from the container.

### 4.2 Record the candidate artifact

Run from a normal clean clone, not a linked Git worktree:

```bash
test -z "$(git status --porcelain)"
git rev-parse HEAD

cd tapdata-plugin
mvn -B -o clean package

shasum -a 256 target/risingwave-connector-1.0.0.jar
unzip -p target/risingwave-connector-1.0.0.jar META-INF/MANIFEST.MF \
  | grep -E '^(Git-Commit|Git-Dirty|Implementation-Version):'
```

Pass criteria:

- `Git-Dirty: false`;
- manifest `Git-Commit` equals `git rev-parse HEAD`;
- SHA-256 is saved in the release record;
- the build used the frozen TapData dependency set.

### 4.3 Start a clean TapData

Use the approved TapData image or installation. Pin production images by digest. The examples below
assume:

```text
TapData UI/API: http://localhost:3030
TapData container: tapdata
```

Confirm that no previous RisingWave connector or acceptance tasks remain.

## 5. Install the exact JAR

Get the Access Code from **Settings -> Access Code**, then run:

```bash
docker cp target/risingwave-connector-1.0.0.jar \
  tapdata:/tmp/risingwave-connector-1.0.0.jar

docker exec tapdata java -jar /tapdata/apps/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <access-code> \
  -l \
  /tmp/risingwave-connector-1.0.0.jar

docker restart tapdata
```

Open **Connections -> Create Connection**.

Pass criteria:

- RisingWave appears once with the expected icon;
- connector version is `1.0.0`;
- the form and right-side help load;
- the type is Target;
- three write modes are visible.

## 6. Create and test three RisingWave connections

Create these target connections:

| Name | Write mode |
|---|---|
| `UAT_RW_WebSocket` | WebSocket streaming |
| `UAT_RW_JSONB` | WebSocket JSONB append-only |
| `UAT_RW_JDBC` | JDBC |

Local values:

```text
Host: host.docker.internal
Port: 4566
Database: dev
Schema: public
User: root
Password: <blank for local development>
Ingest Endpoint: <blank, or ws://host.docker.internal:4560>
SSL Mode: disable or prefer
```

For each connection, click **Test**.

Expected WebSocket result:

```text
Connection       Pass
Version          Pass
schema           Pass
Write            Pass
ingest_endpoint  Pass
```

Expected JDBC result:

```text
Connection  Pass
Version     Pass
schema      Pass
Write       Pass
```

The WebSocket version test must fail clearly against RisingWave older than 3.0.0. JDBC mode should
remain available for an older compatible RisingWave.

## 7. Quick transport smoke with Mock Source

Mock Source is appropriate for continuous transport and throughput checks. It is not the durability
oracle for stop/restart, and its synthetic `rnumber`/`now` fields should not be used to qualify
typed decimal or timezone behavior.

Create a small mock table:

```text
Table: uat_mock_events
id       serial   primary key
payload  rstring
```

Create three full-and-incremental tasks:

| Task | Target | Expected target |
|---|---|---|
| `UAT_WS_Mock` | `UAT_RW_WebSocket` | typed webhook table |
| `UAT_JSONB_Mock` | `UAT_RW_JSONB` | one `data JSONB` column |
| `UAT_JDBC_Mock` | `UAT_RW_JDBC` | typed non-webhook table |

Use **Data Replication -> Create Task** for at least the WebSocket task. Confirm that it appears
under Data Replication, not only Data Transformation.

Verify counts twice:

```sql
select count(*) from public.uat_ws_mock;
select count(*) from public.uat_jsonb_mock;
select count(*) from public.uat_jdbc_mock;
```

Wait five to ten seconds and query again.

Pass criteria:

- all tasks stay Running;
- all counts increase;
- WebSocket PK rows have non-null `id` and `payload`;
- JSONB rows contain the source document under `data`;
- no connector error is shown in task logs.

Do not use abrupt high-rate Mock Source stop behavior as a checkpoint correctness test. Mock data is
not retained like a database WAL, binlog, Change Tracking log, or LogMiner log.

## 8. Retained-log correctness test

Use PostgreSQL for the default manual test, or substitute another qualified retained-log source.
Configure PostgreSQL logical replication before creating the TapData connection.

Example source table:

```sql
create table public.uat_orders (
  id bigint primary key,
  customer_name text,
  amount numeric(20,4),
  updated_at timestamptz,
  attributes jsonb,
  payload bytea
);

alter table public.uat_orders replica identity full;

insert into public.uat_orders values
  (1, 'Alice', 10.2500, '2026-07-24 08:00:00+08',
   '{"tier":"gold","enabled":true}', decode('0102','hex')),
  (2, 'Bob', 20.5000, '2026-07-24 09:00:00+08',
   '{"tier":"silver","enabled":false}', decode('aabb','hex'));
```

Create a **Full & Incremental Sync** Data Replication task:

```text
PostgreSQL.uat_orders -> UAT_RW_WebSocket.uat_orders
Update condition: id
```

After the initial snapshot, verify:

```sql
select id, customer_name, amount, updated_at, attributes,
       encode(payload, 'hex') as payload_hex
from public.uat_orders
order by id;
```

Apply incremental operations at the source:

```sql
update public.uat_orders
set customer_name = 'Alice updated',
    amount = 11.7500,
    attributes = '{"tier":"platinum","enabled":true}'
where id = 1;

delete from public.uat_orders where id = 2;

insert into public.uat_orders values
  (3, 'Carol', 30.1250, now(),
   '{"tier":"gold","enabled":true}', decode('cafe','hex'));
```

Pass criteria in RisingWave:

- `id=1` contains the updated values;
- `id=2` no longer exists;
- `id=3` exists with exact decimal, JSONB, timestamp, and binary values;
- no unchanged column becomes unexpectedly null.

Test a primary-key change:

```sql
update public.uat_orders set id = 10 where id = 1;
```

Pass criteria:

- old `id=1` is removed;
- new `id=10` contains the complete row;
- the total row count does not increase due to a stale old identity.

Repeat this section with `UAT_RW_JDBC` and a different target table, such as
`uat_orders_jdbc`.

## 9. Stop and restart recovery

Use the retained-log task from Section 8.

1. Stop the task normally in TapData.
2. Confirm its status is Stopped.
3. While stopped, perform one update, one delete, and one insert at the source.
4. Confirm RisingWave has not applied those changes.
5. Start the same task without resetting it.
6. Wait for incremental delay to return to normal.

Example stopped-period changes:

```sql
update public.uat_orders
set customer_name = 'changed while stopped'
where id = 10;

delete from public.uat_orders where id = 3;

insert into public.uat_orders values
  (4, 'Dave', 40.0000, now(),
   '{"tier":"bronze"}', decode('0404','hex'));
```

Pass criteria after restart:

- `id=10` is updated;
- `id=3` is deleted;
- `id=4` is inserted;
- the task returns to Running;
- no task reset or full resnapshot is required.

## 10. JSONB append-only behavior

Use a keyless source or Kafka/Mock source and the `UAT_RW_JSONB` connection.

Verify the physical table:

```sql
show create table public.uat_jsonb_events;
select jsonb_typeof(data), data
from public.uat_jsonb_events
limit 10;
```

Pass criteria:

- the table has one `data JSONB` column;
- nested objects and arrays preserve their JSON shape;
- arbitrary-precision numbers follow the documented string representation;
- continuous insert events are accepted.

Boundary check:

- sending an update or delete must fail explicitly;
- retry after a lost ACK may append a duplicate and is not an exactly-once failure.

## 11. Secret and DDL inspection

Create a WebSocket connection with:

```text
Webhook Secret: <test-value>
RisingWave Secret Name: uat_webhook_secret
```

Run Connection Test, then create a task/table.

Inspect RisingWave:

```sql
show secrets;
show create table public.uat_orders;
```

Pass criteria:

- `public.uat_webhook_secret` exists;
- table DDL uses `VALIDATE SECRET`;
- the plaintext secret value does not appear in `SHOW CREATE TABLE`;
- signed WebSocket Connection Test passes;
- changing the configured value rotates the connector-managed Secret.

## 12. Supported source qualification matrix

Run this section only for sources that will be included in the production support statement.

| Source | Required cases |
|---|---|
| PostgreSQL | snapshot, I/U/D, PK change, timestamptz, numeric, JSONB, bytea, restart |
| MySQL | verify `binlog_row_image=FULL`, snapshot, I/U/D, PK change, decimal/time, restart |
| MongoDB | filling enabled, `$set`, top-level `$unset`, replace, nested document, restart |
| Kafka JSONB | continuous keyless inserts, nested JSON, restart, duplicate boundary documented |
| SQL Server | Change Tracking, snapshot, I/U/D, PK change, common types, restart |
| Oracle | LogMiner, snapshot, I/U/D, PK change, number/timestamp/raw, restart |

For each relational source, run both WebSocket PK and JDBC unless the release statement explicitly
limits that source to one mode.

## 13. Automated boundary tests

These cases normally remain automated and do not need to be repeated through the UI for every RC:

| Boundary | Expected result |
|---|---|
| RisingWave version below 3.0 | WebSocket Connection Test fails; JDBC remains available |
| Missing schema | schema test fails and write probe is skipped |
| Missing write/DDL privilege | Write test fails clearly |
| Unreachable WebSocket endpoint | ingest endpoint test fails and probe table is cleaned |
| Existing non-webhook table in WebSocket mode | rejected |
| Existing table with wrong PK/type | rejected |
| Keyless model in WebSocket PK mode | rejected before table creation |
| Keyless model in JSONB mode | accepted for inserts |
| JSONB update/delete | rejected |
| Replace event | `after` is authoritative; omitted known fields become null |
| Top-level removed field | target column becomes null |
| Missing old mutable PK identity | fails closed |
| Unknown relational field | fails closed; no silent schema evolution |
| Composite PK change | delete-old plus upsert-new |
| Concurrent JDBC writes | serialized on shared connection |
| Insert followed by dependent update/delete | required RisingWave FLUSH ordering |
| Batch above 8 MiB | split into ordered frames |
| One record above 8 MiB | rejected explicitly |
| Invalid/unknown ACK | pending writes fail rather than timing out silently |
| Persisted but lost ACK | keyed replay idempotent; JSONB may duplicate |
| Repeated lost ACK/reconnect | client recreated and subsequent writes recover |
| WSS trusted certificate | succeeds |
| Invalid TLS endpoint/certificate | fails closed |

Run fault injection when changing `WsIngestClient`, ACK handling, retry behavior, or replay semantics:

```bash
python3 scripts/ws_ack_drop_proxy.py

mvn -Drisingwave.it=true \
  -Drisingwave.ackLossProxyEndpoint=ws://127.0.0.1:4561 \
  -Dtest=RisingWaveAckLossIT test
```

## 14. Soak and performance acceptance

Use three continuously writing tasks:

- WebSocket PK;
- WebSocket JSONB append-only;
- JDBC.

Recommended minimum:

```text
Duration: 1 hour
Sample interval: 5 minutes
Rate: representative of the first production workload
```

Record at every sample:

- task status;
- row count for all three targets;
- TapData CPU and memory;
- connector error/retry count;
- RisingWave errors.

Pass criteria:

- every task stays Running;
- every row count increases at each sample;
- no unexplained connector error, retry loop, or stalled task;
- memory reaches a plateau rather than growing without bound;
- throughput does not collapse over time;
- final row/value checks pass.

The included benchmark can compare local transport throughput:

```bash
mvn -Drisingwave.it=true -Drisingwave.benchmark=true \
  -Drisingwave.benchmark.rows=10000 \
  -Drisingwave.benchmark.batchSize=1000 \
  -Dtest=RisingWaveWriteBenchmarkIT test
```

Do not turn one local benchmark ratio into a universal product claim. Network, source rate, record
width, TapData batching, and RisingWave deployment size all affect the result.

## 15. User acceptance decision

A user can accept the connector for a stated source and workload when all of these are true:

- the exact clean JAR and SHA-256 are recorded;
- the connector installs cleanly in the approved TapData version;
- all three Connection Tests pass;
- a real Data Replication WebSocket PK task passes snapshot, I/U/D, PK change, and restart;
- JSONB append-only and JDBC smoke tasks pass;
- types used by that user are compared directly in RisingWave;
- the source-specific requirement in Section 3 is enabled;
- a representative soak passes;
- known at-least-once, schema evolution, keyless, and 8 MiB boundaries are accepted;
- no support claim is made for an unqualified source or type;
- CI and required reviews pass for the exact source commit.

The user does not need to rerun every automated fault test. The user-facing acceptance focuses on
the installed artifact and their actual source contract. Connector maintainers remain responsible
for the automated edge-condition suite.

## 16. Acceptance record template

Copy and complete this block for the release:

```text
Date:
Tester:
TapData version/image digest:
RisingWave version/deployment:
Source type/version:

Git commit:
JAR filename:
JAR SHA-256:
Manifest Git-Commit:
Manifest Git-Dirty:
JDK:
Maven:
Frozen TapData dependency checksum:

Unit tests:
Live integration tests:
Connection Test WebSocket PK:
Connection Test JSONB:
Connection Test JDBC:
Data Replication WebSocket snapshot/CDC:
Primary-key change:
Stop/restart:
JSONB append-only:
JDBC:
Webhook Secret inspection:

Soak duration:
Start rows WS/JSONB/JDBC:
End rows WS/JSONB/JDBC:
TapData memory start/max/end:
Task errors/retries:

Qualified sources:
Accepted limitations:
PR/CI:
Reviewer:

Decision: GO / NO-GO
Blocking issues:
```

## 17. Inspect the current local RC environment

The current local acceptance environment uses:

```text
TapData UI: http://localhost:3130
RisingWave SQL: 127.0.0.1:4566
RisingWave WebSocket ingest: 127.0.0.1:4560
```

Open the TapData UI and inspect:

```text
Data Replication:
  RC_Data_Replication_WebSocket_PK

Data Transformation:
  RC_JSONB_AppendOnly_Smoke
  RC_JDBC_Smoke
```

All three should show Running during the soak. The first task must appear under Data Replication.

Inspect the target tables:

```sql
select count(*) from public.rc_ws_events;
select count(*) from public.rc_jsonb_events;
select count(*) from public.rc_jdbc_events;

select id, payload
from public.rc_ws_events
order by id desc
limit 10;

select data
from public.rc_jsonb_events
limit 10;

show create table public.rc_ws_events;
show create table public.rc_jsonb_events;
show create table public.rc_jdbc_events;
```

Run the three count queries again after five to ten seconds. Every count must increase.

The current SQL Server retained-log JDBC recovery result can be inspected with:

```sql
select id, customer_name, amount, status, active,
       encode(payload, 'hex') as payload_hex
from public.orders_jdbc_sqlserver
order by id;
```

Expected rows:

```text
id=5   customer_name='Eve stopped'  status='jdbc_resume_update'
id=70  customer_name='Grace'        status='jdbc_resume_insert'
```

The previous `id=30` row must be absent. This is the update/delete/insert recovery applied after
the JDBC task was stopped and restarted.

The current soak samples are written to:

```text
/tmp/tapdata-rc-acceptance/soak-1h.tsv
```

The completed local RC soak result is:

```text
Duration: 2026-07-24 15:59:25 +08:00 to 17:00:21 +08:00
Samples: 13 at approximately five-minute intervals
Start rows WS/JSONB/JDBC: 49,219 / 15,079 / 7,185
End rows WS/JSONB/JDBC: 410,969 / 376,859 / 281,386
Row deltas WS/JSONB/JDBC: 361,750 / 361,780 / 274,201
Task state: 13/13 samples Running for all three tasks
Row progression: monotonically increasing for all three targets
TapData memory: 6.343 GiB start, 6.944 GiB at one hour, 6.952 GiB ten minutes later
Task error events: none
```

Final target invariants also passed: keyed WebSocket and JDBC rows had non-null, unique primary
keys; JSONB rows were non-null JSON objects. The ten-minute post-soak memory sample changed by only
0.008 GiB, consistent with the runtime reaching a plateau rather than continuing the earlier JVM
warm-up growth.
