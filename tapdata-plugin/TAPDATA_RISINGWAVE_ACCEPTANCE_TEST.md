# TapData RisingWave Connector Acceptance

This is a maintainer and release-owner runbook. Users should start with
[`README.md`](README.md).

Accept one exact JAR, not a version string or a local `target/` directory.

## Release gates

A release candidate must pass:

1. unit tests;
2. live RisingWave integration tests;
3. clean TapData installation of the exact JAR;
4. WebSocket PK, WebSocket JSONB, and JDBC black-box tasks;
5. retained-log snapshot, CDC, and restart recovery;
6. supported-source qualification;
7. lost-ACK, reconnect, payload, and TLS tests;
8. a one-hour representative soak;
9. repository CI and human review.

## 1. Developer regression

```bash
cd tapdata-plugin
mvn clean test
```

Expected:

```text
Tests run: 48, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Run the opt-in live suite against a disposable local RisingWave:

```bash
mvn -Drisingwave.it=true \
  -Dtest=RisingWaveConnectionTestIT \
  test
```

Expected: all live tests pass. The suite creates and drops temporary objects.

## 2. Build and record the candidate

Build from a normal clean clone with the frozen TapData dependencies:

```bash
test -z "$(git status --porcelain)"
git rev-parse HEAD

cd tapdata-plugin
mvn -B -o clean verify

shasum -a 256 target/risingwave-connector-1.0.0.jar
unzip -p target/risingwave-connector-1.0.0.jar META-INF/MANIFEST.MF \
  | grep -E '^(Git-Commit|Git-Dirty|Implementation-Version):'
```

Pass criteria:

- tests pass;
- `Git-Dirty: false`;
- manifest commit equals `git rev-parse HEAD`;
- JAR SHA-256, JDK, Maven, and dependency-bundle checksum are recorded;
- the exact JAR is archived before installation.

Do not rebuild the same released version and replace the archived file.

## 3. Clean TapData installation

Use a clean, approved TapData installation. Get the Access Code from
**Settings -> Access Code**.

```bash
docker cp target/risingwave-connector-1.0.0.jar \
  tapdata:/tmp/risingwave-connector-1.0.0.jar

docker exec tapdata java \
  -jar /opt/app/tapdata/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <access-code> \
  /tmp/risingwave-connector-1.0.0.jar

docker restart tapdata
```

The deployer may instead be under `/tapdata/apps/lib/`.

Pass criteria:

- RisingWave appears once under **Create Connection**;
- version is `1.0.0`;
- type is Target;
- form and help load;
- all three write modes are visible.

When possible, download the stored JAR from TapData and compare its SHA-256 with the candidate.

## 4. Connection tests

Create:

| Name | Write mode |
|---|---|
| `UAT_RW_WebSocket` | WebSocket streaming |
| `UAT_RW_JSONB` | WebSocket JSONB append-only |
| `UAT_RW_JDBC` | JDBC |

Local values when TapData runs in Docker:

```text
Host: host.docker.internal
Port: 4566
Database: dev
Schema: public
User: root
Ingest Endpoint: <blank>
SSL Mode: prefer or disable
```

WebSocket modes must pass:

```text
Connection
Version
schema
Write
ingest_endpoint
```

JDBC must pass the first four items. WebSocket must fail clearly against RisingWave older than 3.0;
JDBC should remain available.

Also test:

- missing schema;
- insufficient DDL/write permission;
- unreachable WebSocket endpoint;
- valid and invalid TLS;
- optional Webhook Secret and RisingWave Secret Name.

Temporary probe objects must be removed after success or failure.

## 5. Three-mode transport smoke

Use a continuous Mock Source with:

```text
id       serial   primary key
payload  rstring
```

Create three full-and-incremental tasks:

| Task | Target | Expected table |
|---|---|---|
| WebSocket PK | `UAT_RW_WebSocket` | typed webhook table |
| JSONB append-only | `UAT_RW_JSONB` | one `data JSONB` column |
| JDBC | `UAT_RW_JDBC` | typed non-webhook table |

At least the WebSocket task must be created under **Data Replication**, not only Data
Transformation.

Query each target twice:

```sql
select count(*) from public.uat_ws_events;
select count(*) from public.uat_jsonb_events;
select count(*) from public.uat_jdbc_events;
```

Pass criteria:

- all tasks remain **Running**;
- all counts increase;
- WebSocket and JDBC rows have non-null unique primary keys;
- JSONB values are non-null JSON objects;
- task logs contain no connector errors.

Mock Source proves transport, not retained-log recovery.

## 6. Retained-log correctness

Use PostgreSQL or another qualified retained-log source. Example PostgreSQL table:

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
   '{"tier":"gold"}', decode('0102','hex')),
  (2, 'Bob', 20.5000, '2026-07-24 09:00:00+08',
   '{"tier":"silver"}', decode('aabb','hex'));
```

Create a **Full & Incremental Sync** task:

```text
PostgreSQL.uat_orders -> UAT_RW_WebSocket.uat_orders
Update condition: id
```

After the snapshot, apply:

```sql
update public.uat_orders
set customer_name = 'Alice updated',
    amount = 11.7500,
    attributes = '{"tier":"platinum"}'
where id = 1;

delete from public.uat_orders where id = 2;

insert into public.uat_orders values
  (3, 'Carol', 30.1250, now(), '{"tier":"gold"}', decode('cafe','hex'));

update public.uat_orders set id = 10 where id = 1;
```

Pass criteria in RisingWave:

- the snapshot values are exact;
- update, delete, and insert are applied;
- old `id=1` is removed and new `id=10` contains the complete row;
- unchanged columns do not become null;
- decimal, timestamp, JSONB, and bytea values round-trip.

Repeat with JDBC using a different target table.

## 7. Stop and restart recovery

1. Stop the retained-log task normally.
2. While stopped, perform one update, delete, and insert at the source.
3. Confirm RisingWave has not applied them.
4. Start the same task without reset.

Pass criteria:

- all stopped-period changes are recovered;
- the task returns to **Running**;
- no full resnapshot is required;
- keyed targets contain no duplicate primary keys.

## 8. Mode boundaries

### WebSocket PK

- requires RisingWave 3.0+ and a primary key;
- insert and update become complete-row upserts;
- primary-key change becomes delete-old plus upsert-new;
- missing old identity, incomplete rows, and unknown relational fields fail;
- a single record larger than 8 MiB fails explicitly.

### JSONB append-only

- keyless inserts succeed;
- the table contains one `data JSONB` column;
- nested objects and arrays keep their shape;
- updates and deletes fail;
- lost-ACK replay may append a documented duplicate.

### JDBC

- insert, update, delete, PK change, and JSONB binding succeed;
- concurrent calls are serialized safely;
- keyless updates require a complete before image.

## 9. Source qualification

| Source | Required test setup |
|---|---|
| PostgreSQL | snapshot, I/U/D, PK change, typed values, restart |
| MySQL 8.4 | `binlog_row_image=FULL`; snapshot, I/U/D, restart |
| MongoDB 7 | `enableFillingModifiedData=true`; update, `$unset`, replace |
| Kafka 3.9.1 | continuous keyless JSONB inserts and restart |
| SQL Server 2022 | database/table Change Tracking; WebSocket and JDBC |
| Oracle 26ai | LogMiner, `autoLog=false`, PK supplemental logging; WebSocket and JDBC |

Do not claim a source outside the tested matrix. SQL Server and Oracle tasks must use the real source
primary key as the update condition.

## 10. Fault, payload, and TLS gates

Verify:

- persisted but lost ACK: keyed replay stays idempotent; JSONB may duplicate;
- reconnect under concurrent table load;
- send timeout and late ACK handling;
- one batch split into ordered frames below 8 MiB;
- one oversized record fails before sending;
- WSS and JDBC TLS succeed with valid trust and fail closed with invalid trust;
- plaintext webhook secret is absent from `SHOW CREATE TABLE`.

Use the repository integration tests and fault proxy for these checks. Do not reproduce them by
modifying a production RisingWave deployment.

## 11. Soak

Run WebSocket PK, JSONB append-only, and JDBC together for at least one hour at a representative
rate. Sample every five minutes.

Record:

- task status and error events;
- start/end row counts;
- primary-key uniqueness and null checks;
- TapData process memory;
- RisingWave errors and reconnects.

Pass criteria:

- all tasks remain **Running**;
- counts grow monotonically;
- no data invariant fails;
- no unbounded memory trend appears;
- no unexplained connector error remains.

## 12. Release decision

Release only when:

- the exact archived JAR passed Sections 1 through 11;
- repository CI is green;
- code-owner and release-owner reviews are complete;
- supported sources and delivery guarantees match the user documentation;
- known limitations are explicit.

The release record should contain only:

```text
Git commit:
JAR SHA-256:
JDK and Maven:
Dependency bundle SHA-256:
Unit/live test results:
Three-mode black-box result:
Source matrix result:
Fault/TLS result:
Soak duration and result:
CI and approvals:
Final decision:
```
