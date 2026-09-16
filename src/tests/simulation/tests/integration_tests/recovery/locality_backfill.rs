// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use anyhow::{Result, bail};
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

const SET_LOCALITY_BACKFILL: &str = "SET enable_locality_backfill = true;";
const SET_LOCALITY_BACKFILL_ALWAYS: &str = "SET locality_backfill_mode = always;";
const SET_BACKGROUND_DDL: &str = "SET background_ddl = true;";
const SET_RATE_LIMIT_1: &str = "SET backfill_rate_limit = 1;";
const CREATE_TABLE: &str = "CREATE TABLE t(a int);";
const SEED_TABLE: &str = "INSERT INTO t SELECT * FROM generate_series(1, 10000, 1);";
const CREATE_MV: &str = "CREATE MATERIALIZED VIEW mv AS SELECT count(*) FROM t GROUP BY a;";
const ALTER_RATE_LIMIT_DEFAULT: &str =
    "ALTER MATERIALIZED VIEW mv SET BACKFILL_RATE_LIMIT = DEFAULT;";
const WAIT: &str = "WAIT;";
const WAIT_INTERNAL_STATE_SECS: u64 = 60;
const MV_RATE_LIMITS: &str = "SELECT node_name, rate_limit FROM rw_catalog.rw_rate_limit \
     JOIN rw_catalog.rw_relations ON table_id = id WHERE name = 'mv' ORDER BY node_name;";

async fn wait_internal_table_name(
    session: &mut Session,
    table_name_pattern: &str,
    context: &str,
) -> Result<String> {
    for _ in 0..WAIT_INTERNAL_STATE_SECS * 10 {
        let table_name = session
            .run(&format!(
                "SELECT name FROM rw_internal_tables WHERE name LIKE '{}' LIMIT 1;",
                table_name_pattern
            ))
            .await?;
        if !table_name.is_empty() {
            return Ok(table_name);
        }

        sleep(Duration::from_millis(100)).await;
    }

    bail!("Internal table should exist {}", context);
}

async fn wait_internal_state_table_non_empty(
    session: &mut Session,
    state_table_name: &str,
    context: &str,
) -> Result<i64> {
    for _ in 0..WAIT_INTERNAL_STATE_SECS * 10 {
        let state_count = session
            .run(&format!("SELECT COUNT(*) FROM {};", state_table_name))
            .await?;
        let state_count_val = state_count.parse::<i64>()?;
        if state_count_val > 0 {
            return Ok(state_count_val);
        }

        sleep(Duration::from_millis(100)).await;
    }

    bail!("State table should have rows {}, got 0", context);
}

/// Test that locality backfill internal tables behave correctly during recovery.
/// This test verifies:
/// 1. State table is non-empty during backfill (contains vnode positions)
/// 2. Progress table is empty during backfill
/// 3. State and progress tables maintain correct state after recovery
/// 4. MV completes successfully after recovery
#[tokio::test]
async fn test_locality_backfill_recovery_internal_tables() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Step 1: Enable locality backfill and configure for slow background DDL
    session.run(SET_LOCALITY_BACKFILL).await?;
    session.run(SET_LOCALITY_BACKFILL_ALWAYS).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session.run(SET_RATE_LIMIT_1).await?;

    // Step 2: Create table and populate with 10,000 rows
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE).await?;
    session.flush().await?;

    // Step 3: Verify row count
    let count = session.run("SELECT COUNT(*) FROM t;").await?;
    assert_eq!(count, "10000", "Table should have 10000 rows");

    // Step 4: Create materialized view with background DDL
    session.run(CREATE_MV).await?;

    // Step 5: Find the internal table names for locality provider
    let state_table_name = wait_internal_table_name(
        &mut session,
        "%localityproviderstate%",
        "for locality provider state",
    )
    .await?;
    let progress_table_name = wait_internal_table_name(
        &mut session,
        "%localityproviderprogress%",
        "for locality provider progress",
    )
    .await?;

    // Step 6: Check internal tables before recovery
    // State table should be non-empty (contains vnode positions during backfill)
    wait_internal_state_table_non_empty(&mut session, &state_table_name, "during backfill").await?;

    // Progress table should be empty
    let progress_count = session
        .run(&format!("SELECT COUNT(*) FROM {};", progress_table_name))
        .await?;
    assert_eq!(
        progress_count, "0",
        "Progress table should be empty during backfill"
    );

    // Step 7: Trigger recovery
    cluster.run("RECOVER").await?;

    // Step 8: Check internal tables after recovery
    // State table should still be non-empty
    wait_internal_state_table_non_empty(&mut session, &state_table_name, "after recovery").await?;

    // Progress table should still be empty
    let progress_count_after = session
        .run(&format!("SELECT COUNT(*) FROM {};", progress_table_name))
        .await?;
    assert_eq!(
        progress_count_after, "0",
        "Progress table should still be empty after recovery"
    );

    // Step 9: Remove rate limit and wait for completion
    session.run(ALTER_RATE_LIMIT_DEFAULT).await?;
    session.run(WAIT).await?;

    // Step 10: Verify MV result
    // Each value from 1 to 10000 appears once, so count(*) group by a should give 10000 rows with count=1
    let mv_count = session.run("SELECT COUNT(*) FROM mv;").await?;
    assert_eq!(
        mv_count, "10000",
        "MV should have 10000 rows (one per distinct value of a)"
    );

    // Verify all counts are 1
    let max_count = session.run("SELECT MAX(count) FROM mv;").await?;
    assert_eq!(max_count, "1", "All counts should be 1");

    let min_count = session.run("SELECT MIN(count) FROM mv;").await?;
    assert_eq!(min_count, "1", "All counts should be 1");

    // Cleanup
    session.run("DROP MATERIALIZED VIEW mv;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}

async fn drained_rows(session: &mut Session, progress_table: &str) -> Result<u64> {
    let sum = session
        .run(&format!(
            "SELECT coalesce(sum(row_count), 0) FROM {progress_table};"
        ))
        .await?;
    Ok(sum.parse()?)
}

async fn wait_drain_started(session: &mut Session) -> Result<String> {
    let progress_table = wait_internal_table_name(
        session,
        "%localityproviderprogress%",
        "for locality provider progress",
    )
    .await?;
    for _ in 0..180 {
        if drained_rows(session, &progress_table).await? > 0 {
            return Ok(progress_table);
        }
        sleep(Duration::from_secs(1)).await;
    }
    bail!("locality backfill did not start draining");
}

#[tokio::test]
async fn test_locality_backfill_rate_limit() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(SET_LOCALITY_BACKFILL).await?;
    session.run(SET_LOCALITY_BACKFILL_ALWAYS).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session.run("SET backfill_rate_limit = 10;").await?;
    session.run(CREATE_TABLE).await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 500);")
        .await?;
    session.flush().await?;
    session.run(CREATE_MV).await?;
    assert_eq!(
        session.run(MV_RATE_LIMITS).await?,
        "LOCALITY_PROVIDER 10\nSTREAM_SCAN 10"
    );

    let progress_table = wait_drain_started(&mut session).await?;
    session
        .run("ALTER MATERIALIZED VIEW mv SET BACKFILL_RATE_LIMIT = 0;")
        .await?;
    sleep(Duration::from_secs(3)).await;
    let frozen = drained_rows(&mut session, &progress_table).await?;
    sleep(Duration::from_secs(10)).await;
    assert_eq!(
        drained_rows(&mut session, &progress_table).await?,
        frozen,
        "locality backfill drained at rate limit 0"
    );

    session
        .run("ALTER MATERIALIZED VIEW mv SET BACKFILL_RATE_LIMIT = 10;")
        .await?;
    sleep(Duration::from_secs(5)).await;
    assert!(
        drained_rows(&mut session, &progress_table).await? > frozen,
        "locality backfill did not resume"
    );

    session.run(ALTER_RATE_LIMIT_DEFAULT).await?;
    // Fragments without a rate limit are not listed.
    assert_eq!(session.run(MV_RATE_LIMITS).await?, "");
    session.run(WAIT).await?;
    assert_eq!(session.run("SELECT count(*) FROM mv;").await?, "500");
    Ok(())
}
