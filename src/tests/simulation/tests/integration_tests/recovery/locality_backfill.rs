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

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time::sleep;

const SET_LOCALITY_BACKFILL: &str = "SET enable_locality_backfill = true;";
const SET_BACKGROUND_DDL: &str = "SET background_ddl = true;";
const SET_RATE_LIMIT_1: &str = "SET backfill_rate_limit = 1;";
const CREATE_TABLE: &str = "CREATE TABLE t(a int);";
const SEED_TABLE: &str = "INSERT INTO t SELECT * FROM generate_series(1, 10000, 1);";
const CREATE_MV: &str = "CREATE MATERIALIZED VIEW mv AS SELECT count(*) FROM t GROUP BY a;";
const ALTER_RATE_LIMIT_DEFAULT: &str =
    "ALTER MATERIALIZED VIEW mv SET BACKFILL_RATE_LIMIT = DEFAULT;";
const WAIT: &str = "WAIT;";

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
    let state_table_name = session
        .run("SELECT name FROM rw_internal_tables WHERE name LIKE '%localityproviderstate%';")
        .await?;
    let progress_table_name = session
        .run("SELECT name FROM rw_internal_tables WHERE name LIKE '%localityproviderprogress%';")
        .await?;

    assert!(!state_table_name.is_empty(), "State table should exist");
    assert!(
        !progress_table_name.is_empty(),
        "Progress table should exist"
    );

    // Step 6: Wait for some backfill progress
    sleep(Duration::from_secs(5)).await;

    // Step 7: Check internal tables before recovery
    // State table should be non-empty (contains vnode positions during backfill)
    let state_count = session
        .run(&format!("SELECT COUNT(*) FROM {};", state_table_name))
        .await?;
    let state_count_val = state_count.parse::<i64>()?;
    assert!(
        state_count_val > 0,
        "State table should have rows during backfill, got {}",
        state_count_val
    );

    // Progress table should be empty
    let progress_count = session
        .run(&format!("SELECT COUNT(*) FROM {};", progress_table_name))
        .await?;
    assert_eq!(
        progress_count, "0",
        "Progress table should be empty during backfill"
    );

    // Step 8: Trigger recovery
    cluster.run("RECOVER").await?;
    sleep(Duration::from_secs(10)).await;

    // Step 9: Check internal tables after recovery
    // State table should still be non-empty
    let state_count_after = session
        .run(&format!("SELECT COUNT(*) FROM {};", state_table_name))
        .await?;
    let state_count_after_val = state_count_after.parse::<i64>()?;
    assert!(
        state_count_after_val > 0,
        "State table should have rows after recovery, got {}",
        state_count_after_val
    );

    // Progress table should still be empty
    let progress_count_after = session
        .run(&format!("SELECT COUNT(*) FROM {};", progress_table_name))
        .await?;
    assert_eq!(
        progress_count_after, "0",
        "Progress table should still be empty after recovery"
    );

    // Step 10: Remove rate limit and wait for completion
    session.run(ALTER_RATE_LIMIT_DEFAULT).await?;
    session.run(WAIT).await?;

    // Step 11: Verify MV result
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
