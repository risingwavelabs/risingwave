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

use crate::utils::kill_cn_and_wait_recover;

/// Test batch refresh MV recovery:
///
/// 1. Create a table and insert 10,000 rows.
/// 2. Create a batch refresh MV (`refresh.interval.sec`) with background DDL and rate limit = 1.
/// 3. Verify DDL progress is reported.
/// 4. Trigger recovery and verify progress survives.
/// 5. Alter rate limit to default (unlimited) and wait for completion.
/// 6. Verify the MV row count matches the source table.
#[tokio::test]
async fn test_batch_refresh_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Step 1: Create table and insert 10,000 rows.
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 10000);")
        .await?;
    session.flush().await?;

    // Verify table has 10,000 rows.
    let count = session.run("SELECT COUNT(*) FROM t;").await?;
    assert_eq!(count, "10000");

    // Step 2: Create a batch refresh MV with background DDL and backfill rate limit = 1.
    session.run("SET BACKGROUND_DDL = true;").await?;
    session.run("SET BACKFILL_RATE_LIMIT = 1;").await?;

    // Create the upstream MV first (batch refresh needs an MV as upstream).
    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Wait for upstream MV to finish.
    session.run("WAIT;").await?;

    // Now create the batch refresh MV on top of the upstream MV.
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_batch WITH (refresh.interval.sec = 600) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Step 3: Wait a bit for backfill to make some progress, then check DDL progress.
    sleep(Duration::from_secs(10)).await;

    let progress = session
        .run("SELECT progress FROM rw_catalog.rw_ddl_progress;")
        .await?;
    eprintln!("=== DDL progress before recovery: {}", progress);
    assert!(
        !progress.is_empty(),
        "DDL progress should be reported during batch refresh backfill"
    );

    // Step 4: Trigger recovery and verify progress survives.
    eprintln!("=== Step 4: triggering recovery during backfill...");
    kill_cn_and_wait_recover(&cluster).await;
    eprintln!("=== Step 4: recovery done");

    let progress_after = session
        .run("SELECT progress FROM rw_catalog.rw_ddl_progress;")
        .await?;
    eprintln!("=== DDL progress after recovery: {}", progress_after);
    assert!(
        !progress_after.is_empty(),
        "DDL progress should still be reported after recovery"
    );

    // Step 5: Alter rate limit to default (unlimited).
    eprintln!("=== Step 5: alter rate limit...");
    session
        .run("ALTER MATERIALIZED VIEW mv_batch SET BACKFILL_RATE_LIMIT = DEFAULT;")
        .await?;

    // Step 6: Wait for the batch refresh to complete.
    eprintln!("=== Step 6: WAIT for completion...");
    session.run("WAIT;").await?;
    eprintln!("=== Step 6: done");

    // Step 7: Verify the MV row count matches the source table.
    eprintln!("=== Step 7: verifying row counts...");
    let t_count = session.run("SELECT COUNT(*) FROM t;").await?;
    let mv_count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(
        t_count, mv_count,
        "MV row count should match source table: t={}, mv={}",
        t_count, mv_count
    );
    assert_eq!(mv_count, "10000");
    eprintln!("=== Step 7: PASSED");

    // Step 8: Now the batch refresh job is idle (snapshot done, waiting for next refresh).
    // Verify no DDL progress is reported.
    eprintln!("=== Step 8: checking DDL progress is empty...");
    let progress_idle = session
        .run("SELECT count(*) FROM rw_catalog.rw_ddl_progress;")
        .await?;
    assert_eq!(
        progress_idle, "0",
        "No DDL progress should be reported when batch refresh is idle"
    );
    eprintln!("=== Step 8: PASSED");

    // Step 9: Trigger another recovery while the batch refresh job is idle.
    eprintln!("=== Step 9: triggering recovery while idle...");
    kill_cn_and_wait_recover(&cluster).await;
    eprintln!("=== Step 9: recovery done");

    // Step 10: Verify that the MV is still queryable after idle recovery.
    eprintln!("=== Step 10: querying mv_batch after idle recovery...");
    let mv_count_after_idle = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(
        mv_count_after_idle, "10000",
        "MV should still be queryable after idle recovery"
    );
    eprintln!(
        "=== Step 10: PASSED - query returned {}",
        mv_count_after_idle
    );

    // Step 11: DROP the MV.
    eprintln!("=== Step 11: dropping mv_batch...");
    session.run("DROP MATERIALIZED VIEW mv_batch;").await?;
    eprintln!("=== Step 11: mv_batch dropped");

    // Cleanup
    eprintln!("=== Cleanup: dropping mv_up...");
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    eprintln!("=== Cleanup: mv_up dropped. dropping t...");
    session.run("DROP TABLE t;").await?;
    eprintln!("=== Cleanup: all done");

    Ok(())
}
