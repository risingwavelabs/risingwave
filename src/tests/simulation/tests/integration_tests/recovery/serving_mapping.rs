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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time::sleep;

/// Test that the serving vnode mapping is available to the frontend even before
/// the barrier manager finishes recovery.
///
/// The serving mapping should be derived from the persistent fragment catalog
/// rather than from the in-memory `shared_actor_infos` (which is only populated
/// after a successful recovery round).
///
/// Setup: two compute nodes with separate roles:
///   - `compute-1`: streaming only (handles actors, participates in recovery)
///   - `compute-2`: serving only (handles batch queries)
///
/// Steps:
/// 1. Start cluster, create a table, insert data, verify query works.
/// 2. Kill all nodes (simulating a full crash).
/// 3. Restart meta first.
/// 4. Restart only the **serving** CN (`compute-2`), but NOT the streaming CN.
///    Recovery cannot complete because the streaming CN is still down.
///    However, `WorkerNodeActivated` for the serving CN should trigger
///    `fetch_serving_infos`, which (with the fix) reads from the DB.
/// 5. Restart frontend.
/// 6. Query the table — should succeed because the serving mapping was derived
///    from the persistent catalog, not from `shared_actor_infos`.
///
/// Without the fix (reading from `shared_actor_infos`), step 6 fails with
/// "Streaming vnode mapping not found" because `shared_actor_infos` is empty
/// (recovery never completed — the streaming CN is still down).
#[tokio::test]
async fn test_serving_mapping_available_before_recovery() -> Result<()> {
    let config = {
        let mut c = Configuration::for_background_ddl();
        // Two compute nodes: streaming-only + serving-only.
        c.compute_nodes = 2;
        c.compute_node_roles =
            HashMap::from([(1, "streaming".to_owned()), (2, "serving".to_owned())]);
        c
    };
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    // Step 1: Create a table and insert data.
    session.run("CREATE TABLE t_serve(v1 int);").await?;
    session
        .run("INSERT INTO t_serve SELECT * FROM generate_series(1, 1000);")
        .await?;
    session.flush().await?;

    // Verify the table is queryable.
    let count = session.run("SELECT COUNT(*) FROM t_serve;").await?;
    assert_eq!(count, "1000");

    // Step 2: Kill all nodes.
    cluster
        .simple_kill_nodes(["compute-1", "compute-2", "meta-1", "frontend-1"])
        .await;

    // Step 3: Restart meta first.
    // At this point shared_actor_infos is empty; recovery cannot proceed
    // without the streaming CN.
    cluster.simple_restart_nodes(["meta-1"]).await;
    sleep(Duration::from_secs(5)).await;

    // Step 4: Restart ONLY the serving CN (compute-2).
    // The streaming CN (compute-1) stays down, so recovery cannot complete
    // and shared_actor_infos remains empty.
    // With the fix: serving mapping reads from DB → mapping available.
    // Without the fix: serving mapping reads from shared_actor_infos (empty) → empty.
    cluster.simple_restart_nodes(["compute-2"]).await;
    sleep(Duration::from_secs(5)).await;

    // Step 5: Restart frontend.
    cluster.simple_restart_nodes(["frontend-1"]).await;
    sleep(Duration::from_secs(5)).await;

    // Step 6: Query the table.
    // This should succeed because the serving mapping was derived from the DB.
    let mut new_session = cluster.start_session();
    let count = new_session.run("SELECT COUNT(*) FROM t_serve;").await?;
    assert_eq!(
        count, "1000",
        "Query should succeed: serving mapping must be available before recovery completes"
    );

    // Cleanup: bring streaming CN back so DROP can proceed.
    cluster.simple_restart_nodes(["compute-1"]).await;
    sleep(Duration::from_secs(10)).await;

    let mut cleanup_session = cluster.start_session();
    cleanup_session.run("DROP TABLE t_serve;").await?;

    Ok(())
}
