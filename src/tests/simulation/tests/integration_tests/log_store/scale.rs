// Copyright 2025 RisingWave Labs
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

use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::runtime::init_logger;
use risingwave_common::hash::WorkerSlotId;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, KillOpts, Session};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use tokio::time::sleep;

use crate::log_store::utils::*;

// NOTE(kwannoel): To troubleshoot, recommend running with the following logging configuration:
// ```sh
// RUST_MIN_STACK=21470000
// RUST_LOG='\
//   risingwave_stream::executor::sync_kv_log_store=trace,\
//   integration_tests::log_store::scale=info,\
//   risingwave_stream::common::log_store_impl::kv_log_store=trace\
// ' ./risedev sit-test test_scale_in_synced_log_store >out.log 2>&1
// ```
// This test will take a long time to run locally,
// so it is recommended to run it in a CI environment.
#[tokio::test]
async fn test_scale_in_synced_log_store() -> Result<()> {
    init_logger();
    let mut cluster = start_sync_log_store_cluster().await?;
    cluster
        .run("alter system set per_database_isolation = false")
        .await?;

    let amplification_factor = 60000;
    let dimension_count = 20;
    let result_count = amplification_factor * dimension_count;

    tracing::info!("setup cluster");

    const UNALIGNED_MV_NAME: &str = "unaligned_mv";
    const ALIGNED_MV_NAME: &str = "aligned_mv";

    // unaligned join workload
    {
        setup_base_tables(&mut cluster, amplification_factor, dimension_count).await?;
        setup_mv(&mut cluster, UNALIGNED_MV_NAME, true).await?;
        tracing::info!("setup tables and mv");
        run_amplification_workload(&mut cluster, dimension_count).await?;
        tracing::info!("ran initial amplification workload");
        assert_lag_in_log_store(&mut cluster, UNALIGNED_MV_NAME, result_count).await?;

        async fn assert_parallelism_eq(session: &mut Session, parallelism: usize) {
            let parallelism_sql = format!(
                "select count(parallelism) filter (where parallelism != {parallelism})\
                from (select count(*) parallelism from rw_actors group by fragment_id);"
            );
            let result = session.run(parallelism_sql).await.unwrap();
            let parallelism_count: usize = result.parse().unwrap();
            if parallelism_count != 0 {
                let result = session.run("select fragment_id, count(*) as parallelism from rw_actors group by fragment_id;").await.unwrap();
                panic!("parallelism is not equal to {parallelism}: {result}");
            }
        }

        /// Trigger a number of scale operations, with different combinations of nodes
        for (a, b) in (1..=2).tuple_combinations() {
            tracing::info!("running delete amplification workload");
            delete_amplification_workload(&mut cluster).await?;
            tracing::info!("ran delete amplification workload");

            let node_name_a = format!("compute-{a}");
            let node_name_b = format!("compute-{b}");
            let nodes = vec![node_name_a.clone(), node_name_b.clone()];

            let mut session = cluster.start_session();
            assert_parallelism_eq(&mut session, 10).await;
            cluster.simple_kill_nodes(&nodes).await;
            tracing::info!("killed compute nodes: {node_name_a}, {node_name_b}");

            cluster.wait_for_recovery().await?;
            assert_lag_in_log_store(&mut cluster, UNALIGNED_MV_NAME, result_count).await?;
            assert_parallelism_eq(&mut session, 6).await;
            tracing::info!("cluster scaled to 6 parallelism and recovered");

            cluster.simple_restart_nodes(&nodes).await;
            tracing::info!("restarted compute nodes: {node_name_a}, {node_name_b}");

            assert_lag_in_log_store(&mut cluster, UNALIGNED_MV_NAME, result_count).await?;
            cluster.wait_for_recovery().await?;
            tracing::info!("cluster recovered");

            cluster.wait_for_scale(10).await?;
            assert_parallelism_eq(&mut session, 10).await;
            assert_lag_in_log_store(&mut cluster, UNALIGNED_MV_NAME, result_count).await?;
            tracing::info!("cluster scaled back to 10 parallelism");

            run_amplification_workload(&mut cluster, dimension_count).await?;
            tracing::info!("ran insert amplification workload");
        }

        wait_unaligned_join(&mut cluster, UNALIGNED_MV_NAME, result_count).await?;
    }

    // aligned join workload
    setup_mv(&mut cluster, ALIGNED_MV_NAME, false).await?;
    let count = get_mv_count(&mut cluster, ALIGNED_MV_NAME).await?;
    assert_eq!(count, result_count);

    // compare results
    let mut first = ALIGNED_MV_NAME;
    let mut second = UNALIGNED_MV_NAME;
    for i in 0..2 {
        let compare_sql = format!("select * from {first} except select * from {second}");
        let mut session = cluster.start_session();
        let result = session.run(compare_sql).await?;
        if !result.is_empty() {
            panic!("{second} missing the following results from {first}: {result}");
        }
        std::mem::swap(&mut first, &mut second);
    }

    Ok(())
}
