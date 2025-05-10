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
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, KillOpts};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use tokio::time::sleep;

use crate::log_store::utils::*;

// NOTE(kwannoel): To troubleshoot, recommend running with the following logging configuration:
// ```sh
// RUST_LOG='\
//   risingwave_stream::executor::sync_kv_log_store=trace,\
//   integration_tests::log_store::scale=info,\
//   risingwave_stream::common::log_store_impl::kv_log_store=trace\
// '\
// ./risedev sit-test test_recover_synced_log_store >out.log 2>&1
// ```
#[tokio::test]
async fn test_scale_in_synced_log_store() -> Result<()> {
    init_logger();
    let mut cluster = start_sync_log_store_cluster().await?;
    cluster
        .run("alter system set per_database_isolation = false")
        .await?;

    let amplification_factor = 80000;
    let dimension_count = 5;
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
        tracing::info!("ran amplification workload");

        /// Trigger a number of scale operations, with different combinations of nodes
        for (a, b) in (1..=5).tuple_combinations() {
            cluster
                .kill_nodes(vec![format!("compute-{a}"), format!("compute-{b}")], 6)
                .await;
            tracing::info!("killed compute nodes: {a}, {b}");
            cluster.wait_for_recovery().await?;
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
