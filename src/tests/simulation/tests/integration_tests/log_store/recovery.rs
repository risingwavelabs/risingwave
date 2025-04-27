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

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use rand::{Rng, rng as thread_rng};
use risingwave_common::bail;
use risingwave_common::hash::WorkerSlotId;
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use tokio::time::sleep;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, KillOpts};
use madsim::runtime::init_logger;

async fn start_sync_log_store_cluster() -> Result<Cluster> {
    // Create a cluster with 3 nodes

    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        ..Default::default()
    })
        .await
}

// NOTE(kwannoel): To troubleshoot, recommend running with the following logging configuration:
// ```sh
// RUST_LOG='\
//   risingwave_stream::executor::sync_kv_log_store=trace,\
//   integration_tests::log_store::recovery=info,\
//   risingwave_stream::common::log_store_impl::kv_log_store=trace\
// '\
// ./risedev sit-test test_recover_synced_log_store >out.log 2>&1
// ```
#[tokio::test]
async fn test_recover_synced_log_store() -> Result<()> {
    async fn setup_base_tables(
        cluster: &mut Cluster,
        amplification_factor: usize,
        dimension_count: usize,
    ) -> Result<()> {
        let fact_count = dimension_count * amplification_factor;
        let mut session = cluster.start_session();
        let create_relations = {
            vec![
                "CREATE TABLE dimension (id INT, d1 INT)",
                "CREATE TABLE fact (id INT, dim_id INT, part_id INT, f1 INT)",
            ]
        };
        let fact_workload = format!("\
        INSERT INTO fact select \
          gen.id as id, \
          gen.id % {dimension_count} as dim_id, \
          gen.id as part_id, \
          gen.id as f1 \
          from generate_series(1, {fact_count}) as gen(id)");

        session.run_all(create_relations).await?;
        session.run(fact_workload).await?;
        session.flush().await?;

        Ok(())
    }

    async fn setup_mv(
        cluster: &mut Cluster,
        name: &str,
        use_unaligned_join: bool,
    ) -> Result<()> {
        let mut session = cluster.start_session();
        let create_relations = vec![
            format!("SET streaming_enable_unaligned_join = {use_unaligned_join}"),
            format!("CREATE MATERIALIZED VIEW {name} AS SELECT part_id, count(*) as cnt FROM fact join dimension on fact.dim_id = dimension.id GROUP BY part_id")
        ];
        session.run_all(create_relations).await?;
        Ok(())
    }

    async fn run_amplification_workload(
        cluster: &mut Cluster,
        dimension_count: usize,
    ) -> Result<()> {
        let mut session = cluster.start_session();
        let dim_workload = format!("INSERT INTO dimension select gen.id - 1 as id, 1 as d1 from generate_series(1, {dimension_count}) as gen(id)");
        session.run(dim_workload).await?;
        session.flush().await?;

        Ok(())
    }

    async fn get_mv_count(
        cluster: &mut Cluster,
        name: &str,
    ) -> Result<usize> {
        let mut session = cluster.start_session();
        let query = format!("SELECT COUNT(*) FROM {name}");
        let result = session.run(query).await?;
        let count: usize = result.parse()?;
        Ok(count)
    }

    /// Wait for it to finish consuming logstore
    async fn wait_unaligned_join(
        cluster: &mut Cluster,
        name: &str,
        result_count: usize,
    ) -> Result<()> {
        let mut session = cluster.start_session();
        let query = format!("SELECT COUNT(*) FROM {name}");
        const MAX_RETRIES: usize = 100;
        let mut current_count = 0;
        for i in 0..MAX_RETRIES {
            let result = session.run(query.clone()).await?;
            current_count = result.parse()?;
            tracing::info!("current count: {current_count}");
            if current_count == result_count {
                if i == 0 {
                    // If count is immediately equal to result_count,
                    // it likely means there's no lag in the logstore.
                    // This is a failure case, as we expect some lag.
                    bail!("there was no lag in the logstore")
                }
                return Ok(());
            }
            sleep(Duration::from_secs(1)).await;
        }
        // In a subsequent step we will compare the results, and print the missing records.
        tracing::error!("failed after {MAX_RETRIES} retries, expected {result_count} but got {current_count}");
        Ok(())
    }

    init_logger();
    let mut cluster = start_sync_log_store_cluster().await?;
    cluster.run("alter system set per_database_isolation = false").await?;

    let amplification_factor = 20000;
    let dimension_count = 5;
    let result_count = amplification_factor * dimension_count;

    const UNALIGNED_MV_NAME: &str = "unaligned_mv";
    const ALIGNED_MV_NAME: &str = "aligned_mv";


    // unaligned join workload
    {
        setup_base_tables(&mut cluster, amplification_factor, dimension_count).await?;
        setup_mv(&mut cluster, UNALIGNED_MV_NAME, true).await?;
        run_amplification_workload(&mut cluster, dimension_count).await?;

        cluster.kill_nodes(vec!["compute-1", "compute-2", "compute-3"], 5).await;
        tracing::info!("killed compute nodes");
        cluster.wait_for_recovery().await?;
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
