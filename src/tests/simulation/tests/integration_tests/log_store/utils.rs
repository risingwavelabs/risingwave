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
use std::time::Duration;

use anyhow::Result;
use madsim::time::sleep;
use risingwave_common::bail;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};

pub(crate) async fn start_sync_log_store_cluster() -> Result<Cluster> {
    // Create a cluster with 3 nodes

    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!(
            "../../../../../config/ci-sim-log-store.toml"
        ))
        .expect("failed to write config file");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 5,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        ..Default::default()
    })
    .await
}

pub(crate) async fn setup_base_tables(
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
    let fact_workload = format!(
        "\
        INSERT INTO fact select \
          gen.id as id, \
          gen.id % {dimension_count} as dim_id, \
          gen.id as part_id, \
          gen.id as f1 \
          from generate_series(1, {fact_count}) as gen(id)"
    );

    session.run_all(create_relations).await?;
    session.run(fact_workload).await?;
    session.flush().await?;

    Ok(())
}

pub(crate) async fn setup_mv(
    cluster: &mut Cluster,
    name: &str,
    use_unaligned_join: bool,
) -> Result<()> {
    let mut session = cluster.start_session();
    let create_relations = vec![
        format!("SET streaming_enable_unaligned_join = {use_unaligned_join}"),
        format!(
            "CREATE MATERIALIZED VIEW {name} AS SELECT part_id, count(*) as cnt FROM fact join dimension on fact.dim_id = dimension.id GROUP BY part_id"
        ),
    ];
    session.run_all(create_relations).await?;
    Ok(())
}

pub(crate) async fn run_amplification_workload(
    cluster: &mut Cluster,
    dimension_count: usize,
) -> Result<()> {
    let mut session = cluster.start_session();
    let dim_workload = format!(
        "INSERT INTO dimension select gen.id - 1 as id, 1 as d1 from generate_series(1, {dimension_count}) as gen(id)"
    );
    session.run(dim_workload).await?;
    session.flush().await?;

    Ok(())
}

pub(crate) async fn delete_amplification_workload(cluster: &mut Cluster) -> Result<()> {
    let mut session = cluster.start_session();
    let dim_workload = format!("DELETE FROM dimension");
    session.run(dim_workload).await?;
    session.flush().await?;
    Ok(())
}

pub(crate) async fn get_mv_count(cluster: &mut Cluster, name: &str) -> Result<usize> {
    let mut session = cluster.start_session();
    let query = format!("SELECT COUNT(*) FROM {name}");
    let result = session.run(query).await?;
    let count: usize = result.parse()?;
    Ok(count)
}

pub(crate) async fn assert_lag_in_log_store(
    cluster: &mut Cluster,
    name: &str,
    result_count: usize,
) -> Result<()> {
    let mut session = cluster.start_session();
    let query = format!("SELECT COUNT(*) FROM {name}");
    let result = session.run(query).await?;
    let current_count: usize = result.parse()?;
    tracing::info!("current count: {current_count}");
    if current_count != result_count {
        return Ok(());
    }
    bail!("there was no lag in the logstore")
}

/// Wait for it to finish consuming logstore
pub(crate) async fn wait_unaligned_join(
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
        sleep(Duration::from_millis(1)).await;
    }
    // In a subsequent step we will compare the results, and print the missing records.
    tracing::error!(
        "failed after {MAX_RETRIES} retries, expected {result_count} but got {current_count}"
    );
    Ok(())
}
