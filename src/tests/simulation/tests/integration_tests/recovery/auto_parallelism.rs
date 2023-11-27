// Copyright 2023 RisingWave Labs
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
use itertools::Itertools;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

/// Please ensure that this value is the same as the one in the `risingwave-auto-scale.toml` file.
const MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE: u64 = 15;

#[tokio::test]
async fn test_passive_online_and_offline() -> Result<()> {
    let config = Configuration::for_auto_scale();
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v1 int);").await?;
    session
        .run("create materialized view m as select count(*) from t;")
        .await?;

    session
        .run("insert into t select * from generate_series(1, 100)")
        .await?;
    session.run("flush").await?;

    sleep(Duration::from_secs(5)).await;

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let single_agg_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("simpleAgg"),
            identity_contains("materialize"),
        ])
        .await?;

    let (_, single_used_parallel_unit_ids) = single_agg_fragment.parallel_unit_usage();

    let used_parallel_unit_id = single_used_parallel_unit_ids.iter().next().unwrap();

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    let prev_workers = workers
        .extract_if(|worker| {
            worker
                .parallel_units
                .iter()
                .map(|parallel_unit| parallel_unit.id)
                .contains(used_parallel_unit_id)
        })
        .collect_vec();

    let prev_worker = prev_workers.into_iter().exactly_one().unwrap();
    let host = prev_worker.host.unwrap().host;
    let host_name = format!("compute-{}", host.split('.').last().unwrap());

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    let initialized_parallelism = used_parallel_units.len();

    assert_eq!(
        initialized_parallelism,
        config.compute_nodes * config.compute_node_cores
    );

    cluster.simple_kill_nodes(vec![host_name.clone()]).await;

    // wait for a while
    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let (_, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(
        initialized_parallelism - config.compute_node_cores,
        used_parallel_units.len()
    );

    let stream_scan_fragment = cluster
        .locate_one_fragment(vec![identity_contains("streamTableScan")])
        .await?;

    let (_, used_parallel_units) = stream_scan_fragment.parallel_unit_usage();

    assert_eq!(
        initialized_parallelism - config.compute_node_cores,
        used_parallel_units.len()
    );

    let single_agg_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("simpleAgg"),
            identity_contains("materialize"),
        ])
        .await?;

    let (_, used_parallel_units_ids) = single_agg_fragment.parallel_unit_usage();

    assert_eq!(used_parallel_units_ids.len(), 1);

    assert_ne!(single_used_parallel_unit_ids, used_parallel_units_ids);

    session
        .run("select count(*) from t")
        .await?
        .assert_result_eq("100");

    session
        .run("select * from m")
        .await?
        .assert_result_eq("100");

    session
        .run("INSERT INTO t select * from generate_series(101, 150)")
        .await?;

    session.run("flush").await?;

    session
        .run("select count(*) from t")
        .await?
        .assert_result_eq("150");

    session
        .run("select * from m")
        .await?
        .assert_result_eq("150");

    cluster.simple_restart_nodes(vec![host_name]).await;

    // wait for a while
    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let (_, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(initialized_parallelism, used_parallel_units.len());

    let stream_scan_fragment = cluster
        .locate_one_fragment(vec![identity_contains("streamTableScan")])
        .await?;

    let (_, used_parallel_units) = stream_scan_fragment.parallel_unit_usage();

    assert_eq!(initialized_parallelism, used_parallel_units.len());
    session
        .run("select count(*) from t")
        .await?
        .assert_result_eq("150");

    session
        .run("select * from m")
        .await?
        .assert_result_eq("150");

    Ok(())
}

#[tokio::test]
async fn test_active_online() -> Result<()> {
    let config = Configuration::for_auto_scale();
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();

    // Keep one worker reserved for adding later.
    cluster
        .simple_kill_nodes(vec!["compute-2".to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    session.run("create table t (v1 int);").await?;
    session
        .run("create materialized view m as select count(*) from t;")
        .await?;

    session
        .run("insert into t select * from generate_series(1, 100)")
        .await?;
    session.run("flush").await?;

    sleep(Duration::from_secs(5)).await;

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    assert_eq!(
        all_parallel_units.len(),
        (config.compute_nodes - 1) * config.compute_node_cores
    );

    cluster
        .simple_restart_nodes(vec!["compute-2".to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    assert_eq!(
        all_parallel_units.len(),
        config.compute_nodes * config.compute_node_cores
    );

    Ok(())
}
