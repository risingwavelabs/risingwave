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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::Fragment;
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

/// Please ensure that this value is the same as the one in the `risingwave-auto-scale.toml` file.
pub const MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE: u64 = 15;

#[tokio::test]
async fn test_passive_online_and_offline() -> Result<()> {
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        true,
    );
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

    assert_eq!(single_agg_fragment.parallelism(), 1);

    let used_worker_slots = single_agg_fragment.used_worker_count();

    let (single_used_worker_id, should_be_one) =
        used_worker_slots.into_iter().exactly_one().unwrap();

    assert_eq!(should_be_one, 1);

    let worker_map: HashMap<_, _> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .map(|worker| (worker.id, worker))
        .collect();

    let prev_worker = worker_map.get(&single_used_worker_id).unwrap();
    let host = prev_worker.clone().host.unwrap().host;
    let host_name = format!("compute-{}", host.split('.').next_back().unwrap());

    let all_worker_slots = table_mat_fragment.all_worker_count();
    let used_worker_slots = table_mat_fragment.used_worker_count();
    assert_eq!(all_worker_slots, used_worker_slots);

    let initialized_parallelism = table_mat_fragment.parallelism();

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

    assert_eq!(
        initialized_parallelism - config.compute_node_cores,
        table_mat_fragment.parallelism()
    );

    let stream_scan_fragment = cluster
        .locate_one_fragment(vec![identity_contains("streamTableScan")])
        .await?;

    assert_eq!(
        initialized_parallelism - config.compute_node_cores,
        stream_scan_fragment.parallelism()
    );

    let single_agg_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("simpleAgg"),
            identity_contains("materialize"),
        ])
        .await?;

    let used_worker_slots = single_agg_fragment.used_worker_count();

    let (curr_used_worker_id, should_be_one) = used_worker_slots.into_iter().exactly_one().unwrap();
    assert_eq!(should_be_one, 1);
    assert_ne!(single_used_worker_id, curr_used_worker_id);
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

    assert_eq!(initialized_parallelism, table_mat_fragment.parallelism());

    let stream_scan_fragment = cluster
        .locate_one_fragment(vec![identity_contains("streamTableScan")])
        .await?;

    assert_eq!(initialized_parallelism, stream_scan_fragment.parallelism());

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
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        true,
    );
    let mut cluster = Cluster::start(config.clone()).await?;

    // Keep one worker reserved for adding later.
    cluster
        .simple_kill_nodes(vec!["compute-2".to_owned()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

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

    let x = session
        .run("select * from rw_streaming_parallelism")
        .await?;
    println!("parallel xxx {}", x);

    let all_worker_slots = table_mat_fragment.all_worker_count();

    let used_worker_slots = table_mat_fragment.used_worker_count();

    println!("all worker {:#?}", all_worker_slots);

    assert_eq!(all_worker_slots, used_worker_slots);

    assert_eq!(all_worker_slots.len(), config.compute_nodes - 1);

    println!("before restart node");
    cluster
        .simple_restart_nodes(vec!["compute-2".to_owned()])
        .await;

    println!("after restart node");
    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    println!("after sleep");

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            no_identity_contains("simpleAgg"),
        ])
        .await?;

    let all_worker_slots = table_mat_fragment.all_worker_count();

    let used_worker_slots = table_mat_fragment.used_worker_count();

    assert_eq!(all_worker_slots, used_worker_slots);
    assert_eq!(all_worker_slots.len(), config.compute_nodes);

    Ok(())
}

#[tokio::test]
async fn test_no_parallelism_control_with_fixed_and_auto() -> Result<()> {
    test_auto_parallelism_control_with_fixed_and_auto_helper(false).await
}

#[tokio::test]
async fn test_auto_parallelism_control_with_fixed_and_auto() -> Result<()> {
    test_auto_parallelism_control_with_fixed_and_auto_helper(true).await
}

async fn test_auto_parallelism_control_with_fixed_and_auto_helper(
    enable_auto_parallelism_control: bool,
) -> Result<()> {
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        enable_auto_parallelism_control,
    );
    let mut cluster = Cluster::start(config.clone()).await?;

    // Keep one worker reserved for adding later.
    let select_worker = "compute-2";
    cluster
        .simple_kill_nodes(vec![select_worker.to_owned()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let mut session = cluster.start_session();

    session.run("create table t (v1 int);").await?;

    session
        .run("select parallelism from rw_table_fragments")
        .await?
        .assert_result_eq("ADAPTIVE");

    async fn locate_table_fragment(cluster: &mut Cluster) -> Result<Fragment> {
        cluster
            .locate_one_fragment(vec![
                identity_contains("materialize"),
                no_identity_contains("simpleAgg"),
            ])
            .await
    }

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let all_worker_slots = table_mat_fragment.all_worker_count();
    let used_worker_slots = table_mat_fragment.used_worker_count();

    assert_eq!(all_worker_slots, used_worker_slots);
    assert_eq!(all_worker_slots.len(), config.compute_nodes - 1);

    session.run("alter table t set parallelism = 3").await?;

    session
        .run("select parallelism from rw_table_fragments")
        .await?
        .assert_result_eq("FIXED(3)");

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    assert_eq!(table_mat_fragment.parallelism(), 3);

    // Keep one worker reserved for adding later.
    cluster
        .simple_restart_nodes(vec![select_worker.to_owned()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    // Now we have 3 nodes
    let workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    assert_eq!(workers.len(), 3);

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let used_worker_slots = table_mat_fragment.used_worker_count();

    assert_eq!(table_mat_fragment.parallelism(), 3);

    // check auto scale out for fixed
    if enable_auto_parallelism_control {
        assert_eq!(used_worker_slots.len(), config.compute_nodes);
    } else {
        // no rebalance process
        assert_eq!(used_worker_slots.len(), config.compute_nodes - 1);
    }

    // We kill compute-2 again to verify the behavior of auto scale-in
    cluster
        .simple_kill_nodes(vec![select_worker.to_owned()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let used_worker_slots = table_mat_fragment.used_worker_count();

    assert_eq!(table_mat_fragment.parallelism(), 3);

    assert_eq!(used_worker_slots.len(), config.compute_nodes - 1);

    // We alter parallelism back to auto

    session
        .run("alter table t set parallelism = adaptive")
        .await?;

    session
        .run("select parallelism from rw_table_fragments")
        .await?
        .assert_result_eq("ADAPTIVE");

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let all_worker_slots = table_mat_fragment.all_worker_count();
    let used_worker_slots = table_mat_fragment.used_worker_count();

    assert_eq!(all_worker_slots, used_worker_slots);
    assert_eq!(all_worker_slots.len(), config.compute_nodes - 1);

    // Keep one worker reserved for adding later.
    cluster
        .simple_restart_nodes(vec![select_worker.to_owned()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let all_worker_slots = table_mat_fragment.all_worker_count();
    let used_worker_slots = table_mat_fragment.used_worker_count();

    // check auto scale out for auto
    if enable_auto_parallelism_control {
        assert_eq!(all_worker_slots, used_worker_slots);
        assert_eq!(all_worker_slots.len(), config.compute_nodes);
    } else {
        assert_eq!(used_worker_slots.len(), config.compute_nodes - 1);
    }

    Ok(())
}
