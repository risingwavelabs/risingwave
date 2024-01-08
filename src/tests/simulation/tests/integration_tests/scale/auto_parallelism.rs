// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::ctl_ext::Fragment;
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

/// Please ensure that this value is the same as the one in the `risingwave-auto-scale.toml` file.
const MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE: u64 = 15;

#[tokio::test]
async fn test_passive_online_and_offline() -> Result<()> {
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        true,
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
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        false,
        true,
    );
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
        true,
        enable_auto_parallelism_control,
    );
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();

    // Keep one worker reserved for adding later.
    let select_worker = "compute-2";
    cluster
        .simple_kill_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

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

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    assert_eq!(
        all_parallel_units.len(),
        (config.compute_nodes - 1) * config.compute_node_cores
    );

    session.run("alter table t set parallelism = 3").await?;

    session
        .run("select parallelism from rw_table_fragments")
        .await?
        .assert_result_eq("FIXED(3)");

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let (_, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(used_parallel_units.len(), 3);

    // Keep one worker reserved for adding later.
    cluster
        .simple_restart_nodes(vec![select_worker.to_string()])
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

    let parallel_unit_to_worker = workers
        .into_iter()
        .flat_map(|worker| {
            worker
                .parallel_units
                .into_iter()
                .map(move |parallel_unit| (parallel_unit.id, worker.id))
        })
        .collect::<HashMap<_, _>>();

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let (_, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(used_parallel_units.len(), 3);

    let worker_ids: HashSet<_> = used_parallel_units
        .iter()
        .map(|id| parallel_unit_to_worker.get(id).unwrap())
        .collect();

    // check auto scale out for fixed
    if enable_auto_parallelism_control {
        assert_eq!(worker_ids.len(), config.compute_nodes);
    } else {
        // no rebalance process
        assert_eq!(worker_ids.len(), config.compute_nodes - 1);
    }

    // We kill compute-2 again to verify the behavior of auto scale-in
    cluster
        .simple_kill_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let (_, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(used_parallel_units.len(), 3);

    let worker_ids: HashSet<_> = used_parallel_units
        .iter()
        .map(|id| parallel_unit_to_worker.get(id).unwrap())
        .collect();

    assert_eq!(worker_ids.len(), config.compute_nodes - 1);

    // We alter parallelism back to auto

    session.run("alter table t set parallelism = adaptive").await?;

    session
        .run("select parallelism from rw_table_fragments")
        .await?
        .assert_result_eq("ADAPTIVE");

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    assert_eq!(all_parallel_units.len(), used_parallel_units.len());

    assert_eq!(
        all_parallel_units.len(),
        (config.compute_nodes - 1) * config.compute_node_cores
    );

    // Keep one worker reserved for adding later.
    cluster
        .simple_restart_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let table_mat_fragment = locate_table_fragment(&mut cluster).await?;

    let (all_parallel_units, used_parallel_units) = table_mat_fragment.parallel_unit_usage();

    // check auto scale out for auto
    if enable_auto_parallelism_control {
        assert_eq!(all_parallel_units.len(), used_parallel_units.len());

        assert_eq!(
            all_parallel_units.len(),
            config.compute_nodes * config.compute_node_cores
        );
    } else {
        assert_eq!(
            used_parallel_units.len(),
            (config.compute_nodes - 1) * config.compute_node_cores
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_compatibility_with_low_level() -> Result<()> {
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        false,
        true,
    );
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();
    session
        .run("SET streaming_enable_arrangement_backfill = false;")
        .await?;

    // Keep one worker reserved for adding later.
    let select_worker = "compute-2";
    cluster
        .simple_kill_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    session.run("create table t(v int);").await?;

    // single fragment downstream
    session
        .run("create materialized view m_simple as select * from t;")
        .await?;

    // multi fragment downstream
    session
        .run("create materialized view m_join as select t1.v as t1v, t2.v as t2v from t t1, t t2 where t1.v = t2.v;")
        .await?;

    session
        .run("select parallelism from table_parallelism")
        .await?
        .assert_result_eq("ADAPTIVE");

    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("union"),
        ])
        .await?;

    let (mut all_parallel_units, _) = table_mat_fragment.parallel_unit_usage();

    let chosen_parallel_unit_a = all_parallel_units.pop().unwrap();
    let chosen_parallel_unit_b = all_parallel_units.pop().unwrap();

    let table_mat_fragment_id = table_mat_fragment.id();

    // manual scale in table materialize fragment
    cluster
        .reschedule(format!(
            "{table_mat_fragment_id}-[{chosen_parallel_unit_a}]",
        ))
        .await?;

    session
        .run("select parallelism from table_parallelism")
        .await?
        .assert_result_eq("CUSTOM");

    session
        .run("select parallelism from mview_parallelism where name = 'm_simple'")
        .await?
        .assert_result_eq("ADAPTIVE");

    let simple_mv_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("StreamTableScan"),
        ])
        .await?;

    let simple_mv_fragment_id = simple_mv_fragment.id();

    // manual scale in m_simple materialize fragment
    cluster
        .reschedule_resolve_no_shuffle(format!(
            "{simple_mv_fragment_id}-[{chosen_parallel_unit_b}]",
        ))
        .await?;

    // Since `m_simple` only has 1 fragment, and this fragment is a downstream of NO_SHUFFLE relation,
    // in reality, `m_simple` does not have a fragment of its own.
    // Therefore, any low-level modifications to this fragment will only be passed up to the highest level through the NO_SHUFFLE relationship and then passed back down.
    // Hence, the parallelism of `m_simple` should still be equivalent to ADAPTIVE of 0 fragment.
    session
        .run("select parallelism from mview_parallelism where name = 'm_simple'")
        .await?
        .assert_result_eq("ADAPTIVE");

    session
        .run("select parallelism from mview_parallelism where name = 'm_join'")
        .await?
        .assert_result_eq("ADAPTIVE");

    let hash_join_fragment = cluster
        .locate_one_fragment(vec![identity_contains("hashJoin")])
        .await?;

    let hash_join_fragment_id = hash_join_fragment.id();

    // manual scale in m_join materialize fragment
    cluster
        .reschedule_resolve_no_shuffle(format!(
            "{hash_join_fragment_id}-[{chosen_parallel_unit_a}]"
        ))
        .await?;

    session
        .run("select parallelism from mview_parallelism where name = 'm_join'")
        .await?
        .assert_result_eq("CUSTOM");

    let before_fragment_parallelism = session
        .run("select fragment_id, parallelism from rw_fragments order by fragment_id;")
        .await?;

    cluster
        .simple_restart_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let after_fragment_parallelism = session
        .run("select fragment_id, parallelism from rw_fragments order by fragment_id;")
        .await?;

    assert_eq!(before_fragment_parallelism, after_fragment_parallelism);

    Ok(())
}

#[tokio::test]
async fn test_compatibility_with_low_level_and_arrangement_backfill() -> Result<()> {
    let config = Configuration::for_auto_parallelism(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE,
        false,
        true,
    );
    let mut cluster = Cluster::start(config.clone()).await?;
    let mut session = cluster.start_session();

    // Keep one worker reserved for adding later.
    let select_worker = "compute-2";
    cluster
        .simple_kill_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    session.run("create table t(v int);").await?;

    // Streaming arrangement backfill
    session
        .run("SET streaming_enable_arrangement_backfill = true;")
        .await?;
    session
        .run("create materialized view m_simple as select * from t;")
        .await?;

    session
        .run("select parallelism from table_parallelism")
        .await?
        .assert_result_eq("AUTO");

    // Find the table materialize fragment
    let table_mat_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("union"),
        ])
        .await?;

    let (mut all_parallel_units, _) = table_mat_fragment.parallel_unit_usage();

    let chosen_parallel_unit_a = all_parallel_units.pop().unwrap();
    let chosen_parallel_unit_b = all_parallel_units.pop().unwrap();

    let table_mat_fragment_id = table_mat_fragment.id();

    // manual scale in table materialize fragment
    cluster
        .reschedule(format!(
            "{table_mat_fragment_id}-[{chosen_parallel_unit_a}]",
        ))
        .await?;

    session
        .run("select parallelism from table_parallelism")
        .await?
        .assert_result_eq("CUSTOM");

    // Upstream changes should not affect downstream.
    session
        .run("select parallelism from mview_parallelism where name = 'm_simple'")
        .await?
        .assert_result_eq("AUTO");

    // Find the table fragment for materialized view
    let simple_mv_fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("StreamTableScan"),
        ])
        .await?;

    let simple_mv_fragment_id = simple_mv_fragment.id();

    // manual scale in m_simple materialize fragment
    cluster
        .reschedule_resolve_no_shuffle(format!(
            "{simple_mv_fragment_id}-[{chosen_parallel_unit_b}]",
        ))
        .await?;

    // The downstream table fragment should be separate from the upstream table fragment.
    session
        .run("select parallelism from mview_parallelism where name = 'm_simple'")
        .await?
        .assert_result_eq("CUSTOM");

    let before_fragment_parallelism = session
        .run("select fragment_id, parallelism from rw_fragments order by fragment_id;")
        .await?;

    cluster
        .simple_restart_nodes(vec![select_worker.to_string()])
        .await;

    sleep(Duration::from_secs(
        MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 2,
    ))
    .await;

    let after_fragment_parallelism = session
        .run("select fragment_id, parallelism from rw_fragments order by fragment_id;")
        .await?;

    assert_eq!(before_fragment_parallelism, after_fragment_parallelism);

    Ok(())
}
