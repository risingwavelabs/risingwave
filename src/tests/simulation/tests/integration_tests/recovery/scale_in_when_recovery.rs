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

#[tokio::test]
async fn test_scale_in_when_recovery() -> Result<()> {
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

    // ensure the restart delay is longer than config in `risingwave-auto-scale.toml`
    let restart_delay = 30;

    cluster
        .kill_nodes_and_restart(vec![host_name], restart_delay)
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

    let chain_fragment = cluster
        .locate_one_fragment(vec![identity_contains("chain")])
        .await?;

    let (_, used_parallel_units) = chain_fragment.parallel_unit_usage();

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

    Ok(())
}
