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

use std::collections::HashSet;

use anyhow::Result;
use risingwave_common::hash::ParallelUnitId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_simulation::cluster::{Cluster, Configuration};

#[madsim::test]
async fn test_cordon_normal() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| {
            worker.r#type() == WorkerType::ComputeNode
                && worker.property.as_ref().unwrap().is_streaming
        })
        .collect();

    let cordoned_worker = workers.pop().unwrap();

    let rest_parallel_unit_ids: HashSet<_> = workers
        .iter()
        .flat_map(|worker| {
            worker
                .parallel_units
                .iter()
                .map(|parallel_unit| parallel_unit.id as ParallelUnitId)
        })
        .collect();

    cluster.cordon_worker(cordoned_worker.id).await?;

    session.run("create table t (v int);").await?;

    let fragments = cluster.locate_fragments([]).await?;

    for fragment in fragments {
        let (_, used) = fragment.parallel_unit_usage();

        assert_eq!(used, rest_parallel_unit_ids);
    }

    session.run("drop table t;").await?;

    cluster.uncordon_worker(cordoned_worker.id).await?;

    session.run("create table t2 (v int);").await?;

    let fragments = cluster.locate_fragments([]).await?;

    for fragment in fragments {
        let (all, used) = fragment.parallel_unit_usage();

        let all: HashSet<_> = all.into_iter().collect();

        assert_eq!(used, all);
    }

    Ok(())
}

#[madsim::test]
async fn test_cordon_no_shuffle_failed() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| {
            worker.r#type() == WorkerType::ComputeNode
                && worker.property.as_ref().unwrap().is_streaming
        })
        .collect();

    session.run("create table t1 (v int);").await?;

    let cordoned_worker = workers.pop().unwrap();

    cluster.cordon_worker(cordoned_worker.id).await?;

    session.run("create table t2 (v int);").await?;

    let result = session
        .run("create materialized view mv1 as select * from t1;")
        .await;

    assert!(result.is_err());

    session
        .run("create materialized view mv2 as select * from t2;")
        .await?;

    cluster.uncordon_worker(cordoned_worker.id).await?;

    session
        .run("create materialized view mv1 as select * from t1;")
        .await?;

    Ok(())
}
