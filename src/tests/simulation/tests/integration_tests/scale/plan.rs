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

use std::collections::HashMap;
use std::default::Default;

use anyhow::Result;
use itertools::Itertools;
use rand::seq::SliceRandom;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::get_reschedule_plan_request::Policy::StableResizePolicy;
use risingwave_pb::meta::get_reschedule_plan_request::{
    PbPolicy, PbStableResizePolicy, WorkerChanges,
};
use risingwave_pb::meta::PbReschedule;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};

#[tokio::test]
async fn test_resize_normal() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t1 (v1 int);").await?;
    session.run("create table t2 (v2 int);").await?;
    session
        .run("create materialized view mv as select * from t1 join t2 on t1.v1 = t2.v2;")
        .await?;

    let join_fragment = cluster
        .locate_one_fragment([
            identity_contains("hashJoin"),
            identity_contains("materialize"),
        ])
        .await?;

    let join_fragment_id = join_fragment.inner.fragment_id;

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    workers.pop();

    let removed_workers = workers.iter().map(|worker| worker.id).collect_vec();

    let resp = cluster
        .get_reschedule_plan(PbPolicy::StableResizePolicy(PbStableResizePolicy {
            fragment_worker_changes: HashMap::from([(
                join_fragment_id,
                WorkerChanges {
                    include_worker_ids: vec![],
                    exclude_worker_ids: removed_workers,
                    ..Default::default()
                },
            )]),
        }))
        .await?;

    let reschedules = resp.reschedules;
    assert_eq!(reschedules.len(), 1);
    let target_plan: PbReschedule = reschedules.get(&join_fragment_id).unwrap().clone();

    assert_eq!(target_plan.added_parallel_units.len(), 0);

    let removed_parallel_unit_id = workers
        .iter()
        .flat_map(|worker| {
            worker
                .parallel_units
                .iter()
                .map(|parallel_unit| parallel_unit.id)
        })
        .sorted()
        .collect_vec();

    assert_eq!(target_plan.removed_parallel_units, removed_parallel_unit_id);

    Ok(())
}
#[tokio::test]
async fn test_resize_single() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("create materialized view mv1 as select count(*) from t;")
        .await?;

    session
        .run("create materialized view mv2 as select * from mv1;")
        .await?;

    let agg_fragment = cluster
        .locate_one_fragment([
            identity_contains("simpleAgg"),
            identity_contains("materialize"),
        ])
        .await?;

    let agg_fragment_id = agg_fragment.inner.fragment_id;

    let (_, used_parallel_unit_ids) = agg_fragment.parallel_unit_usage();

    assert_eq!(used_parallel_unit_ids.len(), 1);

    let used_parallel_unit_id = used_parallel_unit_ids.iter().next().unwrap();

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

    let resp = cluster
        .get_reschedule_plan(StableResizePolicy(PbStableResizePolicy {
            fragment_worker_changes: HashMap::from([(
                agg_fragment_id,
                WorkerChanges {
                    include_worker_ids: vec![],
                    exclude_worker_ids: vec![prev_worker.id],
                    ..Default::default()
                },
            )]),
        }))
        .await?;

    let reschedules = resp.reschedules;
    assert_eq!(reschedules.len(), 1);
    let target_plan: PbReschedule = reschedules.get(&agg_fragment_id).unwrap().clone();
    assert_eq!(target_plan.added_parallel_units.len(), 1);
    assert_eq!(target_plan.removed_parallel_units.len(), 1);

    let removed_parallel_unit_id = target_plan
        .removed_parallel_units
        .iter()
        .exactly_one()
        .unwrap();

    assert!(prev_worker
        .parallel_units
        .iter()
        .map(|parallel_unit| parallel_unit.id)
        .contains(removed_parallel_unit_id));

    Ok(())
}

#[tokio::test]
async fn test_resize_single_failed() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("create materialized view mv1 as select count(*) from t;")
        .await?;

    session
        .run("create materialized view mv2 as select * from mv1;")
        .await?;

    let upstream_fragment = cluster
        .locate_one_fragment([
            identity_contains("simpleAgg"),
            identity_contains("materialize"),
        ])
        .await?;

    let upstream_fragment_id = upstream_fragment.inner.fragment_id;

    let downstream_fragment = cluster
        .locate_one_fragment([identity_contains("chain"), identity_contains("materialize")])
        .await?;

    let downstream_fragment_id = downstream_fragment.inner.fragment_id;

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    let worker_a = workers.pop().unwrap();
    let worker_b = workers.pop().unwrap();

    let resp = cluster
        .get_reschedule_plan(StableResizePolicy(PbStableResizePolicy {
            fragment_worker_changes: HashMap::from([
                (
                    upstream_fragment_id,
                    WorkerChanges {
                        include_worker_ids: vec![],
                        exclude_worker_ids: vec![worker_a.id],
                        ..Default::default()
                    },
                ),
                (
                    downstream_fragment_id,
                    WorkerChanges {
                        include_worker_ids: vec![],
                        exclude_worker_ids: vec![worker_b.id],
                        ..Default::default()
                    },
                ),
            ]),
        }))
        .await;

    assert!(resp.is_err());

    Ok(())
}
#[tokio::test]
async fn test_resize_no_shuffle() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("create materialized view mv1 as select * from t;")
        .await?;
    session
        .run("create materialized view mv2 as select * from t;")
        .await?;
    session
        .run("create materialized view mv3 as select * from mv2;")
        .await?;
    session
        .run("create materialized view mv4 as select * from mv2;")
        .await?;
    session
        .run("create materialized view mv5 as select * from mv3;")
        .await?;
    session
        .run("create materialized view mv6 as select * from mv3;")
        .await?;
    session
        .run(
            "create materialized view mv7 as select mv1.v as mv1v, mv5.v as mv5v from mv1
join mv5 on mv1.v = mv5.v;",
        )
        .await?;

    let chain_fragments: [_; 8] = cluster
        .locate_fragments([identity_contains("chain")])
        .await?
        .try_into()
        .unwrap();

    let selected_fragment = chain_fragments.choose(&mut rand::thread_rng()).unwrap();

    let selected_fragment_id = selected_fragment.inner.fragment_id;

    let mut workers: Vec<WorkerNode> = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .filter(|worker: &WorkerNode| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    workers.pop();

    let removed_worker_ids = workers.iter().map(|worker| worker.id).collect_vec();

    let resp = cluster
        .get_reschedule_plan(PbPolicy::StableResizePolicy(PbStableResizePolicy {
            fragment_worker_changes: HashMap::from([(
                selected_fragment_id,
                WorkerChanges {
                    include_worker_ids: vec![],
                    exclude_worker_ids: removed_worker_ids,
                    ..Default::default()
                },
            )]),
        }))
        .await?;

    let reschedules = resp.reschedules;

    assert_eq!(reschedules.len(), 1);

    let top_materialize_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("chain"),
            no_identity_contains("hashJoin"),
        ])
        .await?;

    let top_materialize_fragment_id = reschedules.keys().exactly_one().cloned().unwrap();

    assert_eq!(
        top_materialize_fragment_id,
        top_materialize_fragment.inner.fragment_id
    );

    Ok(())
}
