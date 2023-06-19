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

use anyhow::Result;
use itertools::Itertools;
use risingwave_common::hash::ParallelUnitId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::get_reschedule_plan_request::Policy::ResizePolicy;
use risingwave_pb::meta::get_reschedule_plan_request::{PbPolicy, PbResizePolicy, WorkerIds};
use risingwave_pb::meta::{PbGetReschedulePlanResponse, PbReschedule};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;

#[madsim::test]
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

    let (_, used_parallel_unit_ids) = join_fragment.parallel_unit_usage();

    let used_parallel_unit_id = used_parallel_unit_ids.iter().next().unwrap();

    let mut workers: Vec<WorkerNode> = cluster
        .list_workers()
        .await
        .unwrap()
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    let retained_worker = workers.pop().unwrap();

    let resp = cluster
        .get_reschedule_plan(ResizePolicy(PbResizePolicy {
            fragment_target_worker_ids: HashMap::from([(
                join_fragment_id,
                WorkerIds {
                    worker_ids: vec![retained_worker.id],
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

#[madsim::test]
async fn test_resize_single() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("create materialized view mv1 as select count(*) from t;")
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
        .list_workers()
        .await
        .unwrap()
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    let prev_workers = workers
        .drain_filter(|worker| {
            worker
                .parallel_units
                .iter()
                .map(|parallel_unit| parallel_unit.id)
                .contains(&used_parallel_unit_id)
        })
        .collect_vec();

    let prev_worker = prev_workers.into_iter().exactly_one().unwrap();

    let resp = cluster
        .get_reschedule_plan(ResizePolicy(PbResizePolicy {
            fragment_target_worker_ids: HashMap::from([(
                agg_fragment_id,
                WorkerIds {
                    worker_ids: workers.iter().map(|worker| worker.id).collect(),
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

#[madsim::test]
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
    session.run("create materialized view mv7 as select mv1.v as mv1v, mv5.v as mv5v from mv1 join mv5 on mv1.v = mv5.v;").await?;

    let chain_fragments: [_; 8] = cluster
        .locate_fragments([identity_contains("chain")])
        .await?
        .try_into()
        .unwrap();

    let selected_fragment = &chain_fragments[0];

    let selected_fragment_id = selected_fragment.inner.fragment_id;

    let workers: Vec<WorkerNode> = cluster
        .list_workers()
        .await?
        .into_iter()
        .filter(|worker: &WorkerNode| worker.r#type() == WorkerType::ComputeNode)
        .collect();

    let selected_worker = &workers[0];

    let selected_worker_id = selected_worker.id;

    // will perform a request with fragment_ids = [selected_fragment.id] and worker_ids =
    // [worker_id]
    let resp = cluster
        .get_reschedule_plan(ResizePolicy(PbResizePolicy {
            fragment_target_worker_ids: HashMap::from([(
                selected_fragment_id,
                WorkerIds {
                    worker_ids: vec![selected_worker_id],
                },
            )]),
        }))
        .await?;

    let reschedules = resp.reschedules;

    let target_plan = reschedules.get(&selected_fragment_id).unwrap();

    for i in 1..chain_fragments.len() as u32 {
        let fragment_id = chain_fragments[i as usize].inner.fragment_id;

        let cloned_plan = reschedules.get(&fragment_id).unwrap();

        assert_eq!(target_plan, cloned_plan);
    }

    Ok(())
}
