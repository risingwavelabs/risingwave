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

#![cfg(madsim)]

use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::time::{sleep, Instant};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::{Actor, Fragment, ParallelUnit, Status, WorkerNode};
use risingwave_pb::meta::table_fragments::fragment;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_simulation::cluster::{Configuration, KillOpts};
use risingwave_simulation::ctl_ext::predicate;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;
use tracing_subscriber::fmt::format;

/// create cluster, cordon node, run query. Cordoned node should NOT contain actors
async fn test_cordon(create: &str, select: &str, drop: &str, number_of_nodes: usize) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: Is the create statement also dropped? After drop will there be zero actors in cluster?
    // TODO: maybe get the expected result in a different cluster
    // setup cluster and calc expected result
    let sleep_sec = 20;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false)
            .await
            .expect("creating cluster failed");
    cluster.run(create).await.expect("failed to run create");
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await.expect("failed to run select");
    cluster.run(drop).await.expect("failed to run drop");
    sleep(Duration::from_secs(sleep_sec)).await;

    // cordon random nodes
    let mut cordoned_nodes: Vec<WorkerNode> = vec![];
    // TODO: I can directly write this in the cordoned_nodes ved
    let rand_nodes: Vec<WorkerNode> = cluster
        .cordon_random_workers(number_of_nodes) // TODO:  sometimes collect same node twice. This is not what we want!
        .await
        .expect("cordoning node failed");
    for rand_node in rand_nodes {
        println!("Unregister compute node {:?}", rand_node); // TODO: remove line
        cordoned_nodes.push(rand_node);
    }
    let cordoned_nodes_ids = cordoned_nodes.iter().map(|n| n.id).collect_vec();

    // compare results
    sleep(Duration::from_secs(sleep_sec)).await;
    let got = cluster.run(select).await.expect("failed to run select");
    assert_eq!(got, expected);

    // no actors on cordoned nodes
    let info = cluster
        .get_cluster_info()
        .await
        .expect("failed to get info");
    let (fragments, workers) = get_schedule(info).await;

    // TODO: do this fancy with map
    let mut cordoned_pus: Vec<Vec<ParallelUnit>> = vec![];
    for worker in workers {
        if cordoned_nodes_ids.contains(&worker.id) {
            cordoned_pus.push(worker.parallel_units);
        }
    }
    assert!(cordoned_nodes.len() == cordoned_pus.len());
    let cordoned_pus_flat = cordoned_pus.iter().flatten().map(|pu| pu.id).collect_vec();
    for frag in fragments {
        for actor in frag.actor_list {
            let pu_id = actor.parallel_units_id;
            assert!(!cordoned_pus_flat.contains(&pu_id));
        }
    }
}

// TODO: remove the meta.get_schedule rpc call and what belongs to it
// TODO: Do not use GetScheduleResponse here
async fn get_schedule(cluster_info: GetClusterInfoResponse) -> (Vec<Fragment>, Vec<WorkerNode>) {
    // Compile fragments
    let mut fragment_list: Vec<Fragment> = vec![];
    for table_fragment in cluster_info.get_table_fragments() {
        for (_, fragment) in table_fragment.get_fragments() {
            let mut actor_list: Vec<Actor> = vec![];
            for actor in fragment.get_actors() {
                let id = actor.actor_id;
                let pu_id = table_fragment
                    .get_actor_status()
                    .get(&id)
                    .expect("expected actor status") // TODO: handle gracefully
                    .get_parallel_unit()
                    .expect("Failed to retrieve parallel units")
                    .get_id();
                actor_list.push(Actor {
                    actor_id: actor.actor_id,
                    parallel_units_id: pu_id,
                });
            }
            fragment_list.push(Fragment {
                id: fragment.get_fragment_id(),
                actor_list,
                type_flag: fragment.fragment_type_mask,
            });
        }
    }

    (fragment_list, cluster_info.worker_nodes)
}

macro_rules! test {
    ($query:ident) => {
        paste::paste! {
         //   #[madsim::test]
         //   async fn [< nexmark_scaling_up_down_1_ $query >]() {
         //       use risingwave_simulation::nexmark::queries::$query::*;
         //       test_cordon(CREATE, SELECT, DROP, 1)
         //       .await
         //    }
            #[madsim::test]
            async fn [< nexmark_scaling_up_down_2_ $query >]() {
                use risingwave_simulation::nexmark::queries::$query::*;
                test_cordon(CREATE, SELECT, DROP, 2)
                .await
            }
        }
    };
}

// TODO: uncommet all of these tests again
// q0, q1, q2: too trivial
test!(q3);
// test!(q4);
// test!(q5);
// // q6: cannot plan
// test!(q7);
// test!(q8);
// test!(q9);
// // q10+: duplicated or unsupported
//
// // Self made queries.
// test!(q101);
// test!(q102);
// test!(q103);
// test!(q104);
// test!(q105);

// new requirement:
// Tool for open source users to scale clusters
// add to risectl cmd line tool
// cordan node -> K8s naming. No new scheduling done on this node
// Do we have an issue for it?
// also provide uncordon command: Mark a DELETING node as non-deleting
// Discuss

// SQL interface for actors and nodes and so on

// Maybe split up my PR in small PRs?
// E.g. one PR with cordon only

// debug approaches
// maybe provide commands above and run against real cluster
// madsim only uses one thread. May get stuck
// runji madsim turn on debugging traces

// Think: Workflow maybe Arne?
// Maybe he has no time

// Mit Peng Chen sprechen

// wg-scaling-compute-node update geben

// https://github.com/risingwavelabs/risingwave-operator/pull/448 review
