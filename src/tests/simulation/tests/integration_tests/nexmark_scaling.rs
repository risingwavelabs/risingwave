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

use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::time::sleep;
use risingwave_pb::common::{Actor, Fragment, ParallelUnit, WorkerNode};
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_simulation::cluster::Configuration;
use risingwave_simulation::nexmark::queries::{q3, q4};
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};

// TODO: add test where that checks that if we have zero cordoned nodes that
// actors_on_cordoned_nodes returns empty vec

// async fn actors_on_cordoned_nodes(info: GetClusterInfoResponse, cordoned_nodes: &Vec<WorkerNode>)
// { No actors from other query on cordoned nodes
//     let info2 = cluster.get_cluster_info().await?;
// let (fragments2, workers2) = get_schedule(info).await;
// let cordoned_nodes_ids = cordoned_nodes.iter().map(|n| n.id).collect_vec();
//
// TODO: do this fancy with map
// let mut cordoned_pus2: Vec<Vec<ParallelUnit>> = vec![];
// for worker in workers2 {
// if cordoned_nodes_ids.contains(&worker.id) {
// cordoned_pus2.push(worker.parallel_units);
// }
// }
// assert!(cordoned_nodes.len() == cordoned_pus2.len());
// TODO: do this with map
// let mut actors_on_cordoned2: Vec<u32> = vec![];
// for frag in fragments2 {
// for actor in frag.actor_list {
// let pu_id = actor.parallel_units_id;
// if cordoned_pus.contains(pu_id) {
// actors_on_cordoned.push(pu_id);
// }
// }
// }
// }

/// create cluster, run query, cordon node, run other query. Cordoned node should NOT contain actors
/// from other query
async fn cordoned_nodes_do_not_get_new_actors(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // setup cluster and calc expected result
    let sleep_sec = 30;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    // do not drop the query

    // cordon random nodes
    let mut cordoned_nodes: Vec<WorkerNode> = vec![];
    // TODO: I can directly write this in the cordoned_nodes vec
    let rand_nodes: Vec<WorkerNode> = cluster
        .cordon_random_workers(number_of_nodes) // TODO:  sometimes collect same node twice. This is not what we want!
        .await?;
    for rand_node in rand_nodes.clone() {
        cordoned_nodes.push(rand_node);
    }

    let mut log_msg = "Cordoned the following nodes:\n".to_string();
    for rand_node in rand_nodes {
        log_msg = format!("{}{:?}\n", log_msg, rand_node);
    }
    println!("{}", log_msg);
    // tracing::info!(log_msg); // TODO: use trace

    let cordoned_nodes_ids = cordoned_nodes.iter().map(|n| n.id).collect_vec();

    // check which actors are on cordoned nodes
    let info = cluster.get_cluster_info().await?;
    let (fragments, workers) = get_schedule(info).await;
    // TODO: do this fancy with map
    let mut cordoned_pus: Vec<Vec<ParallelUnit>> = vec![];
    for worker in workers {
        if cordoned_nodes_ids.contains(&worker.id) {
            cordoned_pus.push(worker.parallel_units);
        }
    }
    assert!(cordoned_nodes.len() == cordoned_pus.len());
    let cordoned_pus_ids = cordoned_pus.iter().flatten().map(|pu| pu.id).collect_vec();
    let mut actors_on_cordoned: Vec<u32> = vec![];
    // TODO: do this with map
    for frag in fragments {
        for actor in frag.actor_list {
            let pu_id = actor.parallel_units_id;
            if cordoned_pus_ids.contains(&pu_id) {
                actors_on_cordoned.push(pu_id);
            }
        }
    }

    println!("here is where the fun begins... Creating additional query"); // TODO: del line

    let dummy_create = "create table t (dummy date, v varchar);";
    let dummy_mv = "create materialized view mv as select v from t;";
    let dummy_in = "insert into t values ('2023-01-01', '1z');";
    let dummy_select = "select * from mv;";
    let dummy_drop = "drop materialized view mv; drop table t;";
    for sql in vec![dummy_create, dummy_mv, dummy_in] {
        cluster.run(sql).await?;
        sleep(Duration::from_secs(sleep_sec)).await;
    }
    assert_eq!(cluster.run(dummy_select).await?, "1z");

    // No actors from other query on cordoned nodes
    let info2 = cluster.get_cluster_info().await?;
    let (fragments2, workers2) = get_schedule(info2).await;
    // TODO: do this fancy with map
    let mut cordoned_pus2: Vec<Vec<ParallelUnit>> = vec![];
    for worker in workers2 {
        if cordoned_nodes_ids.contains(&worker.id) {
            cordoned_pus2.push(worker.parallel_units);
        }
    }
    assert!(cordoned_nodes.len() == cordoned_pus2.len());
    let cordoned_pus_ids = cordoned_pus.iter().flatten().map(|pu| pu.id).collect_vec();
    // TODO: do this with map
    let mut actors_on_cordoned2: Vec<u32> = vec![];
    for frag in fragments2 {
        for actor in frag.actor_list {
            let pu_id = actor.parallel_units_id;
            if cordoned_pus_ids.contains(&pu_id) {
                actors_on_cordoned.push(pu_id); // actor.actor_id
            }
        }
    }

    // TODO: Clarify
    // We allow that an actor moves from a cordoned node to a non-cordoned node
    // We disallow that an actor moves from a non-cordoned node to a cordoned node
    // We disallow that an actor moves from a cordoned node to another cordoned node
    assert!(actors_on_cordoned2.len() <= actors_on_cordoned.len());
    for actors2 in actors_on_cordoned2 {
        assert!(actors_on_cordoned.contains(&actors2));
    }

    // compare results of original query
    cluster.run(dummy_drop).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let got = cluster.run(select).await?;
    assert_eq!(got, expected);

    Ok(())
}

/// create cluster, cordon node, run query. Cordoned node should NOT contain actors
async fn cordoned_nodes_do_not_get_actors(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // setup cluster and calc expected result
    let sleep_sec = 30;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(sleep_sec)).await;

    // cordon random nodes
    let mut cordoned_nodes: Vec<WorkerNode> = vec![];
    // TODO: I can directly write this in the cordoned_nodes ved
    let rand_nodes: Vec<WorkerNode> = cluster
        .cordon_random_workers(number_of_nodes) // TODO:  sometimes collect same node twice. This is not what we want!
        .await?;
    for rand_node in rand_nodes.clone() {
        cordoned_nodes.push(rand_node);
    }

    let mut log_msg = "Cordoned the following nodes:\n".to_string();
    for rand_node in rand_nodes {
        log_msg = format!("{}{:?}\n", log_msg, rand_node);
    }
    println!("{}", log_msg);
    // tracing::info!(log_msg); // TODO: use trace

    let cordoned_nodes_ids = cordoned_nodes.iter().map(|n| n.id).collect_vec();

    // compare results
    sleep(Duration::from_secs(sleep_sec)).await;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let got = cluster.run(select).await?;
    assert_eq!(got, expected);

    // no actors on cordoned nodes
    let info = cluster.get_cluster_info().await?;
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
    Ok(())
}

#[madsim::test]
#[should_panic]
async fn cordon_all_nodes_error() {
    use risingwave_simulation::nexmark::queries::q3::*;
    cordoned_nodes_do_not_get_actors(CREATE, SELECT, DROP, 3)
        .await
        .unwrap()
}

#[madsim::test]
#[should_panic]
async fn cordon_all_nodes_error_2() {
    use risingwave_simulation::nexmark::queries::q3::*;
    cordoned_nodes_do_not_get_new_actors(CREATE, SELECT, DROP, 3)
        .await
        .unwrap()
}

// TODO: remove the meta.get_schedule rpc call and what belongs to it
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
            #[madsim::test]
            async fn [< cordoned_nodes_do_not_get_actors_1_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                cordoned_nodes_do_not_get_actors(CREATE, SELECT, DROP, 1).await
            }
            #[madsim::test]
            async fn [< cordoned_nodes_do_not_get_actors_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                cordoned_nodes_do_not_get_actors(CREATE, SELECT, DROP, 2).await
            }

            #[madsim::test]
            async fn [< cordoned_nodes_do_not_get_new_actors_1_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                cordoned_nodes_do_not_get_new_actors(CREATE, SELECT, DROP, 1).await
            }
            #[madsim::test]
            async fn [< cordoned_nodes_do_not_get_new_actors_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                cordoned_nodes_do_not_get_new_actors(CREATE, SELECT, DROP, 2).await
            }
        }
    };
}

// TODO: uncommet all of these tests again
// q0, q1, q2: too trivial
test!(q3);
test!(q4);
test!(q5);
// // q6: cannot plan
test!(q7);
test!(q8);
test!(q9);
// // q10+: duplicated or unsupported
//
// // Self made queries.
test!(q101);
test!(q102);
test!(q103);
test!(q104);
test!(q105);

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

// TODO: Another test:
// corden nodes, manually schedule actors on cordnened node. Should throw error

// TODO: test
// cordon needs to be idempotent

// cloud:
// 1. cordon
// 2. reschedules
// 2.2. check if no actors on node?
// 3. deleted CN

// Idea: Maybe this is because marking the node as cordoned has some side effect -> Yes!
