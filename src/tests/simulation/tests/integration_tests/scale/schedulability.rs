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
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::time::sleep;
use rand::seq::SliceRandom;
use risingwave_pb::common::{ParallelUnit, WorkerNode};
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::stream_plan::FragmentTypeFlag;
use risingwave_simulation::cluster::Configuration;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};

struct ActorOnPu {
    actor_id: u32,
    parallel_units_id: u32,
}

struct FragmentAndActors {
    id: u32,
    actor_list: Vec<ActorOnPu>,
    type_flag: u32,
}

struct FragmentsAndWorkers {
    fragments: Vec<FragmentAndActors>,
    workers: Vec<WorkerNode>,
}

// Get the ids of all parallel unit which are located on unschedulable workers
fn pu_ids_on_unschedulable_nodes(all_workers: &Vec<WorkerNode>) -> HashSet<u32> {
    let unschedulable_nodes_ids = all_workers
        .iter()
        .filter(|w| {
            !w.get_property()
                .expect("expected worker to have property")
                .is_schedulable
        })
        .map(|n| n.id)
        .collect_vec();
    let mut unschedulable_pu_ids: Vec<Vec<ParallelUnit>> = vec![];
    for worker in all_workers {
        if unschedulable_nodes_ids.contains(&worker.id) {
            unschedulable_pu_ids.push(worker.parallel_units.clone());
        }
    }
    unschedulable_pu_ids
        .iter()
        .flatten()
        .map(|pu| pu.id)
        .collect()
}

// Get the ids of all actors which are located on unschedulable workers
fn actor_ids_on_unschedulable_nodes(info: GetClusterInfoResponse) -> HashSet<u32> {
    let (fragments, workers) = get_schedule(info.clone());
    let unschedulable_pus_ids = pu_ids_on_unschedulable_nodes(&workers);
    let mut actor_ids_on_unschedulable = HashSet::<u32>::new();
    for frag in fragments {
        for actor in frag.actor_list {
            let pu_id = actor.parallel_units_id;
            if unschedulable_pus_ids.contains(&pu_id) {
                actor_ids_on_unschedulable.insert(actor.actor_id);
            }
        }
    }
    actor_ids_on_unschedulable
}

/// create cluster, run query, mark nodes as unschedulable, run other query.
/// Unschedulable node should NOT contain actors from other query
async fn unschedulable_nodes_do_not_get_new_actors(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // setup cluster and calc expected result
    let sleep_sec = 10;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    // do not drop the query

    // mark random nodes as unschedulable
    let unschedulable_nodes = cluster
        .mark_rand_nodes_unschedulable(number_of_nodes)
        .await?;

    let mut log_msg = "Marked the following nodes as unschedulable:\n".to_string();
    for rand_node in &unschedulable_nodes {
        log_msg = format!("{}{:?}\n", log_msg, rand_node);
    }
    tracing::info!(log_msg);

    // check which actors are on unschedulable nodes
    let info = cluster.get_cluster_info().await?;
    let actors_on_unschedulable1 = actor_ids_on_unschedulable_nodes(info);

    let dummy_create = "create table t (dummy date, v varchar);";
    let dummy_mv = "create materialized view mv as select v from t;";
    let dummy_insert = "insert into t values ('2023-01-01', '1z');";
    let dummy_select = "select * from mv;";
    let dummy_drop = "drop materialized view mv; drop table t;";
    for sql in vec![dummy_create, dummy_mv, dummy_insert] {
        cluster.run(sql).await?;
        sleep(Duration::from_secs(sleep_sec)).await;
    }
    assert_eq!(cluster.run(dummy_select).await?, "1z");

    // No actors from other query on unschedulable nodes
    let info2 = cluster.get_cluster_info().await?;
    let actors_on_unschedulable2 = actor_ids_on_unschedulable_nodes(info2);

    // We allow that an actor moves from a unschedulable node to a non-unschedulable node
    // We allow that an actor moves from a unschedulable node to another unschedulable node
    // We disallow that an actor moves from a non-unschedulable node to a unschedulable node
    assert!(actors_on_unschedulable1.is_superset(&actors_on_unschedulable2));

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

/// create cluster, mark nodes as unschedulable, run query. unschedulable node should NOT contain
/// actors
async fn unschedulable_nodes_do_not_get_actors(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // setup cluster and calc expected result
    let sleep_sec = 10;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(sleep_sec)).await;

    // mark rand nodes as unschedulable
    let unschedulable_nodes = cluster
        .mark_rand_nodes_unschedulable(number_of_nodes)
        .await?;
    let mut log_msg = "Mark the following nodes as unschedulable:\n".to_string();
    for rand_node in &unschedulable_nodes {
        log_msg = format!("{}{:?}\n", log_msg, rand_node);
    }
    tracing::info!(log_msg);

    let unschedulable_nodes_ids = unschedulable_nodes.iter().map(|n| n.id).collect_vec();

    // compare results
    sleep(Duration::from_secs(sleep_sec)).await;
    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let got = cluster.run(select).await?;
    assert_eq!(got, expected);

    // no actors on unschedulable nodes
    let info = cluster.get_cluster_info().await?;
    let (fragments, workers) = get_schedule(info);

    let mut unschedulable_pu_ids = HashSet::<u32>::new();
    for worker in workers {
        if unschedulable_nodes_ids.contains(&worker.id) {
            for pu in worker.parallel_units {
                unschedulable_pu_ids.insert(pu.id);
            }
        }
    }
    for frag in fragments {
        for actor in frag.actor_list {
            let pu_id = actor.parallel_units_id;
            assert!(!unschedulable_pu_ids.contains(&pu_id));
        }
    }
    Ok(())
}

async fn mark_as_unschedulable_is_idempotent(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let sleep_sec = 15;
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    let rand_workers = cluster.get_random_worker_nodes(number_of_nodes).await?;
    let old_ids: HashSet<u32> = rand_workers.iter().map(|w| w.id).collect();

    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    cluster.run(drop).await?;

    // mark the same nodes as unschedulable multiple times
    for _ in 0..3 {
        for worker in &rand_workers {
            cluster
                .mark_as_unschedulable(&worker)
                .await
                .expect("expect mark_as_unschedulable to work");
        }

        // expect the same result, independent of how many times we marked as unschedulable
        cluster.run(create).await?;
        sleep(Duration::from_secs(sleep_sec)).await;
        let got = cluster.run(select).await?;
        assert_eq!(expected, got);
        cluster.run(drop).await?;

        // we only ever expect the same workers to be unschedulable
        let (_, workers) = get_schedule(cluster.get_cluster_info().await?);
        let new_ids: HashSet<u32> = workers
            .iter()
            .filter(|w| {
                !w.get_property()
                    .expect("expected node to have property")
                    .is_schedulable
            })
            .map(|w| w.id)
            .collect();
        assert_eq!(new_ids, old_ids);
    }

    Ok(())
}

/// a reschedule request moving an actor to an unschedulable node should fail
async fn invalid_reschedule(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let sleep_sec = 10;

    // setup cluster and calc expected result
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;

    let unschedulable_nodes = cluster
        .mark_rand_nodes_unschedulable(number_of_nodes)
        .await?;
    assert!(!unschedulable_nodes.is_empty());

    // create some actors which we can move
    cluster.run(create).await?;
    cluster.run(select).await?;
    sleep(Duration::from_secs(sleep_sec)).await;

    let (fragments, _) = get_schedule(cluster.get_cluster_info().await?);
    let unschedulable_pus: Vec<&ParallelUnit> = unschedulable_nodes
        .iter()
        .map(|w| w.get_parallel_units())
        .flatten()
        .collect_vec();
    assert!(!unschedulable_pus.is_empty());

    // for a mv fragment, try to move one actor from a unschedulable PU to a schedulable PU
    // using an mv fragment, because they can be moved
    let mv_frags = fragments
        .iter()
        .filter(|f| f.type_flag == FragmentTypeFlag::Mview as u32)
        .collect_vec();
    assert!(!mv_frags.is_empty());
    let mv_frag = mv_frags.first().unwrap();

    let from = mv_frag
        .actor_list
        .choose(&mut rand::thread_rng())
        .expect("expect fragment to have at least 1 actor")
        .parallel_units_id;
    let to = unschedulable_pus
        .choose(&mut rand::thread_rng())
        .expect("expected at least one unschedulable PU")
        .id;
    let f_id = mv_frag.id;
    let result = cluster.reschedule(format!("{f_id}-[{from}]+[{to}]")).await;
    assert!(result.is_err());
    let err_msg = result.err().unwrap().to_string();
    assert_eq!(
        err_msg,
        "gRPC error (Internal error): unable to move actor to node marked as unschedulable"
    );

    cluster.run(drop).await?;
    Ok(())
}

fn get_schedule(cluster_info: GetClusterInfoResponse) -> (Vec<FragmentAndActors>, Vec<WorkerNode>) {
    // Compile fragments
    let mut fragment_list: Vec<FragmentAndActors> = vec![];
    for table_fragment in cluster_info.get_table_fragments() {
        for (_, fragment) in table_fragment.get_fragments() {
            let mut actor_list: Vec<ActorOnPu> = vec![];
            for actor in fragment.get_actors() {
                let id = actor.actor_id;
                let pu_id = table_fragment
                    .get_actor_status()
                    .get(&id)
                    .expect("expected actor status")
                    .get_parallel_unit()
                    .expect("Failed to retrieve parallel units")
                    .get_id();
                actor_list.push(ActorOnPu {
                    actor_id: actor.actor_id,
                    parallel_units_id: pu_id,
                });
            }
            fragment_list.push(FragmentAndActors {
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
            // unschedulable workers on idle cluster
            #[madsim::test]
            async fn [< unschedulable_nodes_do_not_get_actors_1_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                unschedulable_nodes_do_not_get_actors(CREATE, SELECT, DROP, 1).await
            }
            #[madsim::test]
            async fn [< unschedulable_nodes_do_not_get_actors_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                unschedulable_nodes_do_not_get_actors(CREATE, SELECT, DROP, 2).await
            }

            // unschedulable workers on busy cluster
            #[madsim::test]
            async fn [< unschedulable_nodes_do_not_get_new_actors_1_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                unschedulable_nodes_do_not_get_new_actors(CREATE, SELECT, DROP, 1).await
            }
            #[madsim::test]
            async fn [< unschedulable_nodes_do_not_get_new_actors_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                unschedulable_nodes_do_not_get_new_actors(CREATE, SELECT, DROP, 2).await
            }

            // Idempotent
            #[madsim::test]
            async fn [< unschedulable_is_idempotent_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                mark_as_unschedulable_is_idempotent(CREATE, SELECT, DROP, 1).await
            }
            #[madsim::test]
            async fn [< unschedulable_is_idempotent_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                mark_as_unschedulable_is_idempotent(CREATE, SELECT, DROP, 2).await
            }

            // mark all nodes as unschedulable
            #[madsim::test]
            async fn [< unschedulable_all_nodes_error_ $query >]() {
                use risingwave_simulation::nexmark::queries::$query::*;
                let result = unschedulable_nodes_do_not_get_actors(CREATE, SELECT, DROP, 3)
                    .await;
                assert!(result.is_err());
                assert_eq!(result.err().unwrap().to_string(), "db error: ERROR: QueryError: internal error: Service unavailable: No available parallel units to schedule");
            }
            #[madsim::test]
            async fn [< unschedulable_all_nodes_error_2_ $query >]() {
                use risingwave_simulation::nexmark::queries::q3::*;
                let result = unschedulable_nodes_do_not_get_actors(CREATE, SELECT, DROP, 3)
                    .await;
                assert!(result.is_err());
                assert_eq!(result.err().unwrap().to_string(), "db error: ERROR: QueryError: internal error: Service unavailable: No available parallel units to schedule");
            }

            // invalid scheduling request
            #[madsim::test]
            async fn [< invalid_reschedule_1_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                invalid_reschedule(CREATE, SELECT, DROP, 1)
                    .await
            }
            #[madsim::test]
            async fn [< invalid_reschedule_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::q3::*;
                invalid_reschedule(CREATE, SELECT, DROP, 2)
                    .await
            }
        }
    };
}

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
