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
use risingwave_pb::common::WorkerNode;
use risingwave_simulation::cluster::{Configuration, KillOpts};
use risingwave_simulation::ctl_ext::predicate;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;
use tracing_subscriber::fmt::format;

/// gets the output without failures as the standard result
// TODO: may be nice to have
// async fn get_expected(mut cluster: NexmarkCluster, create: &str, select: &str, drop: &str)  ->
// Result<String> {}

/// Setup a nexmark stream, inject failures, and verify results.
async fn nexmark_scaling_up_common(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(30)).await;
    let expected = cluster.run(select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(5)).await;

    cluster.run(create).await?;

    cluster.add_compute_node(number_of_nodes);
    sleep(Duration::from_secs(20)).await;

    cluster.run(select).await?.assert_result_eq(&expected);

    Ok(())
}

/// Setup a nexmark stream, inject failures, and verify results.
async fn nexmark_scaling_down_common(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(30)).await;
    let expected = cluster.run(select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(5)).await;

    cluster.run(create).await?;

    let mut unregistered_nodes: Vec<WorkerNode> = vec![];
    let rand_nodes: Vec<WorkerNode> = cluster
        .unregister_compute_nodes(number_of_nodes) // TODO:  sometimes collect same node twice. This is not what we want!
        .await
        .expect("unregistering node failed");
    for rand_node in rand_nodes {
        println!("Unregister compute node {:?}", rand_node); // TODO: remove line
        println!(
            "Now compute nodes in cluster {}",
            cluster.get_number_worker_nodes().await
        ); // TODO: remove line

        unregistered_nodes.push(rand_node);
        sleep(Duration::from_secs(30)).await;
    }
    let unregistered_node_addrs = unregistered_nodes
        .iter()
        .map(|node| cluster.cn_host_addr_to_task(node.clone().host.unwrap()))
        .collect_vec();

    cluster.kill_nodes(&unregistered_node_addrs).await;

    cluster.run(select).await?.assert_result_eq(&expected);

    Ok(())
}

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

/// Setup a nexmark stream, inject failures, and verify results.
async fn nexmark_scaling_up_down_common(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
    //  tokio::task::spawn(async move {
    //      tokio::time::sleep(Duration::from_secs(120)).await;
    //      println!("max runtime is over");
    //      assert!(false);
    //  });

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    println!("Beginning of test"); // TODO: remove line

    let sleep_sec = 20;

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;
    println!(
        "created OG cluster with {} worker nodes",
        cluster.get_number_worker_nodes().await
    ); // TODO: remove line

    cluster.run(create).await?;
    sleep(Duration::from_secs(sleep_sec)).await;
    let expected = cluster.run(select).await?;
    println!("got expected result"); // TODO: remove line

    println!("Cleaning cluster"); // TODO: remove line
    cluster.run(drop).await?;
    sleep(Duration::from_secs(sleep_sec)).await;

    cluster.run(create).await?;

    // TODO: remove comment
    // not adding compute nodes does not help
    // However, when clearing nodes, if we add nodes we may also clear a node without fragments
    // println!("adding compute node"); // TODO: remove line
    // cluster.add_compute_node(number_of_nodes);
    // sleep(Duration::from_secs(sleep_sec)).await;
    // println!(
    //     "Now {} compute nodes in cluster",
    //     cluster.get_number_worker_nodes().await
    // ); // TODO: remove line

    // TODO: reschedule here as well?

    let mut unregistered_nodes: Vec<WorkerNode> = vec![];
    let rand_nodes: Vec<WorkerNode> = cluster
        .unregister_compute_nodes(number_of_nodes) // TODO:  sometimes collect same node twice. This is not what we want!
        .await
        .expect("unregistering node failed");
    for rand_node in rand_nodes {
        println!("Unregister compute node {:?}", rand_node); // TODO: remove line
        println!(
            "Now compute nodes in cluster {}",
            cluster.get_number_worker_nodes().await
        ); // TODO: remove line

        unregistered_nodes.push(rand_node);
        sleep(Duration::from_secs(sleep_sec)).await;
    }
    let unregistered_node_task_names = unregistered_nodes
        .clone()
        .iter()
        .map(|node| cluster.cn_host_addr_to_task(node.clone().host.unwrap()))
        .collect_vec();
    // make sure that we do not unregister the same node multiple times
    assert!(has_unique_elements(unregistered_node_task_names.iter()));
    println!("all unregistered nodes are uniq"); // TODO: remove line

    // question: If I mark CN as DELETING does meta remove fragments from DELETING CN?
    // answer: NO. Marking as deleted should not trigger rescheduling. We call rescheduling in the
    // end of the workflow

    // TODO: should I make the interface use HostAddress? Then I do not need to convert here
    let addrs = unregistered_nodes
        .iter()
        .map(|n| {
            let a = n.get_host().expect("expected worker node to have host");
            HostAddr {
                host: a.host.clone(),
                port: a.port as u16,
            }
        })
        .collect_vec();
    println!("clearing unregisterd nodes"); // TODO: remove line

    // TODO:remove
    // clearing one worker after another does not help
    //  for addr in addrs {
    //      let mut a: Vec<HostAddr> = vec![];
    //      a.push(addr);
    //      cluster
    //          .clear_worker_nodes(a)
    //          .await
    //          .expect("failed to clear worker nodes");
    //  }

    cluster
        .clear_worker_nodes(addrs.clone())
        .await
        .expect("failed to clear worker nodes");

    // TODO: this should work without sleep
    sleep(Duration::from_secs(sleep_sec)).await;

    // // TODO: remove below
    // cluster
    //     .clear_worker_nodes(addrs.clone())
    //     .await
    //     .expect("failed to clear worker nodes");

    println!("unregistered nodes cleared"); // TODO: remove line

    // Notes: use RescheduleRequest. scale_service.rs reschedule
    // I have to generate the new schedule plan myself. Hardcode it here

    println!("Killing nodes"); // TODO: remove line
    cluster.kill_nodes(&unregistered_node_task_names).await;
    println!(
        // TODO: remove line
        "Nodes killed. Now compute nodes in cluster {}",
        cluster.get_number_worker_nodes().await
    );
    // TODO: assert that cluster really has correct number of nodes

    println!("run select"); // TODO: remove line
    cluster.run(select).await?.assert_result_eq(&expected);

    Ok(())
}

// TODO: We need to call ClearWorkers instead of manually trying to reschedule

macro_rules! test {
    ($query:ident) => {
        paste::paste! {
            // TODO: Uncomment these again
        //   #[madsim::test]
        //   async fn [< nexmark_scaling_up_ $query >]() -> Result<()> {
        //       use risingwave_simulation::nexmark::queries::$query::*;
        //       nexmark_scaling_up_common(CREATE, SELECT, DROP, 1)
        //       .await
        //   }
        //   #[madsim::test]
        //   async fn [< nexmark_scaling_up_2_ $query >]() -> Result<()> {
        //       use risingwave_simulation::nexmark::queries::$query::*;
        //       nexmark_scaling_up_common(CREATE, SELECT, DROP, 2)
        //       .await
        //   }
        //   #[madsim::test]
        //   async fn [< nexmark_scaling_down_ $query >]() -> Result<()> {
        //       use risingwave_simulation::nexmark::queries::$query::*;
        //       nexmark_scaling_down_common(CREATE, SELECT, DROP, 1)
        //       .await
        //   }
        //   #[madsim::test]
        //   async fn [< nexmark_scaling_down_2_ $query >]() -> Result<()> {
        //       use risingwave_simulation::nexmark::queries::$query::*;
        //       nexmark_scaling_down_common(CREATE, SELECT, DROP, 2)
        //       .await
        //   }
           #[madsim::test]
           async fn [< nexmark_scaling_up_down_2_ $query >]() -> Result<()> {
               use risingwave_simulation::nexmark::queries::$query::*;
               nexmark_scaling_up_down_common(CREATE, SELECT, DROP, 2)
               .await
               }
         }
    };
}

// TODO: Why do I sometimes get stuck? Do I maybe scale down the wrong nodes? Am I not allowed to
// clear some nodes?

// TODO: scale down only?

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
