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
use madsim::time::{sleep, Instant};
use risingwave_simulation::cluster::{Configuration, KillOpts};
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;

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

    let mut unregistered_nodes: Vec<String> = vec![];
    for _ in 0..number_of_nodes {
        let node_host_addr = cluster
            .unregister_compute_node()
            .await
            .expect("unregistering node failed");
        unregistered_nodes.push(cluster.cn_host_addr_to_task(&node_host_addr));
        sleep(Duration::from_secs(20)).await;
    }
    cluster.kill_nodes(&unregistered_nodes).await;

    cluster.run(select).await?.assert_result_eq(&expected);

    Ok(())
}

/// Setup a nexmark stream, inject failures, and verify results.
async fn nexmark_scaling_up_down_common(
    create: &str,
    select: &str,
    drop: &str,
    number_of_nodes: usize,
) -> Result<()> {
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

    println!("adding compute node"); // TODO: remove line
    cluster.add_compute_node(number_of_nodes);
    sleep(Duration::from_secs(sleep_sec)).await;
    println!(
        "Now {} compute nodes in cluster",
        cluster.get_number_worker_nodes().await
    ); // TODO: remove line

    let mut unregistered_nodes: Vec<String> = vec![];
    for _ in 0..number_of_nodes {
        let node_host_addr = cluster
            .unregister_compute_node()
            .await
            .expect("unregistering node failed");
        println!("Unregister compute node {:?}", node_host_addr); // TODO: remove line
        println!(
            "Now compute nodes in cluster {}",
            cluster.get_number_worker_nodes().await
        ); // TODO: remove line

        unregistered_nodes.push(cluster.cn_host_addr_to_task(&node_host_addr));
        sleep(Duration::from_secs(sleep_sec)).await;
    }

    // question: If I mark CN as DELETING does meta remove fragments from DELETING CN?
    // answer: NO. Marking as deleted should not trigger rescheduling. We call rescheduling in the
    // end of the workflow

    // TODO: call reschedule here
    println!("Killing nodes"); // TODO: remove line
    cluster.kill_nodes(&unregistered_nodes).await;
    println!(
        // TODO: remove line
        "Nodes killed. Now compute nodes in cluster {}",
        cluster.get_number_worker_nodes().await
    );

    println!("run select"); // TODO: remove line
    let select_result = cluster.run(select).await?;
    println!("Got new result"); // TODO: remove line

    select_result.assert_result_eq(&expected);

    Ok(())
}

// TODO: remove hardcoded version
#[madsim::test]
async fn nexmark_scaling_up_down_2_q3_hardcoded() -> Result<()> {
    use risingwave_simulation::nexmark::queries::q3::*;
    nexmark_scaling_up_down_common(CREATE, SELECT, DROP, 2).await
}

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
