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
use risingwave_simulation::nexmark::{self, NexmarkCluster, THROUGHPUT};
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
    sleep(Duration::from_secs(60)).await;

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

    for _ in 0..number_of_nodes {
        cluster.remove_rand_compute_node().await;
        sleep(Duration::from_secs(60)).await;
    }

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
    sleep(Duration::from_secs(60)).await;

    for _ in 0..number_of_nodes {
        cluster.remove_rand_compute_node().await;
        sleep(Duration::from_secs(60)).await;
    }

    cluster.run(select).await?.assert_result_eq(&expected);

    Ok(())
}

// TODO: rename this to nexmark_scaling
macro_rules! test {
    ($query:ident) => {
        paste::paste! {
            #[madsim::test]
            async fn [< nexmark_scaling_up_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_scaling_up_common(CREATE, SELECT, DROP, 1)
                .await
            }
            #[madsim::test]
            async fn [< nexmark_scaling_up_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_scaling_up_common(CREATE, SELECT, DROP, 2)
                .await
            }
            #[madsim::test]
            async fn [< nexmark_scaling_down_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_scaling_down_common(CREATE, SELECT, DROP, 1)
                .await
            }
            #[madsim::test]
            async fn [< nexmark_scaling_down_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_scaling_down_common(CREATE, SELECT, DROP, 2)
                .await
            }
            #[madsim::test]
            async fn [< nexmark_scaling_up_down_2_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_scaling_up_down_common(CREATE, SELECT, DROP, 2)
                .await
            }
        }
    };
}

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
// 