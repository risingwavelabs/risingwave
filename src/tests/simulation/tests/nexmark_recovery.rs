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

/// Setup a nexmark stream, inject failures, and verify results.
#[madsim::test]
async fn nexmark_recovery() -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 2, Some(THROUGHPUT * 20)).await?;

    // note: feel free to disable queries to speed up the test
    cluster.run(nexmark::queries::q3::CREATE).await.unwrap();
    cluster.run(nexmark::queries::q4::CREATE).await.unwrap();
    cluster.run(nexmark::queries::q5::CREATE).await.unwrap();
    cluster.run(nexmark::queries::q7::CREATE).await.unwrap();
    cluster.run(nexmark::queries::q8::CREATE).await.unwrap();
    cluster.run(nexmark::queries::q9::CREATE).await.unwrap();

    // kill nodes and trigger recovery
    for _ in 0..5 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL).await;
    }

    // make sure running for enough time
    sleep(Duration::from_secs(30)).await;

    let q3 = cluster.run(nexmark::queries::q3::SELECT).await.unwrap();
    let q4 = cluster.run(nexmark::queries::q4::SELECT).await.unwrap();
    let q5 = cluster.run(nexmark::queries::q5::SELECT).await.unwrap();
    let q7 = cluster.run(nexmark::queries::q7::SELECT).await.unwrap();
    let q8 = cluster.run(nexmark::queries::q8::SELECT).await.unwrap();
    let q9 = cluster.run(nexmark::queries::q9::SELECT).await.unwrap();

    // uncomment the following lines to generate results
    // std::fs::write("tests/nexmark_result/q3.txt", q3).unwrap();
    // std::fs::write("tests/nexmark_result/q4.txt", q4).unwrap();
    // std::fs::write("tests/nexmark_result/q5.txt", q5).unwrap();
    // std::fs::write("tests/nexmark_result/q7.txt", q7).unwrap();
    // std::fs::write("tests/nexmark_result/q8.txt", q8).unwrap();
    // std::fs::write("tests/nexmark_result/q9.txt", q9).unwrap();

    assert_eq!(q3, include_str!("nexmark_result/q3.txt"));
    assert_eq!(q4, include_str!("nexmark_result/q4.txt"));
    assert_eq!(q5, include_str!("nexmark_result/q5.txt"));
    assert_eq!(q7, include_str!("nexmark_result/q7.txt"));
    assert_eq!(q8, include_str!("nexmark_result/q8.txt"));
    assert_eq!(q9, include_str!("nexmark_result/q9.txt"));
    Ok(())
}
