// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(madsim)]

use std::time::Duration;

use anyhow::Result;
use futures::future::BoxFuture;
use madsim::time::sleep;
use risingwave_simulation_scale::cluster::Configuration;
use risingwave_simulation_scale::ctl_ext::predicate::{
    identity_contains, upstream_fragment_count, BoxedPredicate,
};
use risingwave_simulation_scale::nexmark::queries::q4::*;
use risingwave_simulation_scale::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation_scale::utils::AssertResult;

const RESULT: &str = r#"
10 28658724.666666666666666666667
11 29973088.407882676443629697525
12 31781439.824623560673162090345
13 27628627.677165354330708661417
14 29324455.522193211488250652742
"#;

async fn init() -> Result<NexmarkCluster> {
    let mut cluster =
        NexmarkCluster::new(Configuration::default(), 6, Some(20 * THROUGHPUT)).await?;
    cluster.run(CREATE).await?;
    Ok(cluster)
}

async fn wait_initial_data(cluster: &mut NexmarkCluster) -> Result<String> {
    cluster
        .wait_until_non_empty(SELECT, INITIAL_INTERVAL, INITIAL_TIMEOUT)
        .await
}

#[madsim::test]
async fn nexmark_q4_ref() -> Result<()> {
    let mut cluster = init().await?;

    sleep(Duration::from_secs(25)).await;
    cluster.run(SELECT).await?.assert_result_eq(RESULT);

    Ok(())
}

async fn nexmark_q4_common_inner(predicates: Vec<BoxedPredicate>) -> Result<()> {
    let mut cluster = init().await?;

    let fragment = cluster.locate_one_fragment(predicates).await?;
    let id = fragment.id();

    // 0s
    wait_initial_data(&mut cluster)
        .await?
        .assert_result_ne(RESULT);

    // 0~10s
    cluster.reschedule(format!("{id}-[0,1]")).await?;

    sleep(Duration::from_secs(5)).await;

    // 5~15s
    cluster.run(SELECT).await?.assert_result_ne(RESULT);
    cluster.reschedule(format!("{id}-[2,3]+[0,1]")).await?;

    sleep(Duration::from_secs(20)).await;

    // 25~35s
    cluster.run(SELECT).await?.assert_result_eq(RESULT);

    Ok(())
}

fn nexmark_q4_common(predicates: Vec<BoxedPredicate>) -> BoxFuture<'static, Result<()>> {
    Box::pin(nexmark_q4_common_inner(predicates))
}

#[madsim::test]
async fn nexmark_q4_materialize_agg() -> Result<()> {
    nexmark_q4_common(vec![
        identity_contains("materialize"),
        identity_contains("hashagg"),
    ])
    .await
}

#[madsim::test]
#[ignore = "there's some problem for scaling nexmark source"]
async fn nexmark_q4_source() -> Result<()> {
    nexmark_q4_common(vec![identity_contains("source: \"bid\"")]).await
}

#[madsim::test]
async fn nexmark_q4_agg_join() -> Result<()> {
    nexmark_q4_common(vec![
        identity_contains("hashagg"),
        identity_contains("hashjoin"),
        upstream_fragment_count(2),
    ])
    .await
}

#[madsim::test]
async fn nexmark_q4_cascade() -> Result<()> {
    let mut cluster = init().await?;

    let fragment_1 = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("hashagg"),
        ])
        .await?;
    let id_1 = fragment_1.id();

    let fragment_2 = cluster
        .locate_one_fragment(vec![
            identity_contains("hashagg"),
            identity_contains("hashjoin"),
            upstream_fragment_count(2),
        ])
        .await?;
    let id_2 = fragment_2.id();

    // 0s
    wait_initial_data(&mut cluster)
        .await?
        .assert_result_ne(RESULT);

    // 0~10s
    cluster
        .reschedule(format!("{id_1}-[0,1]; {id_2}-[0,2,4]"))
        .await?;

    sleep(Duration::from_secs(5)).await;

    // 5~15s
    cluster.run(SELECT).await?.assert_result_ne(RESULT);
    cluster
        .reschedule(format!("{id_1}-[2,4]+[0,1]; {id_2}-[3]+[0,4]"))
        .await?;

    sleep(Duration::from_secs(20)).await;

    // 25~35s
    cluster.run(SELECT).await?.assert_result_eq(RESULT);

    Ok(())
}

// https://github.com/risingwavelabs/risingwave/issues/5567
#[madsim::test]
async fn nexmark_q4_materialize_agg_cache_invalidation() -> Result<()> {
    let mut cluster = init().await?;

    let fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("hashagg"),
        ])
        .await?;
    let id = fragment.id();

    // Let parallel unit 0 handle all groups.
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(10)).await;
    let result_1 = cluster.run(SELECT).await?.assert_result_ne(RESULT);

    // Scale out.
    cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;
    cluster
        .run(SELECT)
        .await?
        .assert_result_ne(result_1)
        .assert_result_ne(RESULT);

    // Let parallel unit 0 handle all groups again.
    // Note that there're only 5 groups, so if the parallel unit 0 doesn't invalidate the cache
    // correctly, it will yield the wrong result.
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(20)).await;

    cluster.run(SELECT).await?.assert_result_eq(RESULT);

    Ok(())
}
