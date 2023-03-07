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
use madsim::time::sleep;
use risingwave_simulation::cluster::Configuration;
use risingwave_simulation::ctl_ext::predicate::{
    identity_contains, upstream_fragment_count, BoxedPredicate,
};
use risingwave_simulation::nexmark::queries::q4::*;
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;

#[cfg(target_os = "linux")]
const RESULT: &str = r#"
10 28586726.812614259597806215722
11 29089413.538191395961369622476
12 29245370.435142594296228150874
13 30910968.113309352517985611511
14 26961712.806972789115646258503
"#;

#[cfg(target_os = "macos")]
const RESULT: &str = r#"
10 28586726.810786106032906764168
11 29089413.531167690956979806848
12 29245370.427782888684452621895
13 30910968.113309352517985611511
14 26961712.806972789115646258503
"#;

async fn init() -> Result<NexmarkCluster> {
    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(20 * THROUGHPUT), false).await?;
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

async fn nexmark_q4_common(predicates: impl IntoIterator<Item = BoxedPredicate>) -> Result<()> {
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

#[madsim::test]
async fn nexmark_q4_materialize_agg() -> Result<()> {
    nexmark_q4_common([
        identity_contains("materialize"),
        identity_contains("hashagg"),
    ])
    .await
}

#[madsim::test]
async fn nexmark_q4_source() -> Result<()> {
    nexmark_q4_common([identity_contains("source: \"bid\"")]).await
}

#[madsim::test]
async fn nexmark_q4_agg_join() -> Result<()> {
    nexmark_q4_common([
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
        .locate_one_fragment([
            identity_contains("materialize"),
            identity_contains("hashagg"),
        ])
        .await?;
    let id_1 = fragment_1.id();

    let fragment_2 = cluster
        .locate_one_fragment([
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
        .locate_one_fragment([
            identity_contains("materialize"),
            identity_contains("hashagg"),
        ])
        .await?;
    let id = fragment.id();

    // Let parallel unit 0 handle all groups.
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(7)).await;
    let result_1 = cluster.run(SELECT).await?.assert_result_ne(RESULT);

    // Scale out.
    cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(7)).await;
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
