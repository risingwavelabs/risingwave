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
use madsim::time::sleep;
use risingwave_simulation_scale::cluster::Cluster;
use risingwave_simulation_scale::ctl_ext::predicate::{identity_contains, upstream_fragment_count};
use risingwave_simulation_scale::utils::AssertResult;

const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q4
AS
SELECT
    Q.category,
    AVG(Q.final) as avg
FROM (
    SELECT
        MAX(B.price) AS final,A.category
    FROM
        auction A,
        bid B
    WHERE
        A.id = B.auction AND
        B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY
        A.id,A.category
    ) Q
GROUP BY
    Q.category;
"#;

const SELECT: &str = "select * from nexmark_q4 order by category;";

const RESULT: &str = r#"
10 29168119.954198473282442748092
11 29692848.961698200276880479926
12 30833586.802419354838709677419
13 28531264.892390814948221521837
14 29586298.617934551636209094773
"#;

async fn init() -> Result<Cluster> {
    let mut cluster = Cluster::start().await?;
    cluster.create_nexmark_source(6, Some(200000)).await?;
    cluster.run(CREATE).await?;
    Ok(cluster)
}

async fn wait_initial_data(cluster: &mut Cluster) -> Result<String> {
    cluster
        .wait_until(
            SELECT,
            |r| !r.trim().is_empty(),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .await
}

#[madsim::test]
async fn nexmark_q4_ref() -> Result<()> {
    let mut cluster = init().await?;

    sleep(Duration::from_secs(25)).await;
    cluster.run(SELECT).await?.assert_result_eq(RESULT);

    Ok(())
}

#[madsim::test]
async fn nexmark_q4_materialize_agg() -> Result<()> {
    let mut cluster = init().await?;

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            identity_contains("hashagg"),
        ])
        .await?;
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
async fn nexmark_q4_agg_join() -> Result<()> {
    let mut cluster = init().await?;

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("hashagg"),
            identity_contains("hashjoin"),
            upstream_fragment_count(2),
        ])
        .await?;
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
