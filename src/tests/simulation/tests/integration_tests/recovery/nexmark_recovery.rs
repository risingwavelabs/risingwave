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

use std::time::Duration;

use anyhow::{Result, bail};
use risingwave_simulation::cluster::{Configuration, KillOpts};
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const RESULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RESULT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const STABLE_SAMPLE_COUNT: usize = 3;

async fn wait_until_result_stable(cluster: &mut NexmarkCluster, sql: &str) -> Result<String> {
    let mut last: Option<String> = None;
    let mut same_count = 0;
    let result = cluster
        .wait_until(
            sql,
            |r| {
                let trimmed = r.trim();
                if trimmed.is_empty() {
                    return false;
                }
                let is_same = last.as_deref().is_some_and(|prev| prev.trim() == trimmed);
                if is_same {
                    same_count += 1;
                } else {
                    last = Some(r.to_string());
                    same_count = 1;
                }
                same_count >= STABLE_SAMPLE_COUNT
            },
            RESULT_POLL_INTERVAL,
            RESULT_WAIT_TIMEOUT,
        )
        .await?;

    if result.trim().is_empty() {
        bail!("wait_until_result_stable returned empty result");
    }
    Ok(result)
}

/// Setup a nexmark stream, inject failures, and verify results.
async fn nexmark_recovery_common(create: &str, select: &str, drop: &str) -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();

    let mut cluster =
        NexmarkCluster::new(Configuration::for_scale(), 6, Some(THROUGHPUT * 20), false).await?;

    // get the output without failures as the standard result
    cluster.run(create).await?;
    sleep(Duration::from_secs(30)).await;
    let expected = wait_until_result_stable(&mut cluster, select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(5)).await;

    cluster.run(create).await?;

    // kill nodes and trigger recovery
    for _ in 0..5 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL).await;
    }
    // wait enough time to make sure the stream is end
    sleep(Duration::from_secs(60)).await;

    cluster
        .wait_until(
            select,
            |r| r.trim() == expected.trim(),
            RESULT_POLL_INTERVAL,
            RESULT_WAIT_TIMEOUT,
        )
        .await?
        .assert_result_eq(&expected);

    Ok(())
}

macro_rules! test {
    ($query:ident) => {
        paste::paste! {
            #[tokio::test]
            async fn [< nexmark_recovery_ $query >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_recovery_common(CREATE, SELECT, DROP)
                .await
            }
        }
    };
}

// q0, q1, q2: too trivial
test!(q3);
test!(q4);
test!(q5);
// q6: cannot plan
test!(q7);
test!(q8);
test!(q9);
// q10+: duplicated or unsupported

// Self made queries.
test!(q101);
test!(q102);
test!(q103);
test!(q104);
test!(q105);
