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
use futures::future::BoxFuture;
use risingwave_simulation::cluster::{Configuration, Session};
use risingwave_simulation::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation::utils::AssertResult;
use tokio::time::{interval, sleep, timeout};

const RESULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RESULT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const STABLE_SAMPLE_COUNT: usize = 3;

async fn wait_until_result_stable(
    session: &mut Session,
    sql: &str,
    poll_interval: Duration,
    timeout_duration: Duration,
) -> Result<String> {
    let mut last: Option<String> = None;
    let mut same_count = 0;
    let fut = async {
        let mut ticker = interval(poll_interval);
        loop {
            ticker.tick().await;
            let result = session.run(sql).await?;
            let trimmed = result.trim();
            if trimmed.is_empty() {
                continue;
            }
            let is_same = last.as_deref().is_some_and(|prev| prev.trim() == trimmed);
            if is_same {
                same_count += 1;
            } else {
                last = Some(result.clone());
                same_count = 1;
            }
            if same_count >= STABLE_SAMPLE_COUNT {
                return Ok::<_, anyhow::Error>(result);
            }
        }
    };

    match timeout(timeout_duration, fut).await {
        Ok(r) => Ok(r?),
        Err(_) => bail!("wait_until_result_stable timeout"),
    }
}

async fn wait_until_result_eq(
    session: &mut Session,
    sql: &str,
    expected: &str,
    poll_interval: Duration,
    timeout_duration: Duration,
) -> Result<String> {
    let expected = expected.trim().to_string();
    let fut = async {
        let mut ticker = interval(poll_interval);
        loop {
            ticker.tick().await;
            let result = session.run(sql).await?;
            if result.trim() == expected {
                return Ok::<_, anyhow::Error>(result);
            }
        }
    };

    match timeout(timeout_duration, fut).await {
        Ok(r) => Ok(r?),
        Err(_) => bail!("wait_until_result_eq timeout"),
    }
}

/// Common code for Nexmark chaos tests.
///
/// - If `MULTIPLE` is false, we'll randomly pick a single fragment and reschedule it twice.
/// - If `MULTIPLE` is true, we'll randomly pick random number of fragments and reschedule them,
///   then pick another set to reschedule again.
async fn nexmark_chaos_common_inner(
    query_name: &'static str,
    create: &'static str,
    select: &'static str,
    drop: &'static str,
    initial_interval: Duration,
    initial_timeout: Duration,
    after_scale_duration: Duration,
    _multiple: bool,
    watermark: bool,
) -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_cores = configuration.total_streaming_cores();
    let mut cluster =
        NexmarkCluster::new(configuration, 6, Some(20 * THROUGHPUT), watermark).await?;
    let mut session = cluster.start_session();
    session.run(create).await?;
    sleep(Duration::from_secs(30)).await;
    let final_result = wait_until_result_stable(
        &mut session,
        select,
        RESULT_POLL_INTERVAL,
        RESULT_WAIT_TIMEOUT,
    )
    .await?;
    session.run(drop).await?;
    sleep(Duration::from_secs(5)).await;

    println!("Reference run done.");
    if final_result.trim().is_empty() {
        anyhow::bail!("Reference run result is empty. Check the query.")
    }
    // Create a new session for the chaos run.
    let mut session = cluster.start_session();
    session.run(create).await?;

    let _initial_result = cluster
        .wait_until_non_empty(select, initial_interval, initial_timeout)
        .await?
        .assert_result_ne(&final_result);

    let (parallelism_1, parallelism_2) = {
        use rand::{Rng, rng as thread_rng};
        let rng = &mut thread_rng();

        let parallelism_1 = rng.random_range(1..=total_cores);
        let parallelism_2 = rng.random_range(1..=total_cores);

        (parallelism_1, parallelism_2)
    };

    cluster
        .run(format!(
            "alter materialized view nexmark_{} set parallelism = {};",
            query_name, parallelism_1,
        ))
        .await?;

    sleep(after_scale_duration).await;
    session.run(select).await?.assert_result_ne(&final_result);

    cluster
        .run(format!(
            "alter materialized view nexmark_{} set parallelism = {};",
            query_name, parallelism_2,
        ))
        .await?;

    let result = if watermark {
        wait_until_result_stable(
            &mut session,
            select,
            RESULT_POLL_INTERVAL,
            RESULT_WAIT_TIMEOUT,
        )
        .await?
    } else {
        wait_until_result_eq(
            &mut session,
            select,
            &final_result,
            RESULT_POLL_INTERVAL,
            RESULT_WAIT_TIMEOUT,
        )
        .await?
    };
    if watermark {
        if result.trim() != final_result.trim() {
            println!(
                "Warn: results mismatch, which might be expected since watermark is used.\nDiff:\n{}",
                pretty_assertions::StrComparison::new(&final_result, &result)
            )
        }
    } else {
        result.assert_result_eq(&final_result);
    }

    Ok(())
}

fn nexmark_chaos_common(
    query_name: &'static str,
    create: &'static str,
    select: &'static str,
    drop: &'static str,
    initial_interval: Duration,
    initial_timeout: Duration,
    after_scale_duration: Duration,
    multiple: bool,
    watermark: bool,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(nexmark_chaos_common_inner(
        query_name,
        create,
        select,
        drop,
        initial_interval,
        initial_timeout,
        after_scale_duration,
        multiple,
        watermark,
    ))
}

macro_rules! test {
    ($query:ident) => {
        test!($query, Duration::from_secs(1));
    };
    ($query:ident, $after_scale_duration:expr) => {
        paste::paste! {
            #[tokio::test]
            async fn [< nexmark_chaos_ $query _single >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_chaos_common(
                    stringify!($query),
                    CREATE,
                    SELECT,
                    DROP,
                    INITIAL_INTERVAL,
                    INITIAL_TIMEOUT,
                    $after_scale_duration,
                    false,
                    WATERMARK,
                )
                .await
            }

            #[tokio::test]
            async fn [< nexmark_chaos_ $query _multiple >]() -> Result<()> {
                use risingwave_simulation::nexmark::queries::$query::*;
                nexmark_chaos_common(
                    stringify!($query),
                    CREATE,
                    SELECT,
                    DROP,
                    INITIAL_INTERVAL,
                    INITIAL_TIMEOUT,
                    $after_scale_duration,
                    true,
                    WATERMARK,
                )
                .await
            }
        }
    };
}

// q0, q1, q2: too trivial
test!(q3);
test!(q4);
test!(q5);
test!(q5_eowc);
test!(q7);
test!(q7_eowc);
test!(q8);
test!(q9);
// q10+: duplicated or unsupported
test!(q15);
test!(q18);

// Self made queries.
test!(q101);
test!(q102);
test!(q103);
test!(q104);
test!(q105);
test!(q106);
test!(q107_eowc);
