#![cfg(madsim)]

use std::time::Duration;

use anyhow::Result;
use madsim::time::sleep;
use risingwave_simulation_scale::cluster::Configuration;
use risingwave_simulation_scale::nexmark::{NexmarkCluster, THROUGHPUT};
use risingwave_simulation_scale::utils::AssertResult;

async fn nexmark_chaos_common(
    create: &'static str,
    select: &'static str,
    drop: &'static str,
    initial_interval: Duration,
    initial_timeout: Duration,
    after_scale_duration: Duration,
) -> Result<()> {
    let mut cluster =
        NexmarkCluster::new(Configuration::default(), 6, Some(20 * THROUGHPUT)).await?;
    cluster.run(create).await?;
    sleep(Duration::from_secs(30)).await;
    let ref_result = cluster.run(select).await?;
    cluster.run(drop).await?;
    sleep(Duration::from_secs(5)).await;

    cluster.run(create).await?;

    cluster
        .wait_until_non_empty(select, initial_interval, initial_timeout)
        .await?
        .assert_result_ne(&ref_result);

    let fragment = cluster.locate_random_fragment().await?;
    let id = fragment.id();
    cluster
        .reschedule(fragment.random_reschedule())
        .await?;

    sleep(after_scale_duration).await;
    cluster.run(select).await?.assert_result_ne(&ref_result);

    let fragment = cluster.locate_fragment_by_id(id).await?;
    cluster
        .reschedule(fragment.random_reschedule())
        .await?;

    sleep(Duration::from_secs(50)).await;

    cluster.run(select).await?.assert_result_eq(&ref_result);

    Ok(())
}

macro_rules! test {
    ($query:ident) => {
        #[madsim::test]
        async fn $query() -> Result<()> {
            use risingwave_simulation_scale::nexmark::queries::$query::*;
            nexmark_chaos_common(
                CREATE,
                SELECT,
                DROP,
                INITIAL_INTERVAL,
                INITIAL_TIMEOUT,
                Duration::from_secs(5),
            )
            .await
        }
    };
}

test!(q4);
