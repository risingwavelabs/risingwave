// Copyright 2025 RisingWave Labs
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

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::nexmark::NexmarkCluster;
use risingwave_simulation::utils::AssertResult;
use tokio::time::{sleep, timeout};

const SET_PARAMETER: &str = "ALTER SYSTEM SET pause_on_next_bootstrap TO true";

#[derive(Clone, Copy)]
enum ResumeBy {
    Risectl,
    Restart,
}

impl ResumeBy {
    async fn resume(self, cluster: &mut Cluster) -> Result<()> {
        match self {
            ResumeBy::Risectl => cluster.resume().await?,
            ResumeBy::Restart => cluster.kill_nodes(["meta-1"], 0).await,
        };
        Ok(())
    }
}

async fn test_impl(resume_by: ResumeBy) -> Result<()> {
    const CREATE_TABLE: &str = "CREATE TABLE t (v int)";
    const INSERT_INTO_TABLE: &str = "INSERT INTO t VALUES (1)";
    const SELECT_COUNT_TABLE: &str = "SELECT COUNT(*) FROM t";

    const CREATE: &str = "CREATE MATERIALIZED VIEW count_bid as SELECT COUNT(*) FROM bid";
    const SELECT: &str = "SELECT * FROM count_bid";

    const CREATE_2: &str = "CREATE MATERIALIZED VIEW count_auction as SELECT COUNT(*) FROM auction";
    const SELECT_2: &str = "SELECT * FROM count_auction";

    const CREATE_VALUES: &str = "CREATE MATERIALIZED VIEW values as VALUES (1), (2), (3)";
    const SELECT_VALUES: &str = "SELECT count(*) FROM values";

    let mut cluster = NexmarkCluster::new(
        Configuration {
            meta_nodes: 1,
            ..Configuration::for_scale()
        },
        6,
        None,
        false,
    )
    .await?;

    cluster.run(SET_PARAMETER).await?;
    cluster.run(CREATE).await?;
    cluster.run(CREATE_TABLE).await?;

    // Run for a while.
    sleep(Duration::from_secs(10)).await;

    // Kill the meta node and wait for the service to recover.
    cluster.kill_nodes(["meta-1"], 0).await;
    sleep(Duration::from_secs(10)).await;

    // The source should be paused.
    let count = cluster.run(SELECT).await?;
    sleep(Duration::from_secs(10)).await;
    cluster.run(SELECT).await?.assert_result_eq(&count);

    // Scaling will trigger a pair of `Pause` and `Resume`. However, this should not affect the
    // "manual" pause.
    let random_fragment_id = cluster.locate_random_fragment().await?;
    cluster
        .reschedule(random_fragment_id.random_reschedule())
        .await?;
    sleep(Duration::from_secs(10)).await;
    cluster.run(SELECT).await?.assert_result_eq(&count);

    // New streaming jobs should also start from paused.
    cluster.run(CREATE_2).await?;
    sleep(Duration::from_secs(10)).await;
    cluster.run(SELECT_2).await?.assert_result_eq("0"); // even there's no data from source, the aggregation
    // result will be 0 instead of empty or NULL

    // `VALUES` should also be paused.
    tokio::time::timeout(Duration::from_secs(10), cluster.run(CREATE_VALUES))
        .await
        .expect_err("`VALUES` should be paused so creation should never complete");

    // DML on tables should be blocked.
    let result = timeout(Duration::from_secs(10), cluster.run(INSERT_INTO_TABLE)).await;
    assert!(result.is_err());
    cluster.run(SELECT_COUNT_TABLE).await?.assert_result_eq("0");

    // Resume the cluster.
    resume_by.resume(&mut cluster).await?;
    sleep(Duration::from_secs(10)).await;

    // The source should be resumed.
    let new_count = cluster.run(SELECT).await?;
    assert_ne!(count, new_count);

    // DML on tables should be allowed. However, we're uncertain whether the previous blocked DML is
    // executed or not. So we just check the count difference.
    {
        let mut session = cluster.start_session();

        session.flush().await?;
        let count: i64 = session.run(SELECT_COUNT_TABLE).await?.parse().unwrap();

        session.run(INSERT_INTO_TABLE).await?;
        session.flush().await?;
        session
            .run(SELECT_COUNT_TABLE)
            .await?
            .assert_result_eq(format!("{}", count + 1));
    }

    if let ResumeBy::Risectl = resume_by {
        // `VALUES` should be successfully created
        cluster.run(SELECT_VALUES).await?.assert_result_eq("3");
    }

    Ok(())
}

#[tokio::test]
async fn test_pause_on_bootstrap_resume_by_risectl() -> Result<()> {
    test_impl(ResumeBy::Risectl).await
}

#[tokio::test]
async fn test_pause_on_bootstrap_resume_by_restart() -> Result<()> {
    test_impl(ResumeBy::Restart).await
}

// The idea is similar to `e2e_test/batch/transaction/now.slt`.
async fn test_temporal_filter(resume_by: ResumeBy) -> Result<()> {
    const CREATE_TABLE: &str = "create table t (ts timestamp)";
    const CREATE_TEMPORAL_FILTER: &str = "create materialized view mv as select count(*) from t where ts at time zone 'utc' >= now()";
    const INSERT_TIMESTAMPS: &str = "
    insert into t select * from generate_series(
        now() at time zone 'utc' - interval '10' second,
        now() at time zone 'utc' + interval '20' second,
        interval '1' second / 20
    );
    ";
    const SELECT: &str = "select * from mv";

    let mut cluster = Cluster::start(Configuration {
        meta_nodes: 1,
        ..Configuration::for_scale()
    })
    .await?;

    cluster.run(SET_PARAMETER).await?;

    {
        let mut session = cluster.start_session();
        session.run(CREATE_TABLE).await?;
        session.run(CREATE_TEMPORAL_FILTER).await?;
        session.run(INSERT_TIMESTAMPS).await?;
        session.flush().await?;
    };

    // Kill the meta node and wait for the service to recover.
    cluster.kill_nodes(["meta-1"], 0).await;
    sleep(Duration::from_secs(10)).await;

    let count: i32 = cluster.run(SELECT).await?.parse()?;
    assert_ne!(count, 0, "the following tests are meaningless");

    sleep(Duration::from_secs(10)).await;
    let new_count: i32 = cluster.run(SELECT).await?.parse()?;
    assert_eq!(count, new_count, "temporal filter should have been paused");

    // Resume the cluster.
    resume_by.resume(&mut cluster).await?;
    sleep(Duration::from_secs(40)).await; // 40 seconds is enough for all timestamps to be expired

    let count: i32 = cluster.run(SELECT).await?.parse()?;
    assert_eq!(count, 0, "temporal filter should have been resumed");

    Ok(())
}

#[tokio::test]
async fn test_pause_on_bootstrap_temporal_filter_resume_by_risectl() -> Result<()> {
    test_temporal_filter(ResumeBy::Risectl).await
}

#[tokio::test]
async fn test_pause_on_bootstrap_temporal_filter_resume_by_restart() -> Result<()> {
    test_temporal_filter(ResumeBy::Restart).await
}
