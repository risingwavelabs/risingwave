use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Configuration, KillOpts};
use risingwave_simulation::nexmark::NexmarkCluster;
use risingwave_simulation::utils::AssertResult;
use tokio::time::{sleep, timeout};

const CREATE_TABLE: &str = "CREATE TABLE t (v int)";
const INSERT_INTO_TABLE: &str = "INSERT INTO t VALUES (1)";
const SELECT_COUNT_TABLE: &str = "SELECT COUNT(*) FROM t";

const CREATE: &str = "CREATE MATERIALIZED VIEW count_bid as SELECT COUNT(*) FROM bid";
const SELECT: &str = "SELECT * FROM count_bid";

const CREATE_2: &str = "CREATE MATERIALIZED VIEW count_auction as SELECT COUNT(*) FROM auction";
const SELECT_2: &str = "SELECT * FROM count_auction";

const SET_PARAMETER: &str = "ALTER SYSTEM SET pause_on_next_bootstrap TO true";

enum ResumeBy {
    Risectl,
    Restart,
}

async fn test_impl(resume_by: ResumeBy) -> Result<()> {
    let mut cluster = NexmarkCluster::new(Configuration::for_scale(), 6, None, false).await?;

    cluster.run(SET_PARAMETER).await?;
    cluster.run(CREATE).await?;
    cluster.run(CREATE_TABLE).await?;

    // Run for a while.
    sleep(Duration::from_secs(10)).await;

    // Kill all nodes and wait for the service to recover.
    cluster.kill_node(&KillOpts::ALL).await;
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
    cluster.run(SELECT_2).await?.assert_result_eq("0"); // even there's no data from source, the
                                                        // result will be 0 instead of empty or NULL

    // DML on tables should be blocked.
    let result = timeout(Duration::from_secs(10), cluster.run(INSERT_INTO_TABLE)).await;
    assert!(result.is_err());
    cluster.run(SELECT_COUNT_TABLE).await?.assert_result_eq("0");

    match resume_by {
        ResumeBy::Risectl => cluster.resume().await?,
        ResumeBy::Restart => cluster.kill_node(&KillOpts::ALL).await,
    }
    sleep(Duration::from_secs(10)).await;

    // The source should be resumed.
    let new_count = cluster.run(SELECT).await?;
    assert_ne!(count, new_count);

    // DML on tables should be allowed.
    {
        let mut session = cluster.start_session();
        session.run(INSERT_INTO_TABLE).await?;
        session.run("FLUSH").await?;
        cluster.run(SELECT_COUNT_TABLE).await?.assert_result_eq("1");
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
