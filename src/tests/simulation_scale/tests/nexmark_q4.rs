use std::time::Duration;

use anyhow::Result;
use madsim::time::sleep;
use risingwave_simulation_scale::cluster::Cluster;
use risingwave_simulation_scale::ctl_ext::predicates::identity_contains;
use risingwave_simulation_scale::utils::AssertResult;

const CREATE_MVIEW: &str = r#"
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

const RESULT: &str = r#"
10 29168119.958688819039066008083
11 29692848.947854176280572219659
12 30833586.803315412186379928315
13 28531264.89509230076542098154
14 29586298.618359541011474713132
"#;

const SELECT: &str = "select * from nexmark_q4 order by category;";

#[madsim::test]
async fn test_nexmark_q4() -> Result<()> {
    let mut cluster = Cluster::start().await?;
    cluster.create_nexmark_source(6, Some(200000)).await?;
    cluster.run(CREATE_MVIEW).await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    let id = fragment.id();

    // 0s
    cluster
        .wait_until(
            SELECT,
            |r| !r.trim().is_empty(),
            Duration::from_millis(1000),
            Duration::from_secs(10),
        )
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
