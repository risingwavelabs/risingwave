use anyhow::Result;
use risingwave_simulation_scale::cluster::Cluster;
use risingwave_simulation_scale::utils::AssertResult;

#[madsim::test]
async fn test_hello() -> Result<()> {
    let mut cluster = Cluster::start().await?;
    cluster
        .run("select concat_ws(', ', 'hello', 'world');")
        .await?
        .assert_result_eq("hello, world");

    Ok(())
}
