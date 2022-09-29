use anyhow::Result;
use risingwave_simulation_scale::cluster::Cluster;

#[madsim::test]
async fn test_hello() -> Result<()> {
    let mut cluster = Cluster::start().await?;
    cluster
        .run("select concat_ws(', ', 'hello', 'world');")
        .await?;

    Ok(())
}
