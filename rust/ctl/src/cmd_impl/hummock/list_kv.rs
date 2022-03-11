use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

pub async fn list_kv() -> anyhow::Result<()> {
    let hummock_opts = HummockServiceOpts::from_env()?;
    let hummock = hummock_opts.create_hummock_store().await?;
    // TODO: support speficy epoch
    tracing::info!("using u64::MAX as epoch");

    for (k, v) in hummock.scan::<_, Vec<u8>>(.., None, u64::MAX).await? {
        println!("{:?} => {:?}", k, v);
    }

    Ok(())
}
