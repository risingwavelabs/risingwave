use risingwave_storage::hummock::hummock_meta_client::HummockMetaClient;

use crate::common::MetaServiceOpts;

pub async fn list_version() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let (_, hummock_client) = meta_opts.create_hummock_meta_client().await?;
    let version = hummock_client.pin_version(u64::MAX).await?;
    println!("{:#?}", version);
    hummock_client.unpin_version(version.id).await?;
    Ok(())
}
