use std::env;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use risingwave_common::config::StorageConfig;
use risingwave_storage::hummock::HummockStateStore;
use risingwave_storage::monitor::{MonitoredStateStore, StateStoreStats};
use risingwave_storage::StateStoreImpl;

use super::MetaServiceOpts;

pub struct HummockServiceOpts {
    pub meta_opts: MetaServiceOpts,
    pub hummock_url: String,
}

impl HummockServiceOpts {
    /// Recover hummock service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_HUMMOCK_URL`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_opts = MetaServiceOpts::from_env()?;
        let hummock_url = env::var("RW_HUMMOCK_URL").unwrap_or_else(|_| {
            const DEFAULT_ADDR: &str = "hummock+minio://hummock:12345678@127.0.0.1:9301/hummock001";
            tracing::warn!(
                "`RW_HUMMOCK_URL` not found, using default hummock URL {}",
                DEFAULT_ADDR
            );
            DEFAULT_ADDR.to_string()
        });
        Ok(Self {
            meta_opts,
            hummock_url,
        })
    }

    pub async fn create_hummock_store(&self) -> Result<MonitoredStateStore<HummockStateStore>> {
        let meta_client = self.meta_opts.create_meta_client().await?;

        // FIXME: allow specify custom config
        let config = StorageConfig::default();

        tracing::info!("using Hummock config: {:#?}", config);

        let state_store_impl = StateStoreImpl::new(
            &self.hummock_url,
            Arc::new(config),
            meta_client,
            Arc::new(StateStoreStats::unused()),
        )
        .await?;

        if let StateStoreImpl::HummockStateStore(hummock_state_store) = state_store_impl {
            Ok(hummock_state_store)
        } else {
            Err(anyhow!("only Hummock state store is supported in risectl"))
        }
    }
}
