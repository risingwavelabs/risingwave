use std::env;
use std::sync::Arc;

use anyhow::Result;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::RpcHummockMetaClient;
use risingwave_storage::monitor::StateStoreStats;

pub struct MetaServiceOpts {
    pub meta_addr: String,
}

impl MetaServiceOpts {
    /// Recover meta service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_META_ADDR`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_addr = env::var("RW_META_ADDR").unwrap_or_else(|_| {
            const DEFAULT_ADDR: &str = "http://127.0.0.1:5690";
            tracing::warn!(
                "`RW_META_ADDR` not found, using default meta address {}",
                DEFAULT_ADDR
            );
            DEFAULT_ADDR.to_string()
        });
        Ok(Self { meta_addr })
    }

    /// Create meta client from options, and register as rise-ctl worker
    pub async fn create_meta_client(&self) -> Result<MetaClient> {
        let mut client = MetaClient::new(&self.meta_addr).await?;
        // FIXME: don't use 127.0.0.1 for ctl
        let worker_id = client
            .register("127.0.0.1:2333".parse().unwrap(), WorkerType::RiseCtl)
            .await?;
        tracing::info!("registered as RiseCtl worker, worker_id = {}", worker_id);
        // TODO: remove worker node
        client.set_worker_id(worker_id);
        Ok(client)
    }

    /// Create hummock meta client from options
    pub async fn create_hummock_meta_client(&self) -> Result<(MetaClient, RpcHummockMetaClient)> {
        let meta_client = self.create_meta_client().await?;
        let stats = Arc::new(StateStoreStats::unused());
        Ok((
            meta_client.clone(),
            RpcHummockMetaClient::new(meta_client, stats),
        ))
    }
}
