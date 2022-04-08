use risingwave_common::error::Result;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};

/// A wrapper around the `MetaClient` that only provides a minor set of meta rpc.
/// Most of the rpc to meta are delegated by other separate structs like `CatalogWriter`,
/// `WorkerNodeManager`, etc. So frontend rarely needs to call `MetaClient` directly.
/// Hence instead of to mock all rpc of `MetaClient` in tests, we aggregate those "direct" rpc
/// in this trait so that the mocking can be simplified.
#[async_trait::async_trait]
pub trait FrontendMetaClient: Send + Sync {
    async fn pin_snapshot(&self, last_pinned: u64) -> Result<u64>;

    async fn flush(&self) -> Result<()>;

    async fn unpin_snapshot(&self, epoch: u64) -> Result<()>;
}

pub struct FrontendMetaClientImpl(pub MetaClient);

#[async_trait::async_trait]
impl FrontendMetaClient for FrontendMetaClientImpl {
    async fn pin_snapshot(&self, last_pinned: u64) -> Result<u64> {
        self.0.pin_snapshot(last_pinned).await
    }

    async fn flush(&self) -> Result<()> {
        self.0.flush().await
    }

    async fn unpin_snapshot(&self, epoch: u64) -> Result<()> {
        self.0.unpin_snapshot(&[epoch]).await
    }
}
