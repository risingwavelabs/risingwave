use risingwave_common::error::Result;
use risingwave_pb::hummock::{HummockSnapshot, PinSnapshotRequest, UnpinSnapshotRequest};
use risingwave_rpc_client::MetaClient;

/// A wrapper around the `MetaClient` that only provides a minor set of meta rpc.
/// Most of the rpc to meta are delegated by other separate structs like `CatalogWriter`,
/// `WorkerNodeManager`, etc. So frontend rarely needs to call `MetaClient` directly.
/// Hence instead of to mock all rpc of `MetaClient` in tests, we aggregate those "direct" rpc
/// in this trait so that the mocking can be simplified.
#[async_trait::async_trait]
pub trait FrontendMetaClient: Send + Sync {
    async fn pin_snapshot(&self) -> Result<u64>;

    async fn flush(&self) -> Result<()>;

    async fn unpin_snapshot(&self, epoch: u64) -> Result<()>;
}

pub struct FrontendMetaClientImpl(pub MetaClient);

#[async_trait::async_trait]
impl FrontendMetaClient for FrontendMetaClientImpl {
    async fn pin_snapshot(&self) -> Result<u64> {
        let resp = self
            .0
            .inner
            .pin_snapshot(PinSnapshotRequest {
                context_id: 0,
                // u64::MAX always return the greatest current epoch. Use correct `last_pinned` when
                // retrying this RPC.
                last_pinned: u64::MAX,
            })
            .await?;
        Ok(resp.get_snapshot()?.epoch)
    }

    async fn flush(&self) -> Result<()> {
        self.0.flush().await
    }

    async fn unpin_snapshot(&self, epoch: u64) -> Result<()> {
        self.0
            .inner
            .unpin_snapshot(UnpinSnapshotRequest {
                context_id: 0,
                snapshots: vec![HummockSnapshot { epoch }],
            })
            .await?;
        Ok(())
    }
}
