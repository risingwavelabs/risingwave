use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::hummock::{HummockSnapshot, PinSnapshotRequest, UnpinSnapshotRequest};
use risingwave_rpc_client::MetaClient;

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
            .await
            .to_rw_result()?;
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
                snapshot: Some(HummockSnapshot { epoch }),
            })
            .await
            .to_rw_result()?;
        Ok(())
    }
}
