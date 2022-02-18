use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, CommitEpochRequest, CompactTask, GetNewTableIdRequest, HummockSnapshot,
    HummockVersion, PinSnapshotRequest, PinVersionRequest, SstableInfo, UnpinSnapshotRequest,
    UnpinVersionRequest,
};

use crate::hummock::hummock_meta_client::{HummockMetaClient, GLOBAL_COLUMN_FAMILY_ID};
use crate::hummock::mock::MockHummockMetaService;
use crate::hummock::{HummockEpoch, HummockResult, HummockSSTableId, HummockVersionId};

pub struct MockHummockMetaClient {
    mock_hummock_meta_service: Arc<MockHummockMetaService>,
}

impl MockHummockMetaClient {
    pub fn new(mock_hummock_meta_service: Arc<MockHummockMetaService>) -> MockHummockMetaClient {
        MockHummockMetaClient {
            mock_hummock_meta_service,
        }
    }
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<HummockVersion> {
        let response = self
            .mock_hummock_meta_service
            .pin_version(PinVersionRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: 0,
            });
        Ok(response.pinned_version.unwrap())
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.mock_hummock_meta_service
            .unpin_version(UnpinVersionRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: 0,
                pinned_version_id,
            });
        Ok(())
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        let epoch = self
            .mock_hummock_meta_service
            .pin_snapshot(PinSnapshotRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: 0,
            })
            .snapshot
            .unwrap()
            .epoch;
        Ok(epoch)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.mock_hummock_meta_service
            .unpin_snapshot(UnpinSnapshotRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: 0,
                snapshot: Some(HummockSnapshot {
                    epoch: pinned_epoch,
                }),
            });
        Ok(())
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        let table_id = self
            .mock_hummock_meta_service
            .get_new_table_id(GetNewTableIdRequest {})
            .table_id;
        Ok(table_id)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersion> {
        let resp = self.mock_hummock_meta_service.add_tables(AddTablesRequest {
            column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
            context_id: 0,
            sstable_info: sstables.to_vec(),
            epoch,
        });
        Ok(resp.version.unwrap())
    }

    async fn get_compaction_task(&self) -> HummockResult<Option<CompactTask>> {
        unimplemented!()
    }

    async fn report_compaction_task(
        &self,
        _compact_task: CompactTask,
        _task_result: bool,
    ) -> HummockResult<()> {
        unimplemented!()
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        self.mock_hummock_meta_service
            .commit_epoch(CommitEpochRequest { epoch });
        Ok(())
    }

    async fn abort_epoch(&self, _epoch: HummockEpoch) -> HummockResult<()> {
        unimplemented!()
    }
}
