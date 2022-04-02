// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::hummock::{
    AddTablesRequest, CommitEpochRequest, CompactTask, GetNewTableIdRequest, HummockSnapshot,
    HummockVersion, PinSnapshotRequest, PinVersionRequest, SstableInfo,
    SubscribeCompactTasksResponse, UnpinSnapshotRequest, UnpinVersionRequest, VacuumTask,
};
use risingwave_rpc_client::HummockMetaClient;
use tonic::Streaming;

use crate::hummock::mock::MockHummockMetaService;
use crate::hummock::{HummockEpoch, HummockSSTableId, HummockVersionId};

/// Note: `MockHummockMetaClient` will be reimplemented by wrapping `HummockManager`.
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
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion> {
        let response = self
            .mock_hummock_meta_service
            .pin_version(PinVersionRequest {
                context_id: 0,
                last_pinned,
            });
        Ok(response.pinned_version.unwrap())
    }

    async fn unpin_version(&self, pinned_version_ids: &[HummockVersionId]) -> Result<()> {
        self.mock_hummock_meta_service
            .unpin_version(UnpinVersionRequest {
                context_id: 0,
                pinned_version_ids: pinned_version_ids.to_owned(),
            });
        Ok(())
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch> {
        let epoch = self
            .mock_hummock_meta_service
            .pin_snapshot(PinSnapshotRequest {
                context_id: 0,
                last_pinned,
            })
            .snapshot
            .unwrap()
            .epoch;
        Ok(epoch)
    }

    async fn unpin_snapshot(&self, pinned_epochs: &[HummockEpoch]) -> Result<()> {
        self.mock_hummock_meta_service
            .unpin_snapshot(UnpinSnapshotRequest {
                context_id: 0,
                snapshots: pinned_epochs
                    .iter()
                    .map(|epoch| HummockSnapshot {
                        epoch: epoch.to_owned(),
                    })
                    .collect_vec(),
            });
        Ok(())
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
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
    ) -> Result<HummockVersion> {
        let resp = self.mock_hummock_meta_service.add_tables(AddTablesRequest {
            context_id: 0,
            tables: sstables.to_vec(),
            epoch,
        });
        Ok(resp.version.unwrap())
    }

    async fn report_compaction_task(&self, _compact_task: CompactTask) -> Result<()> {
        unimplemented!()
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        self.mock_hummock_meta_service
            .commit_epoch(CommitEpochRequest { epoch });
        Ok(())
    }

    async fn abort_epoch(&self, _epoch: HummockEpoch) -> Result<()> {
        unimplemented!()
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        unimplemented!()
    }

    async fn report_vacuum_task(&self, _vacuum_task: VacuumTask) -> Result<()> {
        Ok(())
    }
}
