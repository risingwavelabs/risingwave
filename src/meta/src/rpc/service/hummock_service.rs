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

use std::collections::HashSet;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::catalog::{TableId, NON_RESERVED_PG_CATALOG_TABLE_ID};
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::*;
use tonic::{Request, Response, Status};

use crate::hummock::compaction::ManualCompactionOption;
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::hummock::{
    CompactionResumeTrigger, CompactorManagerRef, HummockManagerRef, VacuumManagerRef,
};
use crate::manager::FragmentManagerRef;
use crate::rpc::service::RwReceiverStream;
use crate::storage::MetaStore;
use crate::MetaError;

pub struct HummockServiceImpl<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
    vacuum_manager: VacuumManagerRef<S>,
    compaction_group_manager: CompactionGroupManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> HummockServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: CompactorManagerRef,
        vacuum_trigger: VacuumManagerRef<S>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        HummockServiceImpl {
            hummock_manager,
            compactor_manager,
            vacuum_manager: vacuum_trigger,
            compaction_group_manager,
            fragment_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> HummockManagerService for HummockServiceImpl<S>
where
    S: MetaStore,
{
    type SubscribeCompactTasksStream = RwReceiverStream<SubscribeCompactTasksResponse>;

    async fn unpin_version_before(
        &self,
        request: Request<UnpinVersionBeforeRequest>,
    ) -> Result<Response<UnpinVersionBeforeResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .unpin_version_before(req.context_id, req.unpin_version_before)
            .await?;
        Ok(Response::new(UnpinVersionBeforeResponse { status: None }))
    }

    async fn get_current_version(
        &self,
        _request: Request<GetCurrentVersionRequest>,
    ) -> Result<Response<GetCurrentVersionResponse>, Status> {
        let current_version = self.hummock_manager.get_current_version().await;
        Ok(Response::new(GetCurrentVersionResponse {
            status: None,
            current_version: Some(current_version),
        }))
    }

    async fn get_version_deltas(
        &self,
        request: Request<GetVersionDeltasRequest>,
    ) -> Result<Response<GetVersionDeltasResponse>, Status> {
        let req = request.into_inner();
        let version_deltas = self
            .hummock_manager
            .get_version_deltas(req.start_id, req.num_epochs)
            .await?;
        let resp = GetVersionDeltasResponse {
            version_deltas: Some(version_deltas),
        };
        Ok(Response::new(resp))
    }

    async fn report_compaction_tasks(
        &self,
        request: Request<ReportCompactionTasksRequest>,
    ) -> Result<Response<ReportCompactionTasksResponse>, Status> {
        let req = request.into_inner();
        match req.compact_task {
            None => Ok(Response::new(ReportCompactionTasksResponse {
                status: None,
            })),
            Some(compact_task) => {
                self.hummock_manager
                    .report_compact_task(req.context_id, &compact_task)
                    .await?;
                Ok(Response::new(ReportCompactionTasksResponse {
                    status: None,
                }))
            }
        }
    }

    async fn pin_snapshot(
        &self,
        request: Request<PinSnapshotRequest>,
    ) -> Result<Response<PinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let hummock_snapshot = self.hummock_manager.pin_snapshot(req.context_id).await?;
        Ok(Response::new(PinSnapshotResponse {
            status: None,
            snapshot: Some(hummock_snapshot),
        }))
    }

    async fn unpin_snapshot(
        &self,
        request: Request<UnpinSnapshotRequest>,
    ) -> Result<Response<UnpinSnapshotResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager.unpin_snapshot(req.context_id).await?;
        Ok(Response::new(UnpinSnapshotResponse { status: None }))
    }

    async fn unpin_snapshot_before(
        &self,
        request: Request<UnpinSnapshotBeforeRequest>,
    ) -> Result<Response<UnpinSnapshotBeforeResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .unpin_snapshot_before(req.context_id, req.min_snapshot.unwrap())
            .await?;
        Ok(Response::new(UnpinSnapshotBeforeResponse { status: None }))
    }

    async fn get_new_sst_ids(
        &self,
        request: Request<GetNewSstIdsRequest>,
    ) -> Result<Response<GetNewSstIdsResponse>, Status> {
        let sst_id_range = self
            .hummock_manager
            .get_new_sst_ids(request.into_inner().number)
            .await?;
        Ok(Response::new(GetNewSstIdsResponse {
            status: None,
            start_id: sst_id_range.start_id,
            end_id: sst_id_range.end_id,
        }))
    }

    async fn subscribe_compact_tasks(
        &self,
        request: Request<SubscribeCompactTasksRequest>,
    ) -> Result<Response<Self::SubscribeCompactTasksStream>, Status> {
        let req = request.into_inner();
        let context_id = req.context_id;
        // check_context and add_compactor as a whole is not atomic, but compactor_manager will
        // remove invalid compactor eventually.
        if !self.hummock_manager.check_context(context_id).await {
            return Err(Status::new(
                tonic::Code::Internal,
                format!("invalid hummock context {}", context_id),
            ));
        }
        let rx = self
            .compactor_manager
            .add_compactor(context_id, req.max_concurrent_task_number);
        // Trigger compaction on all compaction groups.
        for cg_id in self
            .hummock_manager
            .compaction_group_manager()
            .compaction_group_ids()
            .await
        {
            if let Err(e) = self.hummock_manager.try_send_compaction_request(cg_id) {
                tracing::warn!(
                    "Failed to schedule compaction for compaction group {}. {}",
                    cg_id,
                    e
                );
            }
        }
        self.hummock_manager
            .try_resume_compaction(CompactionResumeTrigger::CompactorAddition { context_id });
        Ok(Response::new(RwReceiverStream::new(rx)))
    }

    // TODO: convert this into a stream.
    async fn report_compaction_task_progress(
        &self,
        request: Request<ReportCompactionTaskProgressRequest>,
    ) -> Result<Response<ReportCompactionTaskProgressResponse>, Status> {
        let req = request.into_inner();
        self.compactor_manager
            .update_task_heartbeats(req.context_id, &req.progress);
        Ok(Response::new(ReportCompactionTaskProgressResponse {
            status: None,
        }))
    }

    async fn report_vacuum_task(
        &self,
        request: Request<ReportVacuumTaskRequest>,
    ) -> Result<Response<ReportVacuumTaskResponse>, Status> {
        if let Some(vacuum_task) = request.into_inner().vacuum_task {
            self.vacuum_manager.report_vacuum_task(vacuum_task).await?;
        }
        sync_point::sync_point!("AFTER_REPORT_VACUUM");
        Ok(Response::new(ReportVacuumTaskResponse { status: None }))
    }

    async fn get_compaction_groups(
        &self,
        _request: Request<GetCompactionGroupsRequest>,
    ) -> Result<Response<GetCompactionGroupsResponse>, Status> {
        let resp = GetCompactionGroupsResponse {
            status: None,
            compaction_groups: Some(self.compaction_group_manager.compactiongroups().await),
        };
        Ok(Response::new(resp))
    }

    async fn trigger_manual_compaction(
        &self,
        request: Request<TriggerManualCompactionRequest>,
    ) -> Result<Response<TriggerManualCompactionResponse>, Status> {
        let request = request.into_inner();
        let compaction_group_id = request.compaction_group_id;
        let mut option = ManualCompactionOption {
            level: request.level as usize,
            sst_ids: request.sst_ids,
            ..Default::default()
        };

        // rewrite the key_range
        match request.key_range {
            Some(key_range) => {
                option.key_range = key_range;
            }

            None => {
                option.key_range = KeyRange {
                    inf: true,
                    ..Default::default()
                }
            }
        }

        // get internal_table_id by fragment_manager
        if request.table_id >= NON_RESERVED_PG_CATALOG_TABLE_ID as u32 {
            // We need to make sure to use the correct table_id to filter sst
            let table_id = TableId::new(request.table_id);
            if let Ok(table_fragment) = self
                .fragment_manager
                .select_table_fragments_by_table_id(&table_id)
                .await
            {
                option.internal_table_id = HashSet::from_iter(table_fragment.all_table_ids());
            }
        }

        assert!(option
            .internal_table_id
            .iter()
            .all(|table_id| *table_id >= (NON_RESERVED_PG_CATALOG_TABLE_ID as u32)),);

        tracing::info!(
            "Try trigger_manual_compaction compaction_group_id {} option {:?}",
            compaction_group_id,
            &option
        );

        self.hummock_manager
            .trigger_manual_compaction(compaction_group_id, option)
            .await?;

        Ok(Response::new(TriggerManualCompactionResponse {
            status: None,
        }))
    }

    async fn get_epoch(
        &self,
        _request: Request<GetEpochRequest>,
    ) -> Result<Response<GetEpochResponse>, Status> {
        let hummock_snapshot = self.hummock_manager.get_last_epoch()?;
        Ok(Response::new(GetEpochResponse {
            status: None,
            snapshot: Some(hummock_snapshot),
        }))
    }

    async fn report_full_scan_task(
        &self,
        request: Request<ReportFullScanTaskRequest>,
    ) -> Result<Response<ReportFullScanTaskResponse>, Status> {
        let vacuum_manager = self.vacuum_manager.clone();
        // The following operation takes some time, so we do it in dedicated task and responds the
        // RPC immediately.
        tokio::spawn(async move {
            match vacuum_manager
                .complete_full_gc(request.into_inner().sst_ids)
                .await
            {
                Ok(number) => {
                    tracing::info!("Full GC results {} SSTs to delete", number);
                }
                Err(e) => {
                    tracing::warn!("Full GC SST failed: {:#?}", e);
                }
            }
        });
        Ok(Response::new(ReportFullScanTaskResponse { status: None }))
    }

    async fn trigger_full_gc(
        &self,
        request: Request<TriggerFullGcRequest>,
    ) -> Result<Response<TriggerFullGcResponse>, Status> {
        self.vacuum_manager
            .start_full_gc(Duration::from_secs(
                request.into_inner().sst_retention_time_sec,
            ))
            .await
            .map_err(MetaError::from)?;
        Ok(Response::new(TriggerFullGcResponse { status: None }))
    }

    async fn rise_ctl_get_pinned_versions_summary(
        &self,
        _request: Request<RiseCtlGetPinnedVersionsSummaryRequest>,
    ) -> Result<Response<RiseCtlGetPinnedVersionsSummaryResponse>, Status> {
        let pinned_versions = self.hummock_manager.list_pinned_version().await;
        let workers = self
            .hummock_manager
            .list_workers(&pinned_versions.iter().map(|v| v.context_id).collect_vec())
            .await;
        Ok(Response::new(RiseCtlGetPinnedVersionsSummaryResponse {
            summary: Some(PinnedVersionsSummary {
                pinned_versions,
                workers,
            }),
        }))
    }

    async fn rise_ctl_get_pinned_snapshots_summary(
        &self,
        _request: Request<RiseCtlGetPinnedSnapshotsSummaryRequest>,
    ) -> Result<Response<RiseCtlGetPinnedSnapshotsSummaryResponse>, Status> {
        let pinned_snapshots = self.hummock_manager.list_pinned_snapshot().await;
        let workers = self
            .hummock_manager
            .list_workers(&pinned_snapshots.iter().map(|p| p.context_id).collect_vec())
            .await;
        Ok(Response::new(RiseCtlGetPinnedSnapshotsSummaryResponse {
            summary: Some(PinnedSnapshotsSummary {
                pinned_snapshots,
                workers,
            }),
        }))
    }
}
