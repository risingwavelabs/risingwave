// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::catalog::{TableId, NON_RESERVED_SYS_CATALOG_ID};
use risingwave_pb::hummock::get_compaction_score_response::PickerInfo;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::subscribe_compaction_event_request::Event as RequestEvent;
use risingwave_pb::hummock::version_update_payload::Payload;
use risingwave_pb::hummock::*;
use tonic::{Request, Response, Status, Streaming};

use crate::hummock::compaction::selector::ManualCompactionOption;
use crate::hummock::{HummockManagerRef, VacuumManagerRef};
use crate::manager::FragmentManagerRef;
use crate::RwReceiverStream;
pub struct HummockServiceImpl {
    hummock_manager: HummockManagerRef,
    vacuum_manager: VacuumManagerRef,
    fragment_manager: FragmentManagerRef,
}

impl HummockServiceImpl {
    pub fn new(
        hummock_manager: HummockManagerRef,
        vacuum_trigger: VacuumManagerRef,
        fragment_manager: FragmentManagerRef,
    ) -> Self {
        HummockServiceImpl {
            hummock_manager,
            vacuum_manager: vacuum_trigger,
            fragment_manager,
        }
    }
}

macro_rules! fields_to_kvs {
    ($struct:ident, $($field:ident),*) => {
        {
            let mut kvs = HashMap::default();
            $(
                kvs.insert(stringify!($field).to_string(), $struct.$field.to_string());
            )*
            kvs
        }
    }
}

#[async_trait::async_trait]
impl HummockManagerService for HummockServiceImpl {
    type SubscribeCompactionEventStream = RwReceiverStream<SubscribeCompactionEventResponse>;

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

    async fn replay_version_delta(
        &self,
        request: Request<ReplayVersionDeltaRequest>,
    ) -> Result<Response<ReplayVersionDeltaResponse>, Status> {
        let req = request.into_inner();
        let (version, compaction_groups) = self
            .hummock_manager
            .replay_version_delta(req.version_delta.unwrap())
            .await?;
        Ok(Response::new(ReplayVersionDeltaResponse {
            version: Some(version),
            modified_compaction_groups: compaction_groups,
        }))
    }

    async fn trigger_compaction_deterministic(
        &self,
        request: Request<TriggerCompactionDeterministicRequest>,
    ) -> Result<Response<TriggerCompactionDeterministicResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .trigger_compaction_deterministic(req.version_id, req.compaction_groups)
            .await?;
        Ok(Response::new(TriggerCompactionDeterministicResponse {}))
    }

    async fn disable_commit_epoch(
        &self,
        _request: Request<DisableCommitEpochRequest>,
    ) -> Result<Response<DisableCommitEpochResponse>, Status> {
        let version = self.hummock_manager.disable_commit_epoch().await;
        Ok(Response::new(DisableCommitEpochResponse {
            current_version: Some(version),
        }))
    }

    async fn list_version_deltas(
        &self,
        request: Request<ListVersionDeltasRequest>,
    ) -> Result<Response<ListVersionDeltasResponse>, Status> {
        let req = request.into_inner();
        let version_deltas = self
            .hummock_manager
            .list_version_deltas(req.start_id, req.num_limit, req.committed_epoch_limit)
            .await?;
        let resp = ListVersionDeltasResponse {
            version_deltas: Some(version_deltas),
        };
        Ok(Response::new(resp))
    }

    async fn pin_specific_snapshot(
        &self,
        request: Request<PinSpecificSnapshotRequest>,
    ) -> Result<Response<PinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let hummock_snapshot = self
            .hummock_manager
            .pin_specific_snapshot(req.context_id, req.epoch)
            .await?;
        Ok(Response::new(PinSnapshotResponse {
            status: None,
            snapshot: Some(hummock_snapshot),
        }))
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
                option.key_range = KeyRange::default();
            }
        }

        // get internal_table_id by fragment_manager
        if request.table_id >= NON_RESERVED_SYS_CATALOG_ID as u32 {
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
            .all(|table_id| *table_id >= (NON_RESERVED_SYS_CATALOG_ID as u32)),);

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
        let hummock_snapshot = self.hummock_manager.latest_snapshot();
        Ok(Response::new(GetEpochResponse {
            status: None,
            snapshot: Some(hummock_snapshot),
        }))
    }

    async fn report_full_scan_task(
        &self,
        request: Request<ReportFullScanTaskRequest>,
    ) -> Result<Response<ReportFullScanTaskResponse>, Status> {
        let req = request.into_inner();
        let hummock_manager = self.hummock_manager.clone();
        hummock_manager
            .metrics
            .total_object_count
            .set(req.total_object_count as _);
        hummock_manager
            .metrics
            .total_object_size
            .set(req.total_object_size as _);
        // The following operation takes some time, so we do it in dedicated task and responds the
        // RPC immediately.
        tokio::spawn(async move {
            match hummock_manager.complete_full_gc(req.object_ids).await {
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
        self.hummock_manager.start_full_gc(Duration::from_secs(
            request.into_inner().sst_retention_time_sec,
        ))?;
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

    async fn get_assigned_compact_task_num(
        &self,
        _request: Request<GetAssignedCompactTaskNumRequest>,
    ) -> Result<Response<GetAssignedCompactTaskNumResponse>, Status> {
        let num_tasks = self.hummock_manager.get_assigned_compact_task_num().await;
        Ok(Response::new(GetAssignedCompactTaskNumResponse {
            num_tasks: num_tasks as u32,
        }))
    }

    async fn rise_ctl_list_compaction_group(
        &self,
        _request: Request<RiseCtlListCompactionGroupRequest>,
    ) -> Result<Response<RiseCtlListCompactionGroupResponse>, Status> {
        let compaction_groups = self.hummock_manager.list_compaction_group().await;
        Ok(Response::new(RiseCtlListCompactionGroupResponse {
            status: None,
            compaction_groups,
        }))
    }

    async fn rise_ctl_update_compaction_config(
        &self,
        request: Request<RiseCtlUpdateCompactionConfigRequest>,
    ) -> Result<Response<RiseCtlUpdateCompactionConfigResponse>, Status> {
        let RiseCtlUpdateCompactionConfigRequest {
            compaction_group_ids,
            configs,
        } = request.into_inner();
        self.hummock_manager
            .update_compaction_config(
                compaction_group_ids.as_slice(),
                configs
                    .into_iter()
                    .map(|c| c.mutable_config.unwrap())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .await?;
        Ok(Response::new(RiseCtlUpdateCompactionConfigResponse {
            status: None,
        }))
    }

    async fn init_metadata_for_replay(
        &self,
        request: Request<InitMetadataForReplayRequest>,
    ) -> Result<Response<InitMetadataForReplayResponse>, Status> {
        let InitMetadataForReplayRequest {
            tables,
            compaction_groups,
        } = request.into_inner();

        self.hummock_manager
            .init_metadata_for_version_replay(tables, compaction_groups)
            .await?;
        Ok(Response::new(InitMetadataForReplayResponse {}))
    }

    async fn pin_version(
        &self,
        request: Request<PinVersionRequest>,
    ) -> Result<Response<PinVersionResponse>, Status> {
        let req = request.into_inner();
        let payload = self.hummock_manager.pin_version(req.context_id).await?;
        match payload {
            Payload::PinnedVersion(version) => Ok(Response::new(PinVersionResponse {
                pinned_version: Some(version),
            })),
            Payload::VersionDeltas(_) => {
                unreachable!("pin_version should not return version delta")
            }
        }
    }

    async fn split_compaction_group(
        &self,
        request: Request<SplitCompactionGroupRequest>,
    ) -> Result<Response<SplitCompactionGroupResponse>, Status> {
        let req = request.into_inner();
        let new_group_id = self
            .hummock_manager
            .split_compaction_group(req.group_id, &req.table_ids)
            .await?;
        Ok(Response::new(SplitCompactionGroupResponse { new_group_id }))
    }

    async fn rise_ctl_pause_version_checkpoint(
        &self,
        _request: Request<RiseCtlPauseVersionCheckpointRequest>,
    ) -> Result<Response<RiseCtlPauseVersionCheckpointResponse>, Status> {
        self.hummock_manager.pause_version_checkpoint();
        Ok(Response::new(RiseCtlPauseVersionCheckpointResponse {}))
    }

    async fn rise_ctl_resume_version_checkpoint(
        &self,
        _request: Request<RiseCtlResumeVersionCheckpointRequest>,
    ) -> Result<Response<RiseCtlResumeVersionCheckpointResponse>, Status> {
        self.hummock_manager.resume_version_checkpoint();
        Ok(Response::new(RiseCtlResumeVersionCheckpointResponse {}))
    }

    async fn rise_ctl_get_checkpoint_version(
        &self,
        _request: Request<RiseCtlGetCheckpointVersionRequest>,
    ) -> Result<Response<RiseCtlGetCheckpointVersionResponse>, Status> {
        let checkpoint_version = self.hummock_manager.get_checkpoint_version().await;
        Ok(Response::new(RiseCtlGetCheckpointVersionResponse {
            checkpoint_version: Some(checkpoint_version),
        }))
    }

    async fn rise_ctl_list_compaction_status(
        &self,
        _request: Request<RiseCtlListCompactionStatusRequest>,
    ) -> Result<Response<RiseCtlListCompactionStatusResponse>, Status> {
        let (compaction_statuses, task_assignment) =
            self.hummock_manager.list_compaction_status().await;
        let task_progress = self.hummock_manager.compactor_manager.get_progress();
        Ok(Response::new(RiseCtlListCompactionStatusResponse {
            compaction_statuses,
            task_assignment,
            task_progress,
        }))
    }

    async fn subscribe_compaction_event(
        &self,
        request: Request<Streaming<SubscribeCompactionEventRequest>>,
    ) -> Result<Response<Self::SubscribeCompactionEventStream>, tonic::Status> {
        let mut request_stream: Streaming<SubscribeCompactionEventRequest> = request.into_inner();
        let register_req = {
            let req = request_stream.next().await.ok_or_else(|| {
                Status::invalid_argument("subscribe_compaction_event request is empty")
            })??;

            match req.event {
                Some(RequestEvent::Register(register)) => register,
                _ => {
                    return Err(Status::invalid_argument(
                        "the first message must be `Register`",
                    ))
                }
            }
        };

        let context_id = register_req.context_id;

        // check_context and add_compactor as a whole is not atomic, but compactor_manager will
        // remove invalid compactor eventually.
        if !self.hummock_manager.check_context(context_id).await {
            return Err(Status::new(
                tonic::Code::Internal,
                format!("invalid hummock context {}", context_id),
            ));
        }
        let compactor_manager = self.hummock_manager.compactor_manager.clone();

        let rx: tokio::sync::mpsc::UnboundedReceiver<
            Result<SubscribeCompactionEventResponse, crate::MetaError>,
        > = compactor_manager.add_compactor(context_id);

        // register request stream to hummock
        self.hummock_manager
            .add_compactor_stream(context_id, request_stream);

        // Trigger compaction on all compaction groups.
        for cg_id in self.hummock_manager.compaction_group_ids().await {
            self.hummock_manager
                .try_send_compaction_request(cg_id, compact_task::TaskType::Dynamic);
        }

        Ok(Response::new(RwReceiverStream::new(rx)))
    }

    async fn report_compaction_task(
        &self,
        _request: Request<ReportCompactionTaskRequest>,
    ) -> Result<Response<ReportCompactionTaskResponse>, Status> {
        unreachable!()
    }

    async fn list_branched_object(
        &self,
        _request: Request<ListBranchedObjectRequest>,
    ) -> Result<Response<ListBranchedObjectResponse>, Status> {
        let branched_objects = self
            .hummock_manager
            .list_branched_objects()
            .await
            .into_iter()
            .flat_map(|(object_id, v)| {
                v.into_iter()
                    .map(move |(compaction_group_id, sst_id)| BranchedObject {
                        object_id,
                        sst_id,
                        compaction_group_id,
                    })
            })
            .collect();
        Ok(Response::new(ListBranchedObjectResponse {
            branched_objects,
        }))
    }

    async fn list_active_write_limit(
        &self,
        _request: Request<ListActiveWriteLimitRequest>,
    ) -> Result<Response<ListActiveWriteLimitResponse>, Status> {
        Ok(Response::new(ListActiveWriteLimitResponse {
            write_limits: self.hummock_manager.write_limits().await,
        }))
    }

    async fn list_hummock_meta_config(
        &self,
        _request: Request<ListHummockMetaConfigRequest>,
    ) -> Result<Response<ListHummockMetaConfigResponse>, Status> {
        let opt = &self.hummock_manager.env.opts;
        let configs = fields_to_kvs!(
            opt,
            vacuum_interval_sec,
            vacuum_spin_interval_ms,
            hummock_version_checkpoint_interval_sec,
            min_delta_log_num_for_hummock_version_checkpoint,
            min_sst_retention_time_sec,
            full_gc_interval_sec,
            collect_gc_watermark_spin_interval_sec,
            periodic_compaction_interval_sec,
            periodic_space_reclaim_compaction_interval_sec,
            periodic_ttl_reclaim_compaction_interval_sec,
            periodic_tombstone_reclaim_compaction_interval_sec,
            periodic_split_compact_group_interval_sec,
            split_group_size_limit,
            min_table_split_size,
            do_not_config_object_storage_lifecycle,
            partition_vnode_count,
            table_write_throughput_threshold,
            min_table_split_write_throughput,
            compaction_task_max_heartbeat_interval_secs
        );
        Ok(Response::new(ListHummockMetaConfigResponse { configs }))
    }

    async fn rise_ctl_rebuild_table_stats(
        &self,
        _request: Request<RiseCtlRebuildTableStatsRequest>,
    ) -> Result<Response<RiseCtlRebuildTableStatsResponse>, Status> {
        self.hummock_manager.rebuild_table_stats().await?;
        Ok(Response::new(RiseCtlRebuildTableStatsResponse {}))
    }

    async fn get_compaction_score(
        &self,
        request: Request<GetCompactionScoreRequest>,
    ) -> Result<Response<GetCompactionScoreResponse>, Status> {
        let compaction_group_id = request.into_inner().compaction_group_id;
        let scores = self
            .hummock_manager
            .get_compaction_scores(compaction_group_id)
            .await
            .into_iter()
            .map(|s| PickerInfo {
                score: s.score,
                select_level: s.select_level as _,
                target_level: s.target_level as _,
                picker_type: s.picker_type.to_string(),
            })
            .collect();
        Ok(Response::new(GetCompactionScoreResponse {
            compaction_group_id,
            scores,
        }))
    }

    async fn list_compact_task_assignment(
        &self,
        _request: Request<ListCompactTaskAssignmentRequest>,
    ) -> Result<Response<ListCompactTaskAssignmentResponse>, Status> {
        let (_compaction_statuses, task_assignment) =
            self.hummock_manager.list_compaction_status().await;
        Ok(Response::new(ListCompactTaskAssignmentResponse {
            task_assignment,
        }))
    }

    async fn list_compact_task_progress(
        &self,
        _request: Request<ListCompactTaskProgressRequest>,
    ) -> Result<Response<ListCompactTaskProgressResponse>, Status> {
        let task_progress = self.hummock_manager.compactor_manager.get_progress();

        Ok(Response::new(ListCompactTaskProgressResponse {
            task_progress,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[test]
    fn test_fields_to_kvs() {
        struct S {
            foo: u64,
            bar: String,
        }
        let s = S {
            foo: 15,
            bar: "foobar".to_string(),
        };
        let kvs: HashMap<String, String> = fields_to_kvs!(s, foo, bar);
        assert_eq!(kvs.len(), 2);
        assert_eq!(kvs.get("foo").unwrap(), "15");
        assert_eq!(kvs.get("bar").unwrap(), "foobar");
    }
}
