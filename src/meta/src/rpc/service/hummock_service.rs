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

use risingwave_common::error::{tonic_err, ErrorCode};
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::*;
use tonic::{Request, Response, Status};

use crate::hummock::{CompactorManager, HummockManagerRef, VacuumTrigger};
use crate::rpc::service::RwReceiverStream;
use crate::storage::MetaStore;

pub struct HummockServiceImpl<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: Arc<CompactorManager>,
    vacuum_trigger: Arc<VacuumTrigger<S>>,
}

impl<S> HummockServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: Arc<CompactorManager>,
        vacuum_trigger: Arc<VacuumTrigger<S>>,
    ) -> Self {
        HummockServiceImpl {
            hummock_manager,
            compactor_manager,
            vacuum_trigger,
        }
    }
}

#[async_trait::async_trait]
impl<S> HummockManagerService for HummockServiceImpl<S>
where
    S: MetaStore,
{
    type SubscribeCompactTasksStream = RwReceiverStream<SubscribeCompactTasksResponse>;

    async fn pin_version(
        &self,
        request: Request<PinVersionRequest>,
    ) -> Result<Response<PinVersionResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .pin_version(req.context_id, req.last_pinned)
            .await;
        match result {
            Ok(pinned_version) => Ok(Response::new(PinVersionResponse {
                status: None,
                pinned_version: Some(pinned_version),
            })),
            Err(e) => Err(tonic_err(e)),
        }
    }

    async fn unpin_version(
        &self,
        request: Request<UnpinVersionRequest>,
    ) -> Result<Response<UnpinVersionResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .unpin_version(req.context_id, req.pinned_version_ids)
            .await;
        match result {
            Ok(_) => Ok(Response::new(UnpinVersionResponse { status: None })),
            Err(e) => Err(tonic_err(e)),
        }
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
                let result = self
                    .hummock_manager
                    .report_compact_task(&compact_task)
                    .await;
                match result {
                    Ok(_) => Ok(Response::new(ReportCompactionTasksResponse {
                        status: None,
                    })),
                    Err(e) => Err(tonic_err(e)),
                }
            }
        }
    }

    async fn pin_snapshot(
        &self,
        request: Request<PinSnapshotRequest>,
    ) -> Result<Response<PinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .pin_snapshot(req.context_id, req.last_pinned)
            .await;
        match result {
            Ok(hummock_snapshot) => Ok(Response::new(PinSnapshotResponse {
                status: None,
                snapshot: Some(hummock_snapshot),
            })),
            Err(e) => Err(tonic_err(e)),
        }
    }

    async fn unpin_snapshot(
        &self,
        request: Request<UnpinSnapshotRequest>,
    ) -> Result<Response<UnpinSnapshotResponse>, Status> {
        let req = request.into_inner();
        if let Err(e) = self
            .hummock_manager
            .unpin_snapshot(req.context_id, req.snapshots)
            .await
        {
            return Err(tonic_err(e));
        }
        Ok(Response::new(UnpinSnapshotResponse { status: None }))
    }

    async fn unpin_snapshot_before(
        &self,
        request: Request<UnpinSnapshotRequest>,
    ) -> Result<Response<UnpinSnapshotResponse>, Status> {
        let req = request.into_inner();
        if let Err(e) = self
            .hummock_manager
            .unpin_snapshot_before(req.context_id, req.min_snapshot.unwrap())
            .await
        {
            return Err(tonic_err(e));
        }
        Ok(Response::new(UnpinSnapshotResponse { status: None }))
    }

    async fn get_new_table_id(
        &self,
        _request: Request<GetNewTableIdRequest>,
    ) -> Result<Response<GetNewTableIdResponse>, Status> {
        let result = self.hummock_manager.get_new_table_id().await;
        match result {
            Ok(table_id) => Ok(Response::new(GetNewTableIdResponse {
                status: None,
                table_id,
            })),
            Err(e) => Err(tonic_err(e)),
        }
    }

    async fn subscribe_compact_tasks(
        &self,
        request: Request<SubscribeCompactTasksRequest>,
    ) -> Result<Response<Self::SubscribeCompactTasksStream>, Status> {
        let context_id = request.into_inner().context_id;
        // check_context and add_compactor as a whole is not atomic, but compactor_manager will
        // remove invalid compactor eventually.
        if !self.hummock_manager.check_context(context_id).await {
            return Err(tonic_err(ErrorCode::MetaError(format!(
                "invalid hummock context {}",
                context_id
            ))));
        }
        let rx = self.compactor_manager.add_compactor(context_id);
        Ok(Response::new(RwReceiverStream::new(rx)))
    }

    async fn report_vacuum_task(
        &self,
        request: Request<ReportVacuumTaskRequest>,
    ) -> Result<Response<ReportVacuumTaskResponse>, Status> {
        if let Some(vacuum_task) = request.into_inner().vacuum_task {
            self.vacuum_trigger
                .report_vacuum_task(vacuum_task)
                .await
                .map_err(tonic_err)?;
        }
        Ok(Response::new(ReportVacuumTaskResponse { status: None }))
    }
}
