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

use std::ops::Deref;

use risingwave_pb::backup_service::backup_service_server::BackupService;
use risingwave_pb::backup_service::{
    BackupMetaRequest, BackupMetaResponse, DeleteMetaSnapshotRequest, DeleteMetaSnapshotResponse,
    GetBackupJobStatusRequest, GetBackupJobStatusResponse, GetMetaSnapshotManifestRequest,
    GetMetaSnapshotManifestResponse,
};
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;

pub struct BackupServiceImpl {
    backup_manager: BackupManagerRef,
}

impl BackupServiceImpl {
    pub fn new(backup_manager: BackupManagerRef) -> Self {
        Self { backup_manager }
    }
}

#[async_trait::async_trait]
impl BackupService for BackupServiceImpl {
    async fn backup_meta(
        &self,
        request: Request<BackupMetaRequest>,
    ) -> Result<Response<BackupMetaResponse>, Status> {
        let remarks = request.into_inner().remarks;
        let job_id = self.backup_manager.start_backup_job(remarks).await?;
        Ok(Response::new(BackupMetaResponse { job_id }))
    }

    async fn get_backup_job_status(
        &self,
        request: Request<GetBackupJobStatusRequest>,
    ) -> Result<Response<GetBackupJobStatusResponse>, Status> {
        let job_id = request.into_inner().job_id;
        let (job_status, message) = self.backup_manager.get_backup_job_status(job_id);
        Ok(Response::new(GetBackupJobStatusResponse {
            job_id,
            job_status: job_status as _,
            message,
        }))
    }

    async fn delete_meta_snapshot(
        &self,
        request: Request<DeleteMetaSnapshotRequest>,
    ) -> Result<Response<DeleteMetaSnapshotResponse>, Status> {
        let snapshot_ids = request.into_inner().snapshot_ids;
        self.backup_manager.delete_backups(&snapshot_ids).await?;
        Ok(Response::new(DeleteMetaSnapshotResponse {}))
    }

    async fn get_meta_snapshot_manifest(
        &self,
        _request: Request<GetMetaSnapshotManifestRequest>,
    ) -> Result<Response<GetMetaSnapshotManifestResponse>, Status> {
        Ok(Response::new(GetMetaSnapshotManifestResponse {
            manifest: Some(self.backup_manager.manifest().deref().into()),
        }))
    }
}
