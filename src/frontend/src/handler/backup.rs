// Copyright 2024 RisingWave Labs
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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::types::Fields;
use risingwave_pb::backup_service::BackupJobStatus;
use tokio::time::Duration;

use super::{RwPgResponse, RwPgResponseBuilderExt};
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_backup(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    // Only permit backup for super users.
    if !handler_args.session.is_super_user() {
        return Err(ErrorCode::PermissionDenied(
            "only superusers can trigger adhoc backup".to_owned(),
        )
        .into());
    }
    let snapshot_id = do_backup(&handler_args.session).await?;
    Ok(PgResponse::builder(StatementType::BACKUP)
        .rows([BackupRow { snapshot_id }])
        .into())
}

pub(crate) async fn do_backup(session: &SessionImpl) -> Result<i64> {
    let client = session.env().meta_client();
    let job_id = client.backup_meta(None).await?;
    loop {
        let (job_status, message) = client.get_backup_job_status(job_id).await?;
        match job_status {
            BackupJobStatus::Running => tokio::time::sleep(Duration::from_millis(100)).await,
            BackupJobStatus::Succeeded => {
                return i64::try_from(job_id).map_err(|_| {
                    ErrorCode::InternalError(format!("snapshot id {} exceeds i64 range", job_id))
                        .into()
                });
            }
            BackupJobStatus::NotFound => {
                return Err(ErrorCode::InternalError(format!(
                    "backup job status not found: job {}, {}",
                    job_id, message
                ))
                .into());
            }
            BackupJobStatus::Failed => {
                return Err(ErrorCode::InternalError(format!(
                    "backup job failed: job {}, {}",
                    job_id, message
                ))
                .into());
            }
            BackupJobStatus::Unspecified => {
                return Err(ErrorCode::InternalError(format!(
                    "backup job status unspecified: job {}, {}",
                    job_id, message
                ))
                .into());
            }
        }
    }
}

#[derive(Fields)]
struct BackupRow {
    snapshot_id: i64,
}
