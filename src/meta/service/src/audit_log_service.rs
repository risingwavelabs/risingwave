// Copyright 2026 RisingWave Labs
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

use std::time::{SystemTime, UNIX_EPOCH};

use risingwave_meta::manager::MetaSrvEnv;
use risingwave_meta_model::{AuditDetails, audit_log};
use risingwave_pb::meta::audit_log_service_server::AuditLogService;
use risingwave_pb::meta::{
    AddAuditLogRequest, AddAuditLogResponse, AuditLog, ListAuditLogRequest, ListAuditLogResponse,
};
use sea_orm::{ActiveModelTrait, EntityTrait, QueryOrder, Set};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct AuditLogServiceImpl {
    env: MetaSrvEnv,
}

impl AuditLogServiceImpl {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self { env }
    }
}

#[async_trait::async_trait]
impl AuditLogService for AuditLogServiceImpl {
    async fn list_audit_log(
        &self,
        _request: Request<ListAuditLogRequest>,
    ) -> Result<Response<ListAuditLogResponse>, Status> {
        let logs = audit_log::Entity::find()
            .order_by_desc(audit_log::Column::EventTime)
            .all(&self.env.meta_store().conn)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to list audit logs: {}", e.as_report()))
            })?;

        let audit_logs = logs
            .into_iter()
            .map(|log| {
                let details_json = log
                    .details
                    .map(|details| serde_json::to_string(&details.into_inner()))
                    .transpose()
                    .map_err(|e| {
                        Status::internal(format!(
                            "Failed to serialize audit details: {}",
                            e.as_report()
                        ))
                    })?;
                Ok(AuditLog {
                    id: log.id as u64,
                    event_time: log.event_time as u64,
                    user_name: log.user_name,
                    action: log.action,
                    object_type: log.object_type,
                    object_id: log.object_id.map(|v| v as u32),
                    object_name: log.object_name,
                    database_id: log.database_id.map(|v| v as u32),
                    details_json,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(ListAuditLogResponse { audit_logs }))
    }

    async fn add_audit_log(
        &self,
        request: Request<AddAuditLogRequest>,
    ) -> Result<Response<AddAuditLogResponse>, Status> {
        let req = request.into_inner();
        let event_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("Time error: {}", e.as_report())))?
            .as_millis() as i64;
        let details = match req.details_json {
            Some(details) if !details.trim().is_empty() => {
                let value = serde_json::from_str::<serde_json::Value>(&details).map_err(|e| {
                    Status::invalid_argument(format!(
                        "Invalid audit details JSON: {}",
                        e.as_report()
                    ))
                })?;
                Some(AuditDetails::from(value))
            }
            _ => None,
        };

        let audit = audit_log::ActiveModel {
            id: Default::default(),
            event_time: Set(event_time),
            user_name: Set(req.user_name),
            action: Set(req.action),
            object_type: Set(req.object_type),
            object_id: Set(req.object_id.map(|v| v as i32)),
            object_name: Set(req.object_name),
            database_id: Set(req.database_id.map(|v| v as i32)),
            details: Set(details),
        };

        audit
            .insert(&self.env.meta_store().conn)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to insert audit log: {}", e.as_report()))
            })?;

        Ok(Response::new(AddAuditLogResponse {}))
    }
}
