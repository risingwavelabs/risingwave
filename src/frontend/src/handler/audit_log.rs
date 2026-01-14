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

use risingwave_pb::meta::AddAuditLogRequest;
use serde_json::Value;

use crate::session::SessionImpl;

pub async fn record_audit_log(
    session: &SessionImpl,
    action: &str,
    object_type: Option<&str>,
    object_id: Option<u32>,
    object_name: Option<String>,
    details: Value,
) {
    let database_id = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_database_by_name(&session.database())
        .map(|db| db.id() as u32)
        .ok();

    let details_json = match details {
        Value::Null => None,
        other => match serde_json::to_string(&other) {
            Ok(payload) => Some(payload),
            Err(err) => {
                tracing::warn!(error = %err, "failed to serialize audit log details");
                None
            }
        },
    };

    let req = AddAuditLogRequest {
        user_name: session.user_name().to_owned(),
        action: action.to_owned(),
        object_type: object_type.map(|value| value.to_owned()),
        object_id,
        object_name,
        database_id,
        details_json,
    };

    if let Err(err) = session.env().meta_client().add_audit_log(req).await {
        tracing::warn!(error = %err, "failed to write audit log");
    }
}
