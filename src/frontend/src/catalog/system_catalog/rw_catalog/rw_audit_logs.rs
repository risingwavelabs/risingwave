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

use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use serde_json::Value;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwAuditLog {
    #[primary_key]
    id: i64,
    event_time: Timestamptz,
    user_name: String,
    action: String,
    object_type: Option<String>,
    object_id: Option<i32>,
    object_name: Option<String>,
    database_id: Option<i32>,
    details: Option<JsonbVal>,
}

#[system_catalog(table, "rw_catalog.rw_audit_logs")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwAuditLog>> {
    let logs = reader.meta_client.list_audit_log().await?;
    let rows = logs
        .into_iter()
        .map(|log| {
            let details = match log.details_json {
                Some(payload) if !payload.trim().is_empty() => {
                    let value: Value = serde_json::from_str(&payload)?;
                    Some(JsonbVal::from(value))
                }
                _ => None,
            };
            Ok(RwAuditLog {
                id: log.id as i64,
                event_time: Timestamptz::from_millis(log.event_time as i64).unwrap(),
                user_name: log.user_name,
                action: log.action,
                object_type: log.object_type,
                object_id: log.object_id.map(|v| v as i32),
                object_name: log.object_name,
                database_id: log.database_id.map(|v| v as i32),
                details,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}
