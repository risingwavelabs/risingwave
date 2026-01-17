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

use std::future::Future;

use tonic::metadata::MetadataMap;

pub const AUDIT_USER_HEADER: &str = "rw-audit-user";
pub const AUDIT_DATABASE_HEADER: &str = "rw-audit-database-id";

#[derive(Clone, Debug)]
pub struct AuditContext {
    pub user_name: String,
    pub database_id: Option<u32>,
}

impl AuditContext {
    pub fn new(user_name: String, database_id: Option<u32>) -> Self {
        Self {
            user_name,
            database_id,
        }
    }
}

tokio::task_local! {
    static AUDIT_CONTEXT: AuditContext;
}

pub async fn with_audit_context<T>(ctx: AuditContext, fut: impl Future<Output = T>) -> T {
    AUDIT_CONTEXT.scope(ctx, fut).await
}

pub fn current_audit_context() -> Option<AuditContext> {
    AUDIT_CONTEXT.try_with(|ctx| ctx.clone()).ok()
}

pub fn inject_audit_metadata(metadata: &mut MetadataMap) {
    let Some(ctx) = current_audit_context() else {
        return;
    };
    if metadata.get(AUDIT_USER_HEADER).is_none()
        && let Ok(value) = ctx.user_name.parse()
    {
        metadata.insert(AUDIT_USER_HEADER, value);
    }
    if let Some(database_id) = ctx.database_id
        && metadata.get(AUDIT_DATABASE_HEADER).is_none()
        && let Ok(value) = database_id.to_string().parse()
    {
        metadata.insert(AUDIT_DATABASE_HEADER, value);
    }
}
