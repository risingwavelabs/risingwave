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

use risingwave_rpc_client::audit::{self, AuditContext};

use crate::session::SessionImpl;

pub async fn with_audit_context<T>(session: &SessionImpl, fut: impl Future<Output = T>) -> T {
    let database_id = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_database_by_name(&session.database())
        .map(|db| db.id().as_raw_id())
        .ok();
    let ctx = AuditContext::new(session.user_name(), database_id);
    audit::with_audit_context(ctx, fut).await
}
