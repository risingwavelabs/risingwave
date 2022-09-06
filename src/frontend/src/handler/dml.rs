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

use futures::StreamExt;
use futures_async_stream::for_await;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;
use tokio::sync::oneshot;

use crate::binder::Binder;
use crate::handler::privilege::{check_privileges, resolve_privileges};
use crate::handler::util::to_pg_rows;
use crate::session::{OptimizerContext, SessionImpl};

pub async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
    match stmt_type {
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let client = session.env().meta_client();
            let snapshot = client.flush().await?;
            session
                .env()
                .hummock_snapshot_manager()
                .update_epoch(snapshot);
        }
        _ => {}
    }
    Ok(())
}
