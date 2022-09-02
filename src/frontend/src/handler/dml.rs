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

pub async fn handle_dml(context: OptimizerContext, stmt: Statement) -> Result<PgResponse> {
    let stmt_type = to_statement_type(&stmt);
    let session = context.session_ctx.clone();

    let bound = {
        let mut binder = Binder::new(&session);
        binder.bind(stmt)?
    };

    let check_items = resolve_privileges(&bound);
    check_privileges(&session, &check_items)?;

    // TODO: support dml in local mode
    // Currently dml always runs in distributed mode.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (data_stream, pg_descs) =
        crate::handler::query::distribute_execute(context, bound, shutdown_tx).await?;

    let mut data_stream = data_stream.take_until(shutdown_rx);
    let mut rows = vec![];
    #[for_await]
    for chunk in &mut data_stream {
        rows.extend(to_pg_rows(chunk?, false));
    }

    // Check whether error happen, if yes, returned.
    let execution_ret = data_stream.take_result();
    if let Some(ret) = execution_ret {
        return Err(ret
            .expect("The shutdown message receiver should not fail")
            .into());
    }

    let rows_count = match stmt_type {
        // TODO(renjie): We need a better solution for this.
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let first_row = rows[0].values();
            let affected_rows_str = first_row[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            String::from_utf8(affected_rows_str.to_vec())
                .unwrap()
                .parse()
                .unwrap_or_default()
        }

        _ => unreachable!(),
    };

    // Implicitly flush the writes.
    if session.config().get_implicit_flush() {
        flush_for_write(&session, stmt_type).await?;
    }

    Ok(PgResponse::new(stmt_type, rows_count, rows, pg_descs, true))
}

async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
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

fn to_statement_type(stmt: &Statement) -> StatementType {
    use StatementType::*;

    match stmt {
        Statement::Insert { .. } => INSERT,
        Statement::Delete { .. } => DELETE,
        Statement::Update { .. } => UPDATE,
        _ => unreachable!(),
    }
}
