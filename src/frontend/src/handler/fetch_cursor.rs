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

use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_sqlparser::ast::FetchCursorStatement;

use super::RwPgResponse;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::{Binder, PgResponseStream};

pub async fn handle_fetch_cursor(
    handle_args: HandlerArgs,
    stmt: FetchCursorStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let db_name = session.database();
    let (_, cursor_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.cursor_name.clone())?;

    let cursor_manager = session.get_cursor_manager();

    let (rows, pg_descs) = cursor_manager
        .get_rows_with_cursor(cursor_name, stmt.count, handle_args)
        .await?;
    Ok(build_fetch_cursor_response(rows, pg_descs))
}

fn build_fetch_cursor_response(rows: Vec<Row>, pg_descs: Vec<PgFieldDescriptor>) -> RwPgResponse {
    PgResponse::builder(StatementType::FETCH_CURSOR)
        .row_cnt_opt(Some(rows.len() as i32))
        .values(PgResponseStream::from(rows), pg_descs)
        .into()
}
