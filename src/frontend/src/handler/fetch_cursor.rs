// Copyright 2025 RisingWave Labs
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
use pgwire::types::{Format, Row};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{FetchCursorStatement, Statement};

use super::RwPgResponse;
use super::extended_handle::{PortalResult, PrepareStatement, PreparedResult};
use super::query::BoundResult;
use super::util::convert_interval_to_u64_seconds;
use crate::binder::BoundStatement;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::{Binder, PgResponseStream, WithOptions};

pub async fn handle_fetch_cursor_execute(
    handler_args: HandlerArgs,
    portal_result: PortalResult,
) -> Result<RwPgResponse> {
    if let PortalResult {
        statement: Statement::FetchCursor { stmt },
        bound_result:
            BoundResult {
                bound: BoundStatement::FetchCursor(fetch_cursor),
                ..
            },
        result_formats,
        ..
    } = portal_result
    {
        match fetch_cursor.returning_schema {
            Some(_) => handle_fetch_cursor(handler_args, stmt, &result_formats).await,
            None => Ok(build_fetch_cursor_response(vec![], vec![])),
        }
    } else {
        bail_not_implemented!("unsupported portal {}", portal_result)
    }
}
pub async fn handle_fetch_cursor(
    handler_args: HandlerArgs,
    stmt: FetchCursorStatement,
    formats: &Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let cursor_name = stmt.cursor_name.real_value();
    let with_options = WithOptions::try_from(stmt.with_properties.0.as_slice())?;

    if with_options.len() > 1 {
        bail_not_implemented!("only `timeout` is supported in with options")
    }

    let timeout_seconds = with_options
        .get("timeout")
        .map(convert_interval_to_u64_seconds)
        .transpose()?;

    if with_options.len() == 1 && timeout_seconds.is_none() {
        bail_not_implemented!("only `timeout` is supported in with options")
    }

    let cursor_manager = session.get_cursor_manager();

    let (rows, pg_descs) = cursor_manager
        .get_rows_with_cursor(
            &cursor_name,
            stmt.count,
            handler_args,
            formats,
            timeout_seconds,
        )
        .await?;
    Ok(build_fetch_cursor_response(rows, pg_descs))
}

fn build_fetch_cursor_response(rows: Vec<Row>, pg_descs: Vec<PgFieldDescriptor>) -> RwPgResponse {
    PgResponse::builder(StatementType::FETCH_CURSOR)
        .row_cnt_opt(Some(rows.len() as i32))
        .values(PgResponseStream::from(rows), pg_descs)
        .into()
}

pub async fn handle_parse(
    handler_args: HandlerArgs,
    statement: Statement,
    specific_param_types: Vec<Option<DataType>>,
) -> Result<PrepareStatement> {
    if let Statement::FetchCursor { stmt } = &statement {
        let session = handler_args.session.clone();
        let cursor_name = stmt.cursor_name.real_value();
        let fields = session
            .get_cursor_manager()
            .get_fields_with_cursor(&cursor_name)
            .await?;

        let mut binder = Binder::new_with_param_types(&session, specific_param_types);
        let schema = Some(Schema::new(fields));

        let bound = binder.bind_fetch_cursor(cursor_name, stmt.count, schema)?;
        let bound_result = BoundResult {
            stmt_type: StatementType::FETCH_CURSOR,
            must_dist: false,
            bound: BoundStatement::FetchCursor(Box::new(bound)),
            param_types: binder.export_param_types()?,
            parsed_params: None,
            dependent_relations: binder.included_relations().clone(),
            dependent_udfs: binder.included_udfs().clone(),
        };
        let result = PreparedResult {
            statement,
            bound_result,
        };
        Ok(PrepareStatement::Prepared(result))
    } else {
        bail_not_implemented!("unsupported statement {:?}", statement)
    }
}
