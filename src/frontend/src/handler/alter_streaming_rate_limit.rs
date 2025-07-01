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

use futures::StreamExt;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_pb::meta::ThrottleTarget as PbThrottleTarget;
use risingwave_sqlparser::ast::{BinaryOperator, Expr, Ident, ObjectName, Statement, Value};

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::system_catalog::rw_catalog::rw_sinks::{IS_SINK_DECOUPLE_INDEX, SINK_ID};
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::handler::query::handle_query;
use crate::handler::util::gen_query_from_table_name_with_selection;
use crate::session::SessionImpl;

pub async fn handle_alter_streaming_rate_limit(
    handler_args: HandlerArgs,
    kind: PbThrottleTarget,
    table_name: ObjectName,
    rate_limit: i32,
) -> Result<RwPgResponse> {
    let session = handler_args.clone().session;
    let db_name = &session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (stmt_type, id) = match kind {
        PbThrottleTarget::Mv => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_any_table_by_name(db_name, schema_path, &real_table_name)?;
            if table.table_type != TableType::MaterializedView {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a materialized view",
                ))
                .into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_MATERIALIZED_VIEW, table.id.table_id)
        }
        PbThrottleTarget::Source => {
            let reader = session.env().catalog_reader().read_guard();
            let (source, schema_name) =
                reader.get_source_by_name(db_name, schema_path, &real_table_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**source)?;
            (StatementType::ALTER_SOURCE, source.id)
        }
        PbThrottleTarget::TableWithSource => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            // Get the corresponding source catalog.
            let source_id = if let Some(id) = table.associated_source_id {
                id.table_id()
            } else {
                bail!("ALTER SOURCE_RATE_LIMIT is not for table without source")
            };
            (StatementType::ALTER_SOURCE, source_id)
        }
        PbThrottleTarget::CdcTable => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_any_table_by_name(db_name, schema_path, &real_table_name)?;
            if table.table_type != TableType::Table {
                return Err(ErrorCode::InvalidInputSyntax(format!("\"{table_name}\" ",)).into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_TABLE, table.id.table_id)
        }
        PbThrottleTarget::TableDml => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;
            if table.table_type != TableType::Table {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a table",
                ))
                .into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_TABLE, table.id.table_id)
        }
        PbThrottleTarget::Sink => {
            let sink_id = {
                let reader = session.env().catalog_reader().read_guard();
                let (table, schema_name) =
                    reader.get_sink_by_name(db_name, schema_path, &real_table_name)?;

                if table.target_table.is_some() {
                    bail!("ALTER SINK_RATE_LIMIT is not for sink into table")
                }
                session.check_privilege_for_drop_alter(schema_name, &**table)?;
                table.id.sink_id
            };
            let selection = Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(SINK_ID.into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(sink_id.to_string()))),
            });
            let query = gen_query_from_table_name_with_selection(
                ObjectName::from(vec![
                    Ident::from("rw_catalog"),
                    Ident::from("rw_sink_decouple"),
                ]),
                selection,
            );
            let stmt = Statement::Query(Box::new(query));
            let mut res = handle_query(handler_args, stmt, vec![]).await?;
            while let Some(row) = res.values_stream().next().await {
                let row = row?;
                let is_decouple =
                    row[0][IS_SINK_DECOUPLE_INDEX]
                        .as_ref()
                        .ok_or(ErrorCode::InternalError(format!(
                            "Not find sink decouple message with sink_id {}",
                            sink_id
                        )))?;
                if String::from_utf8(is_decouple.clone().into()).unwrap() != "t" {
                    bail!("ALTER SINK_RATE_LIMIT is only for sink with sink_decouple = true")
                }
            }

            (StatementType::ALTER_SINK, sink_id)
        }
        _ => bail!("Unsupported throttle target: {:?}", kind),
    };
    handle_alter_streaming_rate_limit_by_id(&session, kind, id, rate_limit, stmt_type).await
}

pub async fn handle_alter_streaming_rate_limit_by_id(
    session: &SessionImpl,
    kind: PbThrottleTarget,
    id: u32,
    rate_limit: i32,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let meta_client = session.env().meta_client();

    let rate_limit = if rate_limit < 0 {
        None
    } else {
        Some(rate_limit as u32)
    };

    meta_client.apply_throttle(kind, id, rate_limit).await?;

    Ok(PgResponse::empty_result(stmt_type))
}
