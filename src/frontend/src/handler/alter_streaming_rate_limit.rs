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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_pb::meta::ThrottleTarget as PbThrottleTarget;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::Binder;

pub async fn handle_alter_streaming_rate_limit(
    handler_args: HandlerArgs,
    kind: PbThrottleTarget,
    table_name: ObjectName,
    rate_limit: i32,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

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
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a table",
                ))
                .into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_TABLE, table.id.table_id)
        }
        PbThrottleTarget::TableDml => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;
            if table.table_type != TableType::Table {
                return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a table",
                ))
                .into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_TABLE, table.id.table_id)
        }
        _ => bail!("Unsupported throttle target: {:?}", kind),
    };

    let meta_client = session.env().meta_client();

    let rate_limit = if rate_limit < 0 {
        None
    } else {
        Some(rate_limit as u32)
    };

    meta_client.apply_throttle(kind, id, rate_limit).await?;

    Ok(PgResponse::empty_result(stmt_type))
}
