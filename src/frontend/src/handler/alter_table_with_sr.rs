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

use anyhow::Context;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_sqlparser::ast::{ConnectorSchema, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::alter_source_with_sr::alter_definition_format_encode;
use super::alter_table_column::{fetch_table_catalog_for_alter, replace_table_with_definition};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result, RwError};
use crate::TableCatalog;

fn get_connector_schema_from_table(table: &TableCatalog) -> Result<Option<ConnectorSchema>> {
    let [stmt]: [_; 1] = Parser::parse_sql(&table.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable { source_schema, .. } = stmt else {
        unreachable!()
    };
    Ok(source_schema.map(|schema| schema.into_v2_with_warning()))
}

pub async fn handle_refresh_schema(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let original_table = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    if !original_table.incoming_sinks.is_empty() {
        bail_not_implemented!("alter table with incoming sinks");
    }

    // TODO(yuhao): alter table with generated columns.
    if original_table.has_generated_column() {
        return Err(RwError::from(ErrorCode::BindError(
            "Alter a table with generated column has not been implemented.".to_string(),
        )));
    }

    let connector_schema =
        get_connector_schema_from_table(&original_table)?.ok_or(ErrorCode::NotSupported(
            "Tables without schema registry cannot refreshed".to_string(),
            "try `ALTER TABLE .. ADD/DROP COLUMN ...` instead".to_string(),
        ))?;

    let definition = alter_definition_format_encode(
        &original_table.definition,
        connector_schema.row_options.clone(),
    )?;

    let [definition]: [_; 1] = Parser::parse_sql(&definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    let source_schema = get_connector_schema_from_table(&original_table)?;
    replace_table_with_definition(
        &session,
        table_name,
        definition,
        &original_table,
        source_schema,
    )
    .await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_TABLE))
}
