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

use anyhow::{anyhow, Context};
use fancy_regex::Regex;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_sqlparser::ast::{ConnectorSchema, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::alter_source_with_sr::alter_definition_format_encode;
use super::alter_table_column::{
    fetch_table_catalog_for_alter, replace_table_with_definition, schema_has_schema_registry,
};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};
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

    let connector_schema = {
        let connector_schema = get_connector_schema_from_table(&original_table)?;
        if !connector_schema
            .as_ref()
            .is_some_and(schema_has_schema_registry)
        {
            return Err(ErrorCode::NotSupported(
                "tables without schema registry cannot refreshed".to_string(),
                "try `ALTER TABLE .. ADD/DROP COLUMN ...` instead".to_string(),
            )
            .into());
        }
        connector_schema.unwrap()
    };

    let definition = alter_definition_format_encode(
        &original_table.definition,
        connector_schema.row_options.clone(),
    )?;

    let [definition]: [_; 1] = Parser::parse_sql(&definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    let result = replace_table_with_definition(
        &session,
        table_name,
        definition,
        &original_table,
        Some(connector_schema),
    )
    .await;

    match result {
        Ok(_) => Ok(RwPgResponse::empty_result(StatementType::ALTER_TABLE)),
        Err(e) => {
            let report = e.to_report_string();
            // This is a workaround for reporting errors when columns to drop is referenced by generated column.
            // Finding the actual columns to drop requires generating `PbSource` from the sql definition
            // and fetching schema from schema registry, which will cause a lot of unnecessary refactor.
            // Here we match the error message to yield when failing to bind generated column exprs.
            let re = Regex::new(r#"fail to bind expression in generated column "(.*?)""#).unwrap();
            let captures = re.captures(&report).map_err(anyhow::Error::from)?;
            if let Some(gen_col_name) = captures.and_then(|captures| captures.get(1)) {
                Err(anyhow!(e).context(format!("failed to refresh schema because some of the columns to drop are referenced by a generated column \"{}\"",
                    gen_col_name.as_str())).into())
            } else {
                Err(e)
            }
        }
    }
}
