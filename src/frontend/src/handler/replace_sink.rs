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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ICEBERG_SINK_PREFIX, StreamJobStatus};
use risingwave_connector::sink::SINK_SNAPSHOT_OPTION;
use risingwave_sqlparser::ast::{CreateSink, CreateSinkStatement, ReplaceSinkStatement, Statement};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::RwPgResponse;
use super::create_sink::{SinkPlanContext, gen_sink_plan};
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;
use crate::stream_fragmenter::{GraphJobType, build_graph_with_strategy};

pub async fn handle_replace_sink(
    mut handle_args: HandlerArgs,
    stmt: ReplaceSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_cluster_limits().await?;

    // Check that the sink EXISTS (opposite of create sink which checks it doesn't exist)
    let db_name = &session.database();
    let (schema_name, sink_name) = Binder::resolve_schema_qualified_name(db_name, &stmt.sink_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_name = match &schema_name {
            Some(schema_name) => schema_name.clone(),
            None => catalog_reader
                .first_valid_schema(db_name, &search_path, user_name)?
                .name(),
        };
        let schema_path = SchemaPath::Name(&schema_name);

        // Get the existing sink to validate it
        let (old_sink, _) =
            catalog_reader.get_any_sink_by_name(db_name, schema_path, &sink_name)?;

        if old_sink.stream_job_status != StreamJobStatus::Created {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
                "only sink in Created status can be replaced, current status: {:?}",
                old_sink.stream_job_status
            ))));
        }

        // Validate that the old sink is a FROM-type sink (not AS query)
        // Re-parse the definition to check
        let definition = &old_sink.definition;
        let ast = Parser::parse_sql(definition).map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "Failed to parse old sink definition: {}",
                e.as_report()
            )))
        })?;
        match ast.into_iter().next() {
            Some(Statement::CreateSink {
                stmt: old_create_stmt,
            }) => {
                if matches!(old_create_stmt.sink_from, CreateSink::AsQuery(_)) {
                    return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                        "REPLACE SINK is not supported for sinks created with AS query syntax. Only FROM syntax is supported.".to_owned(),
                    )));
                }
            }
            Some(Statement::ReplaceSink { .. }) => {}
            _ => {
                return Err(RwError::from(ErrorCode::InternalError(
                    "Failed to parse old sink definition as CREATE SINK statement".to_owned(),
                )));
            }
        }
    }

    if stmt.sink_name.base_name().starts_with(ICEBERG_SINK_PREFIX) {
        return Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
            "Sink name cannot start with reserved prefix '{}'",
            ICEBERG_SINK_PREFIX
        ))));
    }

    // Handle snapshot option: REPLACE SINK does not support backfill
    // If user specified snapshot, it must be false; otherwise, we add snapshot = false
    if let Some(snapshot_value) = handle_args.with_options.get(SINK_SNAPSHOT_OPTION) {
        if !snapshot_value.eq_ignore_ascii_case("false") {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "REPLACE SINK does not support backfill. The 'snapshot' option must be 'false' or omitted.".to_owned(),
            )));
        }
    } else {
        handle_args
            .with_options
            .insert(SINK_SNAPSHOT_OPTION.to_owned(), "false".to_owned());
    }

    // Convert ReplaceSinkStatement to CreateSinkStatement for gen_sink_plan
    // Note: REPLACE SINK only supports FROM syntax, not AS query or sink-into-table
    let create_stmt = CreateSinkStatement {
        if_not_exists: false,
        sink_name: stmt.sink_name,
        with_properties: stmt.with_properties,
        sink_from: CreateSink::From(stmt.from_name),
        columns: vec![], // Not used for non-sink-into-table
        emit_mode: stmt.emit_mode,
        sink_schema: stmt.sink_schema,
        into_table_name: None, // REPLACE SINK does not support sink-into-table
    };

    let (sink, graph, dependencies) = {
        let SinkPlanContext {
            query,
            sink_plan: plan,
            sink_catalog: sink,
            target_table_catalog: _, // Not used - REPLACE SINK doesn't support sink-into-table
            dependencies,
        } = gen_sink_plan(handle_args, create_stmt, None, false).await?;

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            plan.ctx().warn_to_user(
                r#"The ORDER BY clause in the REPLACE SINK statement has no effect at all."#
                    .to_owned(),
            );
        }

        let graph = build_graph_with_strategy(
            plan,
            Some(GraphJobType::Sink),
            None, // No backfill for REPLACE SINK
        )?;

        (sink, graph, dependencies)
    };

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_sink(sink.to_proto(), graph, dependencies)
        .await?;

    Ok(PgResponse::empty_result(StatementType::REPLACE_SINK))
}
