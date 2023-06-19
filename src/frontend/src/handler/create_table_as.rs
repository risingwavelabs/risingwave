// Copyright 2023 RisingWave Labs
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
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName, Query, Statement};

use super::{HandlerArgs, RwPgResponse};
use crate::binder::BoundStatement;
use crate::handler::create_table::{gen_create_table_plan_without_bind, ColumnIdGenerator};
use crate::handler::query::handle_query;
use crate::{build_graph, Binder, OptimizerContext};

pub async fn handle_create_as(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_not_exists: bool,
    query: Box<Query>,
    column_defs: Vec<ColumnDef>,
    append_only: bool,
) -> Result<RwPgResponse> {
    if column_defs.iter().any(|column| column.data_type.is_some()) {
        return Err(ErrorCode::InvalidInputSyntax(
            "Should not specify data type in CREATE TABLE AS".into(),
        )
        .into());
    }
    let session = handler_args.session.clone();

    if let Err(e) = session.check_relation_name_duplicated(table_name.clone()) {
        if if_not_exists {
            return Ok(PgResponse::empty_result_with_notice(
                StatementType::CREATE_TABLE,
                format!("relation \"{}\" already exists, skipping", table_name),
            ));
        } else {
            return Err(e);
        }
    }

    let mut col_id_gen = ColumnIdGenerator::new_initial();

    // Generate catalog descs from query
    let mut columns: Vec<_> = {
        let mut binder = Binder::new(&session);
        let bound = binder.bind(Statement::Query(query.clone()))?;
        if let BoundStatement::Query(query) = bound {
            // Create ColumnCatelog by Field
            query
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let id = col_id_gen.generate(&field.name);
                    ColumnCatalog {
                        column_desc: ColumnDesc::from_field_with_column_id(field, id.get_id()),
                        is_hidden: false,
                    }
                })
                .collect()
        } else {
            unreachable!()
        }
    };

    if column_defs.len() > columns.len() {
        return Err(ErrorCode::InvalidInputSyntax(
            "too many column names were specified".to_string(),
        )
        .into());
    }

    // Override column name if it specified in creaet statement.
    column_defs.iter().enumerate().for_each(|(idx, column)| {
        columns[idx].column_desc.name = column.name.real_value();
    });

    let (graph, source, table) = {
        let context = OptimizerContext::from_handler_args(handler_args.clone());
        let properties = handler_args
            .with_options
            .inner()
            .clone()
            .into_iter()
            .collect();
        let (plan, source, table) = gen_create_table_plan_without_bind(
            context,
            table_name.clone(),
            columns,
            vec![],
            vec![],
            properties,
            "".to_owned(), // TODO: support `SHOW CREATE TABLE` for `CREATE TABLE AS`
            vec![],        // No watermark should be defined in for `CREATE TABLE AS`
            append_only,
            Some(col_id_gen.into_version()),
        )?;
        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (graph, source, table)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_table(source, table, graph).await?;

    // Generate insert
    let insert = Statement::Insert {
        table_name,
        columns: vec![],
        source: query,
        returning: vec![],
    };

    handle_query(handler_args, insert, vec![]).await
}
