// Copyright 2023 Singularity Data
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
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName, Query, Statement};

use super::{HandlerArgs, RwPgResponse};
use crate::binder::{BoundSetExpr, BoundStatement};
use crate::handler::create_table::gen_create_table_plan_without_bind;
use crate::handler::query::handle_query;
use crate::{build_graph, Binder, OptimizerContext};

/// Used in `handle_create_as` to convert filed to column desc
fn convert_field_to_column_desc(field: &Field, column_id: i32) -> ColumnDesc {
    let field_descs = field
        .sub_fields
        .iter()
        .map(|field| convert_field_to_column_desc(field, 0))
        .collect();

    ColumnDesc {
        data_type: field.data_type(),
        name: field.name.clone(),
        column_id: column_id.into(),
        field_descs,
        type_name: "".to_string(),
    }
}

pub async fn handle_create_as(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_not_exists: bool,
    query: Box<Query>,
    columns: Vec<ColumnDef>,
) -> Result<RwPgResponse> {
    if columns.iter().any(|column| column.data_type.is_some()) {
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

    // Generate catalog descs from query
    let mut column_descs: Vec<_> = {
        let mut binder = Binder::new(&session);
        let bound = binder.bind(Statement::Query(query.clone()))?;
        if let BoundStatement::Query(query) = bound {
            // Check if all expressions have an alias
            if let BoundSetExpr::Select(select) = &query.body {
                if select.aliases.iter().any(Option::is_none) {
                    return Err(ErrorCode::BindError(
                        "An alias must be specified for an expression".to_string(),
                    )
                    .into());
                }
            }

            // Create ColumnCatelog by Field
            query
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(column_id, field)| convert_field_to_column_desc(field, column_id as i32))
                .collect()
        } else {
            unreachable!()
        }
    };

    if columns.len() > column_descs.len() {
        return Err(ErrorCode::InvalidInputSyntax(
            "too many column names were specified".to_string(),
        )
        .into());
    }

    columns.iter().enumerate().for_each(|(idx, column)| {
        column_descs[idx].name = column.name.real_value();
    });

    let (graph, source, table) = {
        let context = OptimizerContext::from_handler_args(handler_args.clone());
        let (plan, source, table) = gen_create_table_plan_without_bind(
            &session,
            context.into(),
            table_name.clone(),
            column_descs,
            None,
            vec![],
            "".to_owned(), // TODO: support `SHOW CREATE TABLE` for `CREATE TABLE AS`
            None,
        )?;
        let graph = build_graph(plan);

        (graph, source, table)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();

    // TODO(Yuanxin): `source` will contain either an external source or nothing. Rewrite
    // `create_table` accordingly.
    catalog_writer.create_table(source, table, graph).await?;

    // Generate insert
    let insert = Statement::Insert {
        table_name,
        columns: vec![],
        source: query,
    };

    handle_query(handler_args, insert, false).await
}
