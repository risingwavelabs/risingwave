// Copyright 2022 Singularity Data
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
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ColumnDef, Ident, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::create_table::{gen_create_table_plan, ColumnIdGenerator};
use super::{HandlerArgs, RwPgResponse};
use crate::binder::Relation;
use crate::{build_graph, Binder, OptimizerContext};

#[expect(clippy::unused_async)]
pub async fn handle_add_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_column: ColumnDef,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let catalog = {
        let relation = Binder::new(&session).bind_relation_by_name(table_name.clone(), None)?;
        match relation {
            Relation::BaseTable(table) if table.table_catalog.is_table() => table.table_catalog,
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }
    };

    let [mut definition]: [_; 1] = Parser::parse_sql(&catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable { columns, .. } = &mut definition else {
        panic!()
    };

    // Duplicated names can actually be checked by `StreamMaterialize`. We do here for better error
    // reporting.
    let new_column_name = new_column.name.real_value();
    if columns
        .iter()
        .any(|c| c.name.real_value() == new_column_name)
    {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{}\" of table \"{}\" already exists",
            new_column_name, table_name
        )))?
    }

    columns.push(new_column);

    let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
    let mut col_id_gen = ColumnIdGenerator::new(
        catalog.columns(),
        catalog.version.as_ref().unwrap().next_column_id,
    );
    let Statement::CreateTable {
        columns,
        constraints,
        ..
    } = definition else {
        panic!();
    };

    let new_name = {
        let mut idents = table_name.0;
        let last_ident = idents.last_mut().unwrap();
        *last_ident = Ident::new(format!("{}_add_column_{}", last_ident, new_column_name));
        ObjectName(idents)
    };

    let (graph, source, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, source, table) = gen_create_table_plan(
            &session,
            context.into(),
            new_name,
            columns,
            constraints,
            &mut col_id_gen,
        )?;
        let graph = build_graph(plan);

        (graph, source, table)
    };

    let catalog_writer = session.env().catalog_writer();

    catalog_writer.create_table(source, table, graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))

    // Err(ErrorCode::NotImplemented(
    //     "ADD COLUMN".to_owned(),
    //     6903.into(),
    // ))?
}
