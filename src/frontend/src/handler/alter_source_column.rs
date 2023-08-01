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

use anyhow::Context;
use pgwire::pg_response::{PgResponse, StatementType};

use risingwave_common::error::{ErrorCode, Result};


use risingwave_sqlparser::ast::{AlterSourceOperation, ObjectName, Statement, CreateSourceStatement};
use risingwave_sqlparser::parser::Parser;

use super::create_table::bind_sql_columns;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::Binder;

// TODO:
// 1. generated columns?
// 2. `check_privilege_for_drop_alter`
// 3. test_util

// Behaviour:
// 1. wrong if column exists

/// Handle `ALTER TABLE [ADD] COLUMN` statements.
pub async fn handle_alter_source_column(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    operation: AlterSourceOperation,
) -> Result<RwPgResponse> {
    // 1. Get original definition
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        source.clone()
    };

    // Retrieve the original table definition and parse it to AST.
    let [definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    // TODO: simplify
    let Statement::CreateSource { ref stmt } = &definition else {
        panic!("unexpected statement: {:?}", definition);
    };
    let CreateSourceStatement { columns, ..} = stmt;

    let diff_column = match operation {
        AlterSourceOperation::AddColumn { column_def } => {
            let new_column_name = column_def.name.real_value();
            if columns.iter().any(|c| c.name.real_value() == new_column_name) {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{new_column_name}\" of table \"{source_name}\" already exists"
                )))?
            }
            let mut bound_columns = bind_sql_columns(&[column_def])?;
            bound_columns.remove(0)
        }
        _ => unreachable!()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_source_column(original_catalog.id, diff_column.to_protobuf()).await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SOURCE))
}
