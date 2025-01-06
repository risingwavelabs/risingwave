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

use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_sqlparser::ast::*;

use crate::error::Result;
use crate::utils::data_type::DataTypeToAst as _;

/// Try to restore missing column definitions and constraints in the persisted table definition,
/// if the schema of the table is derived from external systems (like schema registry) or it's
/// created by `CREATE TABLE AS`.
///
/// Returns error if restoring failed, or called on non-`TableType::Table`, or the persisted
/// definition is invalid.
pub fn try_purify_table_create_sql_ast(
    mut base: Statement,
    columns: &[ColumnCatalog],
    row_id_index: Option<usize>,
    pk_column_ids: &[ColumnId],
) -> Result<Statement> {
    let Statement::CreateTable {
        columns: column_defs,
        constraints,
        wildcard_idx,
        ..
    } = &mut base
    else {
        bail!("expect `CREATE TABLE` statement, found: `{:?}`", base);
    };

    // Filter out columns that are not defined by users in SQL.
    let defined_columns = columns.iter().filter(|c| c.is_user_defined());

    // If all columns are defined...
    // - either the schema is fully specified by the user,
    // - the persisted definition is already purified.
    // No need to proceed.
    if !column_defs.is_empty() && wildcard_idx.is_none() {
        let defined_columns_len = defined_columns.count();
        if column_defs.len() != defined_columns_len {
            bail /* unlikely */ !(
                "column count mismatch: defined {} columns, but {} columns in the definition",
                defined_columns_len,
                column_defs.len()
            );
        }

        return Ok(base);
    }

    // Schema inferred. Now derive the missing columns and constraints.
    // First, remove the wildcard from the definition.
    *wildcard_idx = None;

    // Derive `ColumnDef` from `ColumnCatalog`.
    let mut purified_column_defs = Vec::new();
    for column in defined_columns {
        // If the column is already defined in the persisted definition, keep it.
        if let Some(existing) = column_defs
            .iter()
            .find(|c| c.name.real_value() == column.name())
        {
            purified_column_defs.push(existing.clone());
            continue;
        }

        if let Some(c) = &column.column_desc.generated_or_default_column {
            match c {
                GeneratedOrDefaultColumn::GeneratedColumn(_) => {
                    unreachable!("generated column must not be inferred");
                }
                GeneratedOrDefaultColumn::DefaultColumn(_) => {
                    // TODO: convert `ExprNode` back to ast can be a bit tricky.
                    // Fortunately, this case is rare as inferring default values is not
                    // widely supported.
                    bail /* unlikely */ !("purifying default value is not supported yet");
                }
            }
        }

        let column_def = ColumnDef {
            name: column.name().into(),
            data_type: Some(column.data_type().to_ast()),
            collation: None,
            options: Vec::new(), // pk will be specified with table constraints
        };
        purified_column_defs.push(column_def);
    }
    *column_defs = purified_column_defs;

    if row_id_index.is_none() {
        // User-defined primary key.
        let mut pk_columns = Vec::new();

        for &id in pk_column_ids {
            let column = columns.iter().find(|c| c.column_id() == id).unwrap();
            if !column.is_user_defined() {
                bail /* unlikely */ !(
                    "primary key column \"{}\" is not user-defined",
                    column.name()
                );
            }
            pk_columns.push(column.name().into());
        }

        let pk_constraint = TableConstraint::Unique {
            name: None,
            columns: pk_columns,
            is_primary: true,
        };

        // We don't support table constraints other than `PRIMARY KEY`, thus simply overwrite.
        assert!(
            constraints.len() <= 1
                && constraints.iter().all(|c| matches!(
                    c,
                    TableConstraint::Unique {
                        is_primary: true,
                        ..
                    }
                )),
            "unexpected table constraints: {constraints:?}",
        );

        *constraints = vec![pk_constraint];
    }

    Ok(base)
}
