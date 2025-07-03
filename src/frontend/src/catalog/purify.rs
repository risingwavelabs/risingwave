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

use anyhow::Context;
use itertools::Itertools;
use prost::Message as _;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_sqlparser::ast::*;

use crate::error::Result;
use crate::session::current;
use crate::utils::data_type::DataTypeToAst as _;

mod pk_column {
    use super::*;
    // Identifies a primary key column...
    pub trait PkColumn {
        fn is(&self, column: &ColumnCatalog) -> bool;
    }
    // ...by column name.
    impl PkColumn for &str {
        fn is(&self, column: &ColumnCatalog) -> bool {
            column.name() == *self
        }
    }
    // ...by column ID.
    impl PkColumn for ColumnId {
        fn is(&self, column: &ColumnCatalog) -> bool {
            column.column_id() == *self
        }
    }
}
use pk_column::PkColumn;

/// Try to restore missing column definitions and constraints in the persisted table (or source)
/// definition, if the schema is derived from external systems (like schema registry) or it's
/// created by `CREATE TABLE AS`.
///
/// Returns error if restoring failed, or the persisted definition is invalid.
pub fn try_purify_table_source_create_sql_ast(
    mut base: Statement,
    columns: &[ColumnCatalog],
    row_id_index: Option<usize>,
    pk_column_ids: &[impl PkColumn],
) -> Result<Statement> {
    if let Some(config) = current::config()
        && config.read().disable_purify_definition()
    {
        current::notice_to_user(
            "purifying definition is disabled via session config, results may be inaccurate",
        );
        return Ok(base);
    }

    let (Statement::CreateTable {
        columns: column_defs,
        constraints,
        wildcard_idx,
        ..
    }
    | Statement::CreateSource {
        stmt:
            CreateSourceStatement {
                columns: column_defs,
                constraints,
                wildcard_idx,
                ..
            },
    }) = &mut base
    else {
        bail!("expect `CREATE TABLE` or `CREATE SOURCE` statement, found: `{base:?}`");
    };

    // First, remove the wildcard from the definition.
    *wildcard_idx = None;

    // Filter out columns that are not defined by users in SQL.
    let defined_columns = columns.iter().filter(|c| c.is_user_defined() || (c.name().starts_with("_rw") && !c.is_hidden()));

    // Derive `ColumnDef` from `ColumnCatalog`.
    let mut purified_column_defs = Vec::new();
    for column in defined_columns {
        let mut column_def = if let Some(existing) = column_defs
            .iter()
            .find(|c| c.name.real_value() == column.name())
        {
            // If the column is already defined in the persisted definition, retrieve it.
            existing.clone()
        } else {
            assert!(
                !column.is_generated(),
                "generated column must not be inferred"
            );

            // Generate a new `ColumnDef` from the catalog.
            ColumnDef {
                name: Ident::from_real_value(column.name()),
                data_type: Some(column.data_type().to_ast()),
                collation: None,
                options: Vec::new(), // pk will be specified with table constraints
            }
        };

        // Fill in the persisted default value desc.
        if let Some(c) = &column.column_desc.generated_or_default_column
            && let GeneratedOrDefaultColumn::DefaultColumn(desc) = c
        {
            let persisted = desc.encode_to_vec().into_boxed_slice();

            let default_value_option = column_def
                .options
                .extract_if(.., |o| {
                    matches!(
                        o.option,
                        ColumnOption::DefaultValue { .. }
                            | ColumnOption::DefaultValueInternal { .. }
                    )
                })
                .at_most_one()
                .ok()
                .context("multiple default value options found")?;

            let expr = default_value_option.and_then(|o| match o.option {
                ColumnOption::DefaultValue(expr) => Some(expr),
                ColumnOption::DefaultValueInternal { expr, .. } => expr,
                _ => unreachable!(),
            });

            column_def.options.push(ColumnOptionDef {
                name: None,
                option: ColumnOption::DefaultValueInternal { persisted, expr },
            });
        }

        purified_column_defs.push(column_def);
    }
    *column_defs = purified_column_defs;

    // Specify user-defined primary key in table constraints.
    let has_pk_column_constraint = column_defs.iter().any(|c| {
        c.options
            .iter()
            .any(|o| matches!(o.option, ColumnOption::Unique { is_primary: true }))
    });
    if !has_pk_column_constraint && row_id_index.is_none() {
        let mut pk_columns = Vec::new();

        for id in pk_column_ids {
            let column = columns
                .iter()
                .find(|c| id.is(c))
                .context("primary key column not found")?;
            if !column.is_user_defined() {
                bail /* unlikely */ !(
                    "primary key column \"{}\" is not user-defined",
                    column.name()
                );
            }
            // Find the name in `Ident` form from `column_defs` to preserve quote style best.
            let name_ident = column_defs
                .iter()
                .find(|c| c.name.real_value() == column.name())
                .unwrap()
                .name
                .clone();
            pk_columns.push(name_ident);
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
