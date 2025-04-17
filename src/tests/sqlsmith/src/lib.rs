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

#![feature(let_chains)]
#![feature(if_let_guard)]
#![feature(box_patterns)]
#![feature(register_tool)]
#![register_tool(rw)]
#![allow(rw::format_error)] // test code

risingwave_expr_impl::enable!();

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use itertools::Itertools;
use rand::Rng;
use rand::prelude::IndexedRandom;
use risingwave_sqlparser::ast::{
    BinaryOperator, ColumnOption, Expr, Join, JoinConstraint, JoinOperator, Statement,
    TableConstraint,
};
use risingwave_sqlparser::parser::Parser;

use crate::sql_gen::SqlGenerator;

pub mod reducer;
mod sql_gen;
pub mod test_runners;
mod utils;
pub mod validation;
pub use validation::is_permissible_error;

pub use crate::sql_gen::{Table, print_function_table};

/// Generate a random SQL string.
pub fn sql_gen(rng: &mut impl Rng, tables: Vec<Table>) -> String {
    let mut r#gen = SqlGenerator::new(rng, tables);
    format!("{}", r#gen.gen_batch_query_stmt())
}

/// Generate `INSERT`
pub fn insert_sql_gen(rng: &mut impl Rng, tables: Vec<Table>, count: usize) -> Vec<String> {
    let mut r#gen = SqlGenerator::new(rng, vec![]);
    tables
        .into_iter()
        .map(|table| format!("{}", r#gen.generate_insert_statement(&table, count)))
        .collect()
}

/// Generate a random CREATE MATERIALIZED VIEW sql string.
/// These are derived from `tables`.
pub fn mview_sql_gen<R: Rng>(rng: &mut R, tables: Vec<Table>, name: &str) -> (String, Table) {
    let mut r#gen = SqlGenerator::new_for_mview(rng, tables);
    let (mview, table) = r#gen.gen_mview_stmt(name);
    (mview.to_string(), table)
}

pub fn differential_sql_gen<R: Rng>(
    rng: &mut R,
    tables: Vec<Table>,
    name: &str,
) -> Result<(String, String, Table)> {
    let mut r#gen = SqlGenerator::new_for_mview(rng, tables);
    let (stream, table) = r#gen.gen_mview_stmt(name);
    let batch = match stream {
        Statement::CreateView { ref query, .. } => query.to_string(),
        _ => bail!("Differential pair should be mview statement!"),
    };
    Ok((batch, stream.to_string(), table))
}

/// TODO(noel): Eventually all session variables should be fuzzed.
/// For now we start of with a few hardcoded configs.
/// Some config need workarounds, for instance `QUERY_MODE`,
/// which can lead to stack overflow
/// (a simple workaround is limit length of
/// generated query when `QUERY_MODE=local`.
pub fn session_sql_gen<R: Rng>(rng: &mut R) -> String {
    [
        "SET ENABLE_TWO_PHASE_AGG TO TRUE",
        "SET ENABLE_TWO_PHASE_AGG TO FALSE",
        "SET RW_FORCE_TWO_PHASE_AGG TO TRUE",
        "SET RW_FORCE_TWO_PHASE_AGG TO FALSE",
    ]
    .choose(rng)
    .unwrap()
    .to_string()
}

pub fn generate_update_statements<R: Rng>(
    rng: &mut R,
    tables: &[Table],
    inserts: &[Statement],
) -> Result<Vec<Statement>> {
    let mut r#gen = SqlGenerator::new(rng, vec![]);
    r#gen.generate_update_statements(tables, inserts)
}

/// Parse SQL
/// FIXME(Noel): Introduce error type for sqlsmith for this.
pub fn parse_sql<S: AsRef<str>>(sql: S) -> Vec<Statement> {
    let sql = sql.as_ref();
    Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql))
}

/// Extract relevant info from CREATE TABLE statement, to construct a Table
pub fn create_table_statement_to_table(statement: &Statement) -> Table {
    match statement {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let column_name_to_index_mapping: HashMap<_, _> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| (&c.name, i))
                .collect();
            let mut pk_indices = HashSet::new();
            for (i, column) in columns.iter().enumerate() {
                let is_primary_key = column
                    .options
                    .iter()
                    .any(|option| option.option == ColumnOption::Unique { is_primary: true });
                if is_primary_key {
                    pk_indices.insert(i);
                }
            }
            for constraint in constraints {
                if let TableConstraint::Unique {
                    columns,
                    is_primary: true,
                    ..
                } = constraint
                {
                    for column in columns {
                        let pk_index = column_name_to_index_mapping.get(column).unwrap();
                        pk_indices.insert(*pk_index);
                    }
                }
            }
            let mut pk_indices = pk_indices.into_iter().collect_vec();
            pk_indices.sort_unstable();
            Table::new_for_base_table(
                name.0[0].real_value(),
                columns.iter().map(|c| c.clone().into()).collect(),
                pk_indices,
            )
        }
        _ => panic!(
            "Only CREATE TABLE statements permitted, received: {}",
            statement
        ),
    }
}

pub fn parse_create_table_statements(sql: impl AsRef<str>) -> (Vec<Table>, Vec<Statement>) {
    let statements = parse_sql(&sql);
    let tables = statements
        .iter()
        .map(create_table_statement_to_table)
        .collect();
    (tables, statements)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{Expect, expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_parse_create_table_statements_no_pk() {
        let test_string = "
CREATE TABLE t(v1 int);
CREATE TABLE t2(v1 int, v2 bool);
CREATE TABLE t3(v1 int, v2 bool, v3 smallint);
        ";
        check(
            parse_create_table_statements(test_string),
            expect![[r#"
                (
                    [
                        Table {
                            name: "t",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                            ],
                            pk_indices: [],
                            is_base_table: true,
                        },
                        Table {
                            name: "t2",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                                Column {
                                    name: "v2",
                                    data_type: Boolean,
                                },
                            ],
                            pk_indices: [],
                            is_base_table: true,
                        },
                        Table {
                            name: "t3",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                                Column {
                                    name: "v2",
                                    data_type: Boolean,
                                },
                                Column {
                                    name: "v3",
                                    data_type: Int16,
                                },
                            ],
                            pk_indices: [],
                            is_base_table: true,
                        },
                    ],
                    [
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t2",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v2",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Boolean,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t3",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v2",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Boolean,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v3",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        SmallInt,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                    ],
                )"#]],
        );
    }

    #[test]
    fn test_parse_create_table_statements_with_pk() {
        let test_string = "
CREATE TABLE t(v1 int PRIMARY KEY);
CREATE TABLE t2(v1 int, v2 smallint PRIMARY KEY);
CREATE TABLE t3(v1 int PRIMARY KEY, v2 smallint PRIMARY KEY);
CREATE TABLE t4(v1 int PRIMARY KEY, v2 smallint PRIMARY KEY, v3 bool PRIMARY KEY);
";
        check(
            parse_create_table_statements(test_string),
            expect![[r#"
                (
                    [
                        Table {
                            name: "t",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                            ],
                            pk_indices: [
                                0,
                            ],
                            is_base_table: true,
                        },
                        Table {
                            name: "t2",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                                Column {
                                    name: "v2",
                                    data_type: Int16,
                                },
                            ],
                            pk_indices: [
                                1,
                            ],
                            is_base_table: true,
                        },
                        Table {
                            name: "t3",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                                Column {
                                    name: "v2",
                                    data_type: Int16,
                                },
                            ],
                            pk_indices: [
                                0,
                                1,
                            ],
                            is_base_table: true,
                        },
                        Table {
                            name: "t4",
                            columns: [
                                Column {
                                    name: "v1",
                                    data_type: Int32,
                                },
                                Column {
                                    name: "v2",
                                    data_type: Int16,
                                },
                                Column {
                                    name: "v3",
                                    data_type: Boolean,
                                },
                            ],
                            pk_indices: [
                                0,
                                1,
                                2,
                            ],
                            is_base_table: true,
                        },
                    ],
                    [
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t2",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v2",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        SmallInt,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t3",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v2",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        SmallInt,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                        CreateTable {
                            or_replace: false,
                            temporary: false,
                            if_not_exists: false,
                            name: ObjectName(
                                [
                                    Ident {
                                        value: "t4",
                                        quote_style: None,
                                    },
                                ],
                            ),
                            columns: [
                                ColumnDef {
                                    name: Ident {
                                        value: "v1",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Int,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v2",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        SmallInt,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                                ColumnDef {
                                    name: Ident {
                                        value: "v3",
                                        quote_style: None,
                                    },
                                    data_type: Some(
                                        Boolean,
                                    ),
                                    collation: None,
                                    options: [
                                        ColumnOptionDef {
                                            name: None,
                                            option: Unique {
                                                is_primary: true,
                                            },
                                        },
                                    ],
                                },
                            ],
                            wildcard_idx: None,
                            constraints: [],
                            with_options: [],
                            format_encode: None,
                            source_watermarks: [],
                            append_only: false,
                            on_conflict: None,
                            with_version_column: None,
                            query: None,
                            cdc_table_info: None,
                            include_column_options: [],
                            webhook_info: None,
                            engine: Hummock,
                        },
                    ],
                )"#]],
        );
    }
}
