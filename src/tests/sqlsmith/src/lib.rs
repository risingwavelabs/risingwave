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

#![feature(let_chains)]
#![feature(if_let_guard)]
#![feature(lazy_cell)]
#![feature(box_patterns)]

use std::collections::{HashMap, HashSet};

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{BinaryOperator, ColumnOption, Expr, Join, JoinConstraint, JoinOperator, Statement, TableConstraint};
use risingwave_sqlparser::parser::Parser;

use crate::sql_gen::SqlGenerator;

pub mod reducer;
pub mod runner;
mod sql_gen;
mod utils;
pub mod validation;
pub use validation::is_permissible_error;

pub use crate::sql_gen::{print_function_table, Table};

/// Generate a random SQL string.
pub fn sql_gen(rng: &mut impl Rng, tables: Vec<Table>) -> String {
    let mut gen = SqlGenerator::new(rng, tables);
    format!("{}", gen.gen_batch_query_stmt())
}

/// Generate `INSERT`
pub fn insert_sql_gen(rng: &mut impl Rng, tables: Vec<Table>, count: usize) -> Vec<String> {
    let mut gen = SqlGenerator::new(rng, vec![]);
    tables
        .into_iter()
        .map(|table| format!("{}", gen.gen_insert_stmt(table, count)))
        .collect()
}

/// Generate a random CREATE MATERIALIZED VIEW sql string.
/// These are derived from `tables`.
pub fn mview_sql_gen<R: Rng>(rng: &mut R, tables: Vec<Table>, name: &str) -> (String, Table) {
    let mut gen = SqlGenerator::new_for_mview(rng, tables);
    let (mview, table) = gen.gen_mview_stmt(name);
    (mview.to_string(), table)
}

/// TODO(noel): Eventually all session variables should be fuzzed.
/// For now we start of with a few hardcoded configs.
/// Some config need workarounds, for instance `QUERY_MODE`,
/// which can lead to stack overflow
/// (a simple workaround is limit length of
/// generated query when `QUERY_MODE=local`.
pub fn session_sql_gen<R: Rng>(rng: &mut R) -> String {
    [
        "SET RW_ENABLE_TWO_PHASE_AGG TO TRUE",
        "SET RW_ENABLE_TWO_PHASE_AGG TO FALSE",
        "SET RW_FORCE_TWO_PHASE_AGG TO TRUE",
        "SET RW_FORCE_TWO_PHASE_AGG TO FALSE",
    ]
    .choose(rng)
    .unwrap()
    .to_string()
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
                let is_primary_key = column.options.iter().any(|option|
                    match option.option {
                        ColumnOption::Unique { is_primary: true } => true,
                        _ => false,
                    }
                );
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
            let pk_indices = pk_indices.into_iter().collect();
            Table::new_with_pk(
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

pub fn parse_create_table_statements(sql: &str) -> Vec<Table> {
    let statements = parse_sql(&sql);
    statements
        .iter()
        .map(create_table_statement_to_table)
        .collect()
}

#[cfg(test)]
mod tests {
    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: Vec<Table>, expect: Expect) {
        let actual = format!("{:#?}", actual); // pretty print the output.
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
                },
            ]"#]],
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
                            2,
                            0,
                            1,
                        ],
                    },
                ]"#]],
        );
    }
}
