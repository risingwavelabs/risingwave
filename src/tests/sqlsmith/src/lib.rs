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
#![feature(once_cell)]
#![feature(box_patterns)]

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Statement,
};
use risingwave_sqlparser::parser::Parser;

use crate::sql_gen::SqlGenerator;

pub mod runner;
mod sql_gen;
pub mod validation;
pub use validation::is_permissible_error;

pub use crate::sql_gen::{print_function_table, Table};

/// Generate a random SQL string.
pub fn sql_gen(rng: &mut impl Rng, tables: Vec<Table>) -> String {
    let mut gen = SqlGenerator::new(rng, tables);
    format!("{}", gen.gen_batch_query_stmt())
}

/// Generate `INSERT`
#[allow(dead_code)]
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
        Statement::CreateTable { name, columns, .. } => Table {
            name: name.0[0].real_value(),
            columns: columns.iter().map(|c| c.clone().into()).collect(),
        },
        _ => panic!(
            "Only CREATE TABLE statements permitted, received: {}",
            statement
        ),
    }
}
