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

#![feature(let_chains)]
#![feature(once_cell)]

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

/// Generate a random CREATE MATERIALIZED VIEW sql string.
/// These are derived from `tables`.
pub fn mview_sql_gen<R: Rng>(rng: &mut R, tables: Vec<Table>, name: &str) -> (String, Table) {
    let mut gen = SqlGenerator::new_for_mview(rng, tables);
    let (mview, table) = gen.gen_mview_stmt(name);
    (mview.to_string(), table)
}

/// Parse SQL
pub fn parse_sql(sql: &str) -> Vec<Statement> {
    Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql))
}

/// Extract relevant info from CREATE TABLE statement, to construct a Table
pub fn create_table_statement_to_table(statement: &Statement) -> Table {
    match statement {
        Statement::CreateTable { name, columns, .. } => Table {
            name: name.0[0].real_value(),
            columns: columns.iter().map(|c| c.clone().into()).collect(),
        },
        _ => panic!("Unexpected statement: {}", statement),
    }
}
