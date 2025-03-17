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

//! Provides Data structures for query generation,
//! and the interface for generating
//! stream (MATERIALIZED VIEW) and batch query statements.

use std::vec;

use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_frontend::bind_data_type;
use risingwave_sqlparser::ast::{ColumnDef, Expr, Ident, ObjectName, Statement};

mod agg;
mod cast;
mod expr;
pub use expr::print_function_table;

mod dml;
mod functions;
mod query;
mod relation;
mod scalar;
mod time_window;
mod types;
mod utils;

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_indices: Vec<usize>,
    pub is_base_table: bool,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            pk_indices: vec![],
            is_base_table: false,
        }
    }

    pub fn new_for_base_table(name: String, columns: Vec<Column>, pk_indices: Vec<usize>) -> Self {
        Self {
            name,
            columns,
            pk_indices,
            is_base_table: true,
        }
    }

    pub fn get_qualified_columns(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|c| Column {
                name: format!("{}.{}", self.name, c.name),
                data_type: c.data_type.clone(),
            })
            .collect()
    }
}

/// Sqlsmith Column definition
#[derive(Clone, Debug)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
}

impl From<ColumnDef> for Column {
    fn from(c: ColumnDef) -> Self {
        Self {
            name: c.name.real_value(),
            data_type: bind_data_type(&c.data_type.expect("data type should not be none")).unwrap(),
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) struct SqlGeneratorContext {
    can_agg: bool, // This is used to disable agg expr totally,
    // Used in top level, where we want to test queries
    // without aggregates.
    inside_agg: bool,
}

impl SqlGeneratorContext {
    pub fn new() -> Self {
        SqlGeneratorContext {
            can_agg: true,
            inside_agg: false,
        }
    }

    pub fn new_with_can_agg(can_agg: bool) -> Self {
        Self {
            can_agg,
            inside_agg: false,
        }
    }

    pub fn set_inside_agg(self) -> Self {
        Self {
            inside_agg: true,
            ..self
        }
    }

    pub fn can_gen_agg(self) -> bool {
        self.can_agg && !self.inside_agg
    }

    pub fn is_inside_agg(self) -> bool {
        self.inside_agg
    }
}

pub(crate) struct SqlGenerator<'a, R: Rng> {
    tables: Vec<Table>,
    rng: &'a mut R,

    /// Relation ID used to generate table names and aliases
    relation_id: u32,

    /// Relations bound in generated query.
    /// We might not read from all tables.
    bound_relations: Vec<Table>,

    /// Columns bound in generated query.
    /// May not contain all columns from `Self::bound_relations`.
    /// e.g. GROUP BY clause will constrain `bound_columns`.
    bound_columns: Vec<Column>,

    /// `SqlGenerator` can be used in two execution modes:
    /// 1. Generating Query Statements.
    /// 2. Generating queries for CREATE MATERIALIZED VIEW.
    ///    Under this mode certain restrictions and workarounds are applied
    ///    for unsupported stream executors.
    is_mview: bool,

    recursion_weight: f64,
    // /// Count number of subquery.
    // /// We don't want too many per query otherwise it is hard to debug.
    // with_statements: u64,
}

/// Generators
impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new(rng: &'a mut R, tables: Vec<Table>) -> Self {
        SqlGenerator {
            tables,
            rng,
            relation_id: 0,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: false,
            recursion_weight: 0.3,
        }
    }

    pub(crate) fn new_for_mview(rng: &'a mut R, tables: Vec<Table>) -> Self {
        // distinct aggregate is not allowed for MV
        SqlGenerator {
            tables,
            rng,
            relation_id: 0,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: true,
            recursion_weight: 0.3,
        }
    }

    pub(crate) fn gen_batch_query_stmt(&mut self) -> Statement {
        let (query, _) = self.gen_query();
        Statement::Query(Box::new(query))
    }

    pub(crate) fn gen_mview_stmt(&mut self, name: &str) -> (Statement, Table) {
        let (query, schema) = self.gen_query();
        let query = Box::new(query);
        let table = Table::new(name.to_owned(), schema);
        let name = ObjectName(vec![Ident::new_unchecked(name)]);
        let mview = Statement::CreateView {
            or_replace: false,
            materialized: true,
            if_not_exists: false,
            name,
            columns: vec![],
            query,
            with_options: vec![],
            emit_mode: None,
        };
        (mview, table)
    }

    /// 50/50 chance to be true/false.
    fn flip_coin(&mut self) -> bool {
        self.rng.gen_bool(0.5)
    }

    /// Provide recursion bounds.
    pub(crate) fn can_recurse(&mut self) -> bool {
        if self.recursion_weight <= 0.0 {
            return false;
        }
        let can_recurse = self.rng.gen_bool(self.recursion_weight);
        if can_recurse {
            self.recursion_weight *= 0.9;
            if self.recursion_weight < 0.05 {
                self.recursion_weight = 0.0;
            }
        }
        can_recurse
    }
}
