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

use std::collections::HashSet;
use std::vec;

use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_frontend::bind_data_type;
use risingwave_sqlparser::ast::{
    ColumnDef, EmitMode, Expr, Ident, ObjectName, SourceWatermark, Statement,
};

mod agg;
mod cast;
mod expr;
pub use expr::print_function_table;

use crate::config::{Configuration, Feature, GenerateItem};

mod dml;
mod functions;
mod query;
mod relation;
mod scalar;
mod table_functions;
mod time_window;
mod types;
mod utils;

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_indices: Vec<usize>,
    pub is_base_table: bool,
    pub is_append_only: bool,
    pub source_watermarks: Vec<SourceWatermark>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            pk_indices: vec![],
            is_base_table: false,
            is_append_only: false,
            source_watermarks: vec![],
        }
    }

    pub fn new_for_base_table(
        name: String,
        columns: Vec<Column>,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        source_watermarks: Vec<SourceWatermark>,
    ) -> Self {
        Self {
            name,
            columns,
            pk_indices,
            is_base_table: true,
            is_append_only,
            source_watermarks,
        }
    }

    pub fn get_qualified_columns(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|c| {
                let mut name = c.name.clone();
                name.0.insert(0, Ident::new_unchecked(&self.name));
                Column {
                    name,
                    data_type: c.data_type.clone(),
                }
            })
            .collect()
    }
}

/// Sqlsmith Column definition
#[derive(Clone, Debug)]
pub struct Column {
    pub(crate) name: ObjectName,
    pub(crate) data_type: DataType,
}

impl From<ColumnDef> for Column {
    fn from(c: ColumnDef) -> Self {
        Self {
            name: ObjectName(vec![c.name]),
            data_type: bind_data_type(&c.data_type.expect("data type should not be none")).unwrap(),
        }
    }
}

impl Column {
    pub fn name_expr(&self) -> Expr {
        if self.name.0.len() == 1 {
            Expr::Identifier(self.name.0[0].clone())
        } else {
            Expr::CompoundIdentifier(self.name.0.clone())
        }
    }

    pub fn base_name(&self) -> Ident {
        self.name.0.last().unwrap().clone()
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
    pub fn new(can_agg: bool, inside_agg: bool) -> Self {
        SqlGeneratorContext {
            can_agg,
            inside_agg,
        }
    }

    pub fn is_inside_agg(self) -> bool {
        self.inside_agg
    }

    pub fn can_gen_agg(self) -> bool {
        self.can_agg && !self.inside_agg
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

    /// Configuration to control weight.
    config: Configuration,
    // /// Count number of subquery.
    // /// We don't want too many per query otherwise it is hard to debug.
    // with_statements: u64,
}

/// Generators
impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new(rng: &'a mut R, tables: Vec<Table>, config: Configuration) -> Self {
        SqlGenerator {
            tables,
            rng,
            relation_id: 0,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: false,
            recursion_weight: 0.3,
            config,
        }
    }

    pub(crate) fn new_for_mview(rng: &'a mut R, tables: Vec<Table>, config: Configuration) -> Self {
        // distinct aggregate is not allowed for MV
        SqlGenerator {
            tables,
            rng,
            relation_id: 0,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: true,
            recursion_weight: 0.3,
            config,
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

        // Randomly choose emit mode if allowed
        let emit_mode = if self.should_generate(Feature::Eowc) {
            Some(EmitMode::OnWindowClose)
        } else {
            None
        };

        let mview = Statement::CreateView {
            or_replace: false,
            materialized: true,
            if_not_exists: false,
            name,
            columns: vec![],
            query,
            with_options: vec![],
            emit_mode,
        };
        (mview, table)
    }

    /// 50/50 chance to be true/false.
    fn flip_coin(&mut self) -> bool {
        self.rng.random_bool(0.5)
    }

    /// Provide recursion bounds.
    pub(crate) fn can_recurse(&mut self) -> bool {
        if self.recursion_weight <= 0.0 {
            return false;
        }
        let can_recurse = self.rng.random_bool(self.recursion_weight);
        if can_recurse {
            self.recursion_weight *= 0.9;
            if self.recursion_weight < 0.05 {
                self.recursion_weight = 0.0;
            }
        }
        can_recurse
    }

    pub(crate) fn get_columns_with_watermark(&mut self, columns: &[Column]) -> Vec<Column> {
        let watermark_names: HashSet<_> = self
            .get_append_only_tables()
            .iter()
            .flat_map(|t| t.source_watermarks.iter().map(|wm| wm.column.real_value()))
            .collect();

        columns
            .iter()
            .filter(|c| watermark_names.contains(&c.name.base_name()))
            .cloned()
            .collect()
    }

    pub(crate) fn get_append_only_tables(&mut self) -> Vec<Table> {
        self.tables
            .iter()
            .filter(|t| t.is_append_only)
            .cloned()
            .collect()
    }

    /// Decide whether to generate on config.
    pub(crate) fn should_generate<T: Into<GenerateItem>>(&mut self, item: T) -> bool {
        self.config.should_generate(item, self.rng)
    }
}
