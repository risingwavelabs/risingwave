// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem;

use rand::Rng;
use risingwave_sqlparser::ast::{
    FunctionArg, FunctionArgExpr, TableAlias, TableFactor, TableWithJoins,
};

use crate::{Column, Expr, Ident, ObjectName, SqlGenerator, Table};

type Context = (Vec<Column>, Vec<Table>);

/// Context utils
impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new_local_ctxt(&mut self) -> Context {
        let current_bound_relations = mem::take(&mut self.bound_relations);
        let current_bound_columns = mem::take(&mut self.bound_columns);
        (current_bound_columns, current_bound_relations)
    }

    pub(crate) fn restore_ctxt(&mut self, (old_cols, old_rels): Context) {
        self.bound_relations = old_rels;
        self.bound_columns = old_cols;
    }
}

/// Gen utils
impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn create_table_name_with_prefix(&self, prefix: &str) -> String {
        format!("{}_{}", prefix, &self.bound_relations.len())
    }

    pub(crate) fn gen_alias_with_prefix(&self, prefix: &str) -> TableAlias {
        let name = &self.create_table_name_with_prefix(prefix);
        create_alias(name)
    }
}

pub(crate) fn create_table_factor_from_table(table: &Table) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![Ident::new(&table.name)]),
        alias: None,
        args: vec![],
    }
}

pub(crate) fn create_table_with_joins_from_table(table: &Table) -> TableWithJoins {
    TableWithJoins {
        relation: create_table_factor_from_table(table),
        joins: vec![],
    }
}

pub(crate) fn create_table_alias(table_name: &str) -> TableAlias {
    TableAlias {
        name: table_name.into(),
        columns: vec![],
    }
}

pub(crate) fn create_args(arg_exprs: Vec<Expr>) -> Vec<FunctionArg> {
    arg_exprs
        .into_iter()
        .map(create_function_arg_from_expr)
        .collect()
}

/// Create `FunctionArg` from an `Expr`.
fn create_function_arg_from_expr(expr: Expr) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
}
