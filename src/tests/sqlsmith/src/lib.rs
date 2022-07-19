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

use std::vec;

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_frontend::binder::bind_data_type;
use risingwave_frontend::expr::DataTypeName;
use risingwave_sqlparser::ast::{
    BinaryOperator, ColumnDef, Expr, Ident, Join, JoinConstraint, JoinOperator, ObjectName,
    OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableWithJoins, Value, With,
};

mod expr;
pub use expr::print_function_table;
mod relation;
mod scalar;
mod time_window;

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }

    pub fn get_qualified_columns(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|c| Column {
                name: format!("{}.{}", self.name, c.name),
                data_type: c.data_type,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    data_type: DataTypeName,
}

impl From<ColumnDef> for Column {
    fn from(c: ColumnDef) -> Self {
        Self {
            name: c.name.value.clone(),
            data_type: bind_data_type(&c.data_type).unwrap().into(),
        }
    }
}

struct SqlGenerator<'a, R: Rng> {
    tables: Vec<Table>,
    rng: &'a mut R,

    /// is_distinct_allowed - Distinct and Orderby/Approx.. cannot be generated together among agg
    ///                       having and
    /// When this variable is true, it means distinct only
    /// When this variable is false, it means orderby and approx only.
    is_distinct_allowed: bool,

    /// Relations bound in generated query.
    /// We might not read from all tables.
    bound_relations: Vec<Table>,

    /// Columns bound in generated query.
    /// May not contain all columns from Self::bound_relations.
    /// e.g. GROUP BY clause will constrain bound_columns.
    bound_columns: Vec<Column>,

    /// SqlGenerator can be used in two execution modes:
    /// 1. Generating Query Statements.
    /// 2. Generating queries for CREATE MATERIALIZED VIEW.
    ///    Under this mode certain restrictions and workarounds are applied
    ///    for unsupported stream executors.
    is_mview: bool,
}

/// Generators
impl<'a, R: Rng> SqlGenerator<'a, R> {
    fn new(rng: &'a mut R, tables: Vec<Table>) -> Self {
        let is_distinct_allowed = rng.gen_bool(0.5);
        SqlGenerator {
            tables,
            rng,
            is_distinct_allowed,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: false,
        }
    }

    fn new_for_mview(rng: &'a mut R, tables: Vec<Table>) -> Self {
        // distinct aggregate is not allowed for MV
        SqlGenerator {
            tables,
            rng,
            is_distinct_allowed: false,
            bound_relations: vec![],
            bound_columns: vec![],
            is_mview: true,
        }
    }

    fn add_relation_to_context(&mut self, table: Table) {
        let mut bound_columns = table.get_qualified_columns();
        self.bound_columns.append(&mut bound_columns);
        self.bound_relations.push(table);
    }

    fn gen_stmt(&mut self) -> Statement {
        let (query, _) = self.gen_query();
        Statement::Query(Box::new(query))
    }

    pub fn gen_mview(&mut self, name: &str) -> (Statement, Table) {
        let (query, schema) = self.gen_query();
        let query = Box::new(query);
        let table = Table {
            name: name.to_string(),
            columns: schema,
        };
        let name = ObjectName(vec![Ident::new(name)]);
        let mview = Statement::CreateView {
            or_replace: false,
            materialized: true,
            name,
            columns: vec![],
            query,
            with_options: vec![],
        };
        (mview, table)
    }

    fn gen_query(&mut self) -> (Query, Vec<Column>) {
        let with = self.gen_with();
        let (query, schema) = self.gen_set_expr();
        (
            Query {
                with,
                body: query,
                order_by: self.gen_order_by(),
                limit: self.gen_limit(),
                offset: None,
                fetch: None,
            },
            schema,
        )
    }

    fn gen_with(&mut self) -> Option<With> {
        None
    }

    fn gen_set_expr(&mut self) -> (SetExpr, Vec<Column>) {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let (select, schema) = self.gen_select_stmt();
                (SetExpr::Select(Box::new(select)), schema)
            }
            _ => unreachable!(),
        }
    }

    fn gen_order_by(&mut self) -> Vec<OrderByExpr> {
        if self.bound_columns.is_empty() || !self.is_distinct_allowed {
            return vec![];
        }
        let mut order_by = vec![];
        while self.flip_coin() {
            let column = self.bound_columns.choose(&mut self.rng).unwrap();
            order_by.push(OrderByExpr {
                expr: Expr::Identifier(Ident::new(&column.name)),
                asc: Some(self.rng.gen_bool(0.5)),
                nulls_first: None,
            })
        }
        order_by
    }

    fn gen_limit(&mut self) -> Option<Expr> {
        if !self.is_mview && self.rng.gen_bool(0.2) {
            Some(Expr::Value(Value::Number(
                self.rng.gen_range(0..=100).to_string(),
                false,
            )))
        } else {
            None
        }
    }

    fn gen_select_stmt(&mut self) -> (Select, Vec<Column>) {
        // Generate random tables/relations first so that select items can refer to them.
        let from = self.gen_from();
        let selection = self.gen_where();
        let group_by = self.gen_group_by();
        let having = self.gen_having(!group_by.is_empty());
        let (select_list, schema) = self.gen_select_list();
        let select = Select {
            distinct: false,
            projection: select_list,
            from,
            lateral_views: vec![],
            selection,
            group_by,
            having,
        };
        (select, schema)
    }

    fn gen_select_list(&mut self) -> (Vec<SelectItem>, Vec<Column>) {
        let items_num = self.rng.gen_range(1..=4);
        let can_agg = self.flip_coin();
        (0..items_num)
            .map(|i| self.gen_select_item(i, can_agg))
            .unzip()
    }

    fn gen_select_item(&mut self, i: i32, can_agg: bool) -> (SelectItem, Column) {
        use DataTypeName as T;
        let ret_type = *[
            T::Boolean,
            T::Int16,
            T::Int32,
            T::Int64,
            T::Decimal,
            T::Float32,
            T::Float64,
            T::Varchar,
            T::Date,
            T::Timestamp,
            T::Timestampz,
            T::Time,
            T::Interval,
        ]
        .choose(&mut self.rng)
        .unwrap();
        let expr = self.gen_expr(ret_type, can_agg, false);

        let alias = format!("col_{}", i);
        (
            SelectItem::ExprWithAlias {
                expr,
                alias: Ident::new(alias.clone()),
            },
            Column {
                name: alias,
                data_type: ret_type,
            },
        )
    }

    fn gen_from(&mut self) -> Vec<TableWithJoins> {
        if self.is_mview {
            assert!(!self.tables.is_empty());
            return vec![self.gen_from_relation()];
        }
        let mut from = vec![];
        for _ in 0..self.tables.len() {
            if self.flip_coin() {
                from.push(self.gen_from_relation());
            }
        }
        from
    }

    fn gen_where(&mut self) -> Option<Expr> {
        if self.flip_coin() {
            let can_agg = false;
            let inside_agg = false;
            Some(self.gen_expr(DataTypeName::Boolean, can_agg, inside_agg))
        } else {
            None
        }
    }

    fn gen_group_by(&mut self) -> Vec<Expr> {
        let mut available = self.bound_columns.clone();
        if !available.is_empty() {
            available.shuffle(self.rng);
            let n_group_by_cols = self.rng.gen_range(1..=available.len());
            let group_by_cols = available.drain(0..n_group_by_cols).collect_vec();
            self.bound_columns = group_by_cols.clone();
            group_by_cols
                .into_iter()
                .map(|c| Expr::Identifier(Ident::new(c.name)))
                .collect_vec()
        } else {
            vec![]
        }
    }

    fn gen_having(&mut self, have_group_by: bool) -> Option<Expr> {
        if have_group_by & self.flip_coin() {
            let can_agg = true;
            let inside_agg = false;
            Some(self.gen_expr(DataTypeName::Boolean, can_agg, inside_agg))
        } else {
            None
        }
    }

    /// 50/50 chance to be true/false.
    fn flip_coin(&mut self) -> bool {
        self.rng.gen_bool(0.5)
    }

    /// Provide recursion bounds.
    pub(crate) fn can_recurse(&mut self) -> bool {
        self.rng.gen_bool(0.3)
    }
}

/// Generate a random SQL string.
pub fn sql_gen(rng: &mut impl Rng, tables: Vec<Table>) -> String {
    let mut gen = SqlGenerator::new(rng, tables);
    format!("{}", gen.gen_stmt())
}

/// Generate a random CREATE MATERIALIZED VIEW sql string.
/// These are derived from `tables`.
pub fn mview_sql_gen<R: Rng>(rng: &mut R, tables: Vec<Table>, name: &str) -> (String, Table) {
    let mut gen = SqlGenerator::new_for_mview(rng, tables);
    let (mview, table) = gen.gen_mview(name);
    (mview.to_string(), table)
}
