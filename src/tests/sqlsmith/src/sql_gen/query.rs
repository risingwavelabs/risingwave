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

//! Interface for generating a query
//! We construct Query based on the AST representation,
//! as defined in the [`risingwave_sqlparser`] module.
use std::vec;

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{
    Cte, Distinct, Expr, Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, TableWithJoins,
    With,
};

use crate::sql_gen::types::DataType;
use crate::sql_gen::utils::create_table_with_joins_from_table;
use crate::sql_gen::{Column, SqlGenerator, SqlGeneratorContext, Table};

/// Generators
impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// Generates query expression and returns its
    /// query schema as well.
    pub(crate) fn gen_query(&mut self) -> (Query, Vec<Column>) {
        if self.can_recurse() {
            self.gen_complex_query()
        } else {
            self.gen_simple_query()
        }
    }

    /// Generates a complex query which may recurse.
    /// e.g. through `gen_with` or other generated parts of the query.
    fn gen_complex_query(&mut self) -> (Query, Vec<Column>) {
        let (with, with_tables) = self.gen_with();
        let (query, schema) = self.gen_set_expr(with_tables);
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

    /// Generates a simple query which will not recurse.
    fn gen_simple_query(&mut self) -> (Query, Vec<Column>) {
        let with_tables = vec![];
        let (query, schema) = self.gen_set_expr(with_tables);
        (
            Query {
                with: None,
                body: query,
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            },
            schema,
        )
    }

    /// Generates a query with local context.
    /// Used by `WITH`, `Table Subquery` in Relation
    pub(crate) fn gen_local_query(&mut self) -> (Query, Vec<Column>) {
        let old_ctxt = self.new_local_context();
        let t = self.gen_query();
        self.restore_context(old_ctxt);
        t
    }

    /// Generates a query with correlated context to ensure proper recursion.
    /// Used by Exists `Subquery`
    /// TODO: <https://github.com/risingwavelabs/risingwave/pull/4431#issuecomment-1327417328>
    fn _gen_correlated_query(&mut self) -> (Query, Vec<Column>) {
        let old_ctxt = self._clone_local_context();
        let t = self.gen_query();
        self.restore_context(old_ctxt);
        t
    }

    fn gen_with(&mut self) -> (Option<With>, Vec<Table>) {
        match self.flip_coin() {
            true => (None, vec![]),
            false => {
                let (with, tables) = self.gen_with_inner();
                (Some(with), tables)
            }
        }
    }

    fn gen_with_inner(&mut self) -> (With, Vec<Table>) {
        let alias = self.gen_table_alias_with_prefix("with");
        let (query, query_schema) = self.gen_local_query();
        let from = None;
        let cte = Cte {
            alias: alias.clone(),
            query,
            from,
        };

        let with_tables = vec![Table {
            name: alias.name.real_value(),
            columns: query_schema,
        }];
        (
            With {
                recursive: false,
                cte_tables: vec![cte],
            },
            with_tables,
        )
    }

    fn gen_set_expr(&mut self, with_tables: Vec<Table>) -> (SetExpr, Vec<Column>) {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let (select, schema) = self.gen_select_stmt(with_tables);
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

    fn gen_limit(&mut self) -> Option<String> {
        if !self.is_mview && self.rng.gen_bool(0.2) {
            Some(self.rng.gen_range(0..=100).to_string())
        } else {
            None
        }
    }

    fn gen_select_stmt(&mut self, with_tables: Vec<Table>) -> (Select, Vec<Column>) {
        // Generate random tables/relations first so that select items can refer to them.
        let from = self.gen_from(with_tables);
        let selection = self.gen_where();
        let group_by = self.gen_group_by();
        let having = self.gen_having(!group_by.is_empty());
        let (select_list, schema) = self.gen_select_list();
        let select = Select {
            distinct: Distinct::All,
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
        let context = SqlGeneratorContext::new_with_can_agg(can_agg);
        (0..items_num)
            .map(|i| self.gen_select_item(i, context))
            .unzip()
    }

    fn gen_select_item(&mut self, i: i32, context: SqlGeneratorContext) -> (SelectItem, Column) {
        use DataType as T;
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
            T::Timestamptz,
            T::Time,
            T::Interval,
            T::StructOfInt,
            T::ListOfVarchar,
            T::ListOfInt,
        ]
        .choose(&mut self.rng)
        .unwrap();

        let expr = self.gen_expr(ret_type, context);

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

    fn gen_from(&mut self, with_tables: Vec<Table>) -> Vec<TableWithJoins> {
        let mut from = if !with_tables.is_empty() {
            let with_table = with_tables
                .choose(&mut self.rng)
                .expect("with tables should not be empty");
            vec![create_table_with_joins_from_table(with_table)]
        } else {
            let (rel, tables) = self.gen_from_relation();
            self.add_relations_to_context(tables);
            vec![rel]
        };

        if self.is_mview {
            // TODO: These constraints are workarounds required by mview.
            // Tracked by: <https://github.com/risingwavelabs/risingwave/issues/4024>.
            assert!(!self.tables.is_empty());
            return from;
        }
        let mut lateral_contexts = vec![];
        for _ in 0..self.tables.len() {
            if self.flip_coin() {
                let (table_with_join, mut table) = self.gen_from_relation();
                from.push(table_with_join);
                lateral_contexts.append(&mut table);
            }
        }
        self.add_relations_to_context(lateral_contexts);
        from
    }

    fn gen_where(&mut self) -> Option<Expr> {
        if self.flip_coin() {
            let context = SqlGeneratorContext::new_with_can_agg(false);
            Some(self.gen_expr(DataType::Boolean, context))
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
            let context = SqlGeneratorContext::new();
            Some(self.gen_expr(DataType::Boolean, context))
        } else {
            None
        }
    }
}
