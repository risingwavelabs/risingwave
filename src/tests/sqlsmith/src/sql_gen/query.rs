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

//! Interface for generating a query
//! We construct Query based on the AST representation,
//! as defined in the [`risingwave_sqlparser`] module.

use std::vec;

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{
    Cte, Distinct, Expr, Ident, Query, Select, SelectItem, SetExpr, TableWithJoins, With,
};

use crate::sql_gen::utils::create_table_with_joins_from_table;
use crate::sql_gen::{Column, SqlGenerator, SqlGeneratorContext, Table};

/// Generators
impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// Generates query expression and returns its
    /// query schema as well.
    pub(crate) fn gen_query(&mut self) -> (Query, Vec<Column>) {
        if self.rng.gen_bool(0.3) {
            self.gen_complex_query()
        } else {
            self.gen_simple_query()
        }
    }

    /// Generates a complex query which may recurse.
    /// e.g. through `gen_with` or other generated parts of the query.
    fn gen_complex_query(&mut self) -> (Query, Vec<Column>) {
        let num_select_items = self.rng.gen_range(1..=4);
        let (with, with_tables) = self.gen_with();
        let (query, schema) = self.gen_set_expr(with_tables, num_select_items);
        let order_by = self.gen_order_by();
        let has_order_by = !order_by.is_empty();
        (
            Query {
                with,
                body: query,
                order_by,
                limit: self.gen_limit(has_order_by),
                offset: None,
                fetch: None,
            },
            schema,
        )
    }

    /// This query can still recurse, but it is "simpler"
    /// does not have "with" clause, "order by".
    /// Which makes it more unlikely to recurse.
    fn gen_simple_query(&mut self) -> (Query, Vec<Column>) {
        let num_select_items = self.rng.gen_range(1..=4);
        let with_tables = vec![];
        let (query, schema) = self.gen_set_expr(with_tables, num_select_items);
        (
            Query {
                with: None,
                body: query,
                order_by: vec![],
                limit: self.gen_limit(false),
                offset: None,
                fetch: None,
            },
            schema,
        )
    }

    /// Generates a query with a single SELECT item. e.g. SELECT v from t;
    /// Returns the query and the SELECT column alias.
    pub(crate) fn gen_single_item_query(&mut self) -> (Query, Column) {
        let with_tables = vec![];
        let (query, schema) = self.gen_set_expr(with_tables, 1);
        (
            Query {
                with: None,
                body: query,
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            },
            schema[0].clone(),
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
    pub(crate) fn gen_correlated_query(&mut self) -> (Query, Vec<Column>) {
        let old_ctxt = self.clone_local_context();
        let t = self.gen_query();
        self.restore_context(old_ctxt);
        t
    }

    fn gen_with(&mut self) -> (Option<With>, Vec<Table>) {
        match self.can_recurse() {
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

        let with_tables = vec![Table::new(alias.name.real_value(), query_schema)];
        (
            With {
                recursive: false,
                cte_tables: vec![cte],
            },
            with_tables,
        )
    }

    fn gen_set_expr(
        &mut self,
        with_tables: Vec<Table>,
        num_select_items: usize,
    ) -> (SetExpr, Vec<Column>) {
        match self.rng.gen_range(0..=9) {
            // TODO: Generate other `SetExpr`
            0..=9 => {
                let (select, schema) = self.gen_select_stmt(with_tables, num_select_items);
                (SetExpr::Select(Box::new(select)), schema)
            }
            _ => unreachable!(),
        }
    }

    fn gen_limit(&mut self, has_order_by: bool) -> Option<String> {
        if (!self.is_mview || has_order_by) && self.flip_coin() {
            Some(self.rng.gen_range(0..=100).to_string())
        } else {
            None
        }
    }

    fn gen_select_stmt(
        &mut self,
        with_tables: Vec<Table>,
        num_select_items: usize,
    ) -> (Select, Vec<Column>) {
        // Generate random tables/relations first so that select items can refer to them.
        let from = self.gen_from(with_tables);
        let selection = self.gen_where();
        let group_by = self.gen_group_by();
        let having = self.gen_having(!group_by.is_empty());
        let (select_list, schema) = self.gen_select_list(num_select_items);
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

    fn gen_select_list(&mut self, num_select_items: usize) -> (Vec<SelectItem>, Vec<Column>) {
        let can_agg = self.flip_coin();
        let context = SqlGeneratorContext::new_with_can_agg(can_agg);
        (0..num_select_items)
            .map(|i| self.gen_select_item(i, context))
            .unzip()
    }

    fn gen_select_item(&mut self, i: usize, context: SqlGeneratorContext) -> (SelectItem, Column) {
        let (ret_type, expr) = self.gen_arbitrary_expr(context);

        let alias = format!("col_{}", i);
        (
            SelectItem::ExprWithAlias {
                expr,
                alias: Ident::new_unchecked(alias.clone()),
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

        // We short-circuit here for mview to avoid streaming nested loop join,
        // since CROSS JOIN below could be correlated.
        if self.is_mview {
            assert!(!self.tables.is_empty());
            return from;
        }

        // Generate one cross join at most.
        let mut lateral_contexts = vec![];
        if self.rng.gen_bool(0.3) {
            let (table_with_join, mut table) = self.gen_from_relation();
            from.push(table_with_join);
            lateral_contexts.append(&mut table);
        }
        self.add_relations_to_context(lateral_contexts);
        from
    }

    fn gen_where(&mut self) -> Option<Expr> {
        if self.flip_coin() {
            let context = SqlGeneratorContext::new_with_can_agg(false);
            Some(self.gen_expr(&DataType::Boolean, context))
        } else {
            None
        }
    }

    /// GROUP BY will constrain the generated columns.
    fn gen_group_by(&mut self) -> Vec<Expr> {
        let mut available = self.bound_columns.clone();
        if !available.is_empty() {
            available.shuffle(self.rng);
            let upper_bound = (available.len() + 1) / 2;
            let n = self.rng.gen_range(1..=upper_bound);
            let group_by_cols = available.drain(..n).collect_vec();
            self.bound_columns = group_by_cols.clone();
            group_by_cols
                .into_iter()
                .map(|c| Expr::Identifier(Ident::new_unchecked(c.name)))
                .collect_vec()
        } else {
            vec![]
        }
    }

    fn gen_having(&mut self, have_group_by: bool) -> Option<Expr> {
        if have_group_by & self.flip_coin() {
            let context = SqlGeneratorContext::new();
            Some(self.gen_expr(&DataType::Boolean, context))
        } else {
            None
        }
    }
}
