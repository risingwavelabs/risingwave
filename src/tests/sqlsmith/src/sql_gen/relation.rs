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

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataType::Boolean;
use risingwave_sqlparser::ast::{
    Ident, ObjectName, TableAlias, TableFactor, TableWithJoins, Value,
};

use crate::sql_gen::{Column, SqlGenerator, SqlGeneratorContext};
use crate::{BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Table};

fn create_equi_expr(left: String, right: String) -> Expr {
    let left = Box::new(Expr::Identifier(Ident::new_unchecked(left)));
    let right = Box::new(Expr::Identifier(Ident::new_unchecked(right)));
    Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> (TableWithJoins, Vec<Table>) {
        let range = if self.can_recurse() { 3 } else { 4 };
        match self.rng.gen_range(0..=range) {
            0..=0 => self.gen_simple_table(),
            1..=1 => self.gen_time_window_func(),
            2..=3 => self.gen_join_clause(),
            4..=4 => self.gen_table_subquery(),
            _ => unreachable!(),
        }
    }

    fn gen_simple_table(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (relation, _, table) = self.gen_simple_table_factor();

        (
            TableWithJoins {
                relation,
                joins: vec![],
            },
            table,
        )
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let alias = self.gen_table_name_with_prefix("t");
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new_unchecked(&table.name)]),
            alias: Some(TableAlias {
                name: alias.as_str().into(),
                columns: vec![],
            }),
        };
        table.name = alias; // Rename the table.
        let columns = table.get_qualified_columns();
        (table_factor, columns, vec![table])
    }

    fn gen_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let current_context = self.new_local_context();
        let factor = self.gen_table_factor_inner();
        self.restore_context(current_context);
        factor
    }

    /// Generates a table factor, and provides bound columns.
    /// Generated column names should be qualified by table name.
    fn gen_table_factor_inner(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        // TODO: TableFactor::Derived, TableFactor::TableFunction, TableFactor::NestedJoin
        self.gen_simple_table_factor()
    }

    /// TODO:
    /// Generate equi join with columns not of the same type,
    /// use functions to transform to same type.
    fn gen_equi_join_columns(
        &mut self,
        left_columns: Vec<Column>,
        right_columns: Vec<Column>,
    ) -> Option<(Column, Column)> {
        let mut available_join_on_columns = vec![];
        for left_column in &left_columns {
            for right_column in &right_columns {
                if left_column.data_type == right_column.data_type {
                    available_join_on_columns.push((left_column, right_column))
                }
            }
        }
        if available_join_on_columns.is_empty() {
            return None;
        }
        let i = self.rng.gen_range(0..available_join_on_columns.len());
        let (left_column, right_column) = available_join_on_columns[i];
        Some((left_column.clone(), right_column.clone()))
    }

    /// Generates the `ON` clause in `t JOIN t2 ON ...`
    /// It will generate at least one equi join condition
    /// This will reduce chance of nested loop join from being generated.
    /// TODO: Generate equi-join on different types.
    fn gen_join_on_expr(
        &mut self,
        left_columns: Vec<Column>,
        left_table: Vec<Table>,
        right_columns: Vec<Column>,
        right_table: Vec<Table>,
    ) -> Option<Expr> {
        // We always generate an equi join, to avoid stream nested loop join.
        let Some((l, r)) = self.gen_equi_join_columns(left_columns, right_columns) else {
            return None;
        };
        let mut join_on_expr = create_equi_expr(l.name, r.name);
        // Add extra boolean expressions
        if self.flip_coin() {
            let old_context = self.new_local_context();
            self.add_relations_to_context(left_table);
            self.add_relations_to_context(right_table);
            let expr = self.gen_expr(&Boolean, SqlGeneratorContext::new_with_can_agg(false));
            self.restore_context(old_context);
            // FIXME(noel): Hack to reduce streaming nested loop join occurrences.
            // ... JOIN ON x=y AND false => ... JOIN ON x=y
            // We can use const folding, then remove the right expression,
            // if it evaluates to `false` after const folding.
            if expr != Expr::Value(Value::Boolean(false)) {
                join_on_expr = Expr::BinaryOp {
                    left: Box::new(join_on_expr),
                    op: BinaryOperator::And,
                    right: Box::new(expr),
                }
            }
        }
        Some(join_on_expr)
    }

    fn gen_join_constraint(
        &mut self,
        left_columns: Vec<Column>,
        left_table: Vec<Table>,
        right_columns: Vec<Column>,
        right_table: Vec<Table>,
    ) -> Option<JoinConstraint> {
        let expr = self.gen_join_on_expr(left_columns, left_table, right_columns, right_table)?;
        Some(JoinConstraint::On(expr))
    }

    /// Generates t1 JOIN t2 ON ...
    fn gen_join_clause(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (left_factor, left_columns, mut left_table) = self.gen_table_factor();
        let (right_factor, right_columns, mut right_table) = self.gen_table_factor();

        let join_constraint = self.gen_join_constraint(
            left_columns,
            left_table.clone(),
            right_columns,
            right_table.clone(),
        );
        let Some(join_constraint) = join_constraint else {
            return self.gen_simple_table();
        };

        // NOTE: INNER JOIN works fine, usually does not encounter `StreamNestedLoopJoin` much.
        // If many failures due to `StreamNestedLoopJoin`, try disable the others.
        let join_operator = match self.rng.gen_range(0..=3) {
            0 => JoinOperator::Inner(join_constraint),
            1 => JoinOperator::LeftOuter(join_constraint),
            2 => JoinOperator::RightOuter(join_constraint),
            _ => JoinOperator::FullOuter(join_constraint),
            // NOTE: Do not generate CrossJoin,
            // it has been already generated in query.
            // _ => JoinOperator::CrossJoin,
        };

        let right_factor_with_join = Join {
            relation: right_factor,
            join_operator,
        };
        // TODO: Different structures of joins (bushy, left, right, cycles)
        left_table.append(&mut right_table);
        (
            TableWithJoins {
                relation: left_factor,
                joins: vec![right_factor_with_join],
            },
            left_table,
        )
    }

    fn gen_table_subquery(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (subquery, columns) = self.gen_local_query();
        let alias = self.gen_table_name_with_prefix("sq");
        let table = Table {
            name: alias.clone(),
            columns,
        };
        let relation = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(subquery),
                alias: Some(TableAlias {
                    name: Ident::new_unchecked(alias),
                    columns: vec![],
                }),
            },
            joins: vec![],
        };

        (relation, vec![table])
    }
}
