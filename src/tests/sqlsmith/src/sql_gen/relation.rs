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

use crate::sql_gen::types::BINARY_INEQUALITY_OP_TABLE;
use crate::sql_gen::{Column, SqlGenerator, SqlGeneratorContext};
use crate::{BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Table};

fn create_binary_expr(op: BinaryOperator, left: String, right: String) -> Expr {
    let left = Box::new(Expr::Identifier(Ident::new_unchecked(left)));
    let right = Box::new(Expr::Identifier(Ident::new_unchecked(right)));
    Expr::BinaryOp { left, op, right }
}

fn create_equi_expr(left: String, right: String) -> Expr {
    create_binary_expr(BinaryOperator::Eq, left, right)
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> (TableWithJoins, Vec<Table>) {
        let range = if self.can_recurse() { 1 } else { 10 };
        match self.rng.gen_range(0..=range) {
            0..=1 => self.gen_no_join(),
            2..=6 => self
                .gen_simple_join_clause()
                .unwrap_or_else(|| self.gen_no_join()),
            7..=10 => self.gen_more_joins(),
            // TODO(kwannoel): cycles, bushy joins.
            _ => unreachable!(),
        }
    }

    fn gen_no_join(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (relation, table) = self.gen_table_factor();
        (
            TableWithJoins {
                relation,
                joins: vec![],
            },
            vec![table],
        )
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Table) {
        let alias = self.gen_table_name_with_prefix("t");
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new_unchecked(&table.name)]),
            alias: Some(TableAlias {
                name: alias.as_str().into(),
                columns: vec![],
            }),
            for_system_time_as_of_proctime: false,
        };
        table.name = alias; // Rename the table.
        (table_factor, table)
    }

    fn gen_table_factor(&mut self) -> (TableFactor, Table) {
        let current_context = self.new_local_context();
        let factor = self.gen_table_factor_inner();
        self.restore_context(current_context);
        factor
    }

    /// Generates a table factor, and provides bound columns.
    /// Generated column names should be qualified by table name.
    fn gen_table_factor_inner(&mut self) -> (TableFactor, Table) {
        // TODO: TableFactor::Derived, TableFactor::TableFunction, TableFactor::NestedJoin
        match self.rng.gen_range(0..=2) {
            0 => self.gen_time_window_func(),
            1 => {
                if self.can_recurse() {
                    self.gen_table_subquery()
                } else {
                    self.gen_simple_table_factor()
                }
            }
            2 => self.gen_simple_table_factor(),
            _ => unreachable!(),
        }
    }

    fn gen_equi_join_columns(
        &mut self,
        left_columns: Vec<Column>,
        right_columns: Vec<Column>,
    ) -> Vec<(Column, Column)> {
        let mut available_join_on_columns = vec![];
        for left_column in &left_columns {
            for right_column in &right_columns {
                if left_column.data_type == right_column.data_type {
                    available_join_on_columns.push((left_column.clone(), right_column.clone()))
                }
            }
        }
        available_join_on_columns
    }

    fn gen_bool_with_tables(&mut self, tables: Vec<Table>) -> Expr {
        let old_context = self.new_local_context();
        self.add_relations_to_context(tables);
        let expr = self.gen_expr(&Boolean, SqlGeneratorContext::new_with_can_agg(false));
        self.restore_context(old_context);
        expr
    }

    fn gen_single_equi_join_expr(
        &mut self,
        left_columns: Vec<Column>,
        right_columns: Vec<Column>,
    ) -> Option<(Expr, Vec<(Column, Column)>)> {
        let mut available_join_on_columns = self.gen_equi_join_columns(left_columns, right_columns);
        if available_join_on_columns.is_empty() {
            return None;
        }
        available_join_on_columns.shuffle(&mut self.rng);
        let remaining_columns = available_join_on_columns.split_off(1);
        let (left_column, right_column) = available_join_on_columns.drain(..).next().unwrap();
        let join_on_expr = create_equi_expr(left_column.name, right_column.name);
        Some((join_on_expr, remaining_columns))
    }

    fn gen_non_equi_expr(&mut self, available_join_on_columns: Vec<(Column, Column)>) -> Expr {
        let expr = Expr::Value(Value::Boolean(true));
        if available_join_on_columns.is_empty() {
            return expr;
        }
        let n = self.rng.gen_range(0..available_join_on_columns.len());
        let mut count = 0;
        for (l_col, r_col) in available_join_on_columns {
            if count >= n {
                break;
            }
            let Some(inequality_ops) =
                BINARY_INEQUALITY_OP_TABLE.get(&(l_col.data_type, r_col.data_type))
            else {
                continue;
            };
            let inequality_op = inequality_ops.choose(&mut self.rng).unwrap();
            let _non_equi_expr = create_binary_expr(inequality_op.clone(), l_col.name, r_col.name);
            count += 1;
        }
        expr
    }

    fn gen_more_equi_join_exprs(
        &mut self,
        mut available_join_on_columns: Vec<(Column, Column)>,
    ) -> Expr {
        let mut expr = Expr::Value(Value::Boolean(true));
        if available_join_on_columns.is_empty() {
            return expr;
        }
        let n_join_cols = available_join_on_columns.len();
        let n = if n_join_cols < 2 {
            n_join_cols
        } else {
            match self.rng.gen_range(0..100) {
                0..=10 => self.rng.gen_range(n_join_cols / 2..n_join_cols),
                11..=100 => self.rng.gen_range(0..n_join_cols / 2),
                _ => unreachable!(),
            }
        };

        for (l_col, r_col) in available_join_on_columns.drain(0..n) {
            let equi_expr = create_equi_expr(l_col.name, r_col.name);
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: Box::new(equi_expr),
            }
        }
        expr
    }

    fn gen_arbitrary_bool(&mut self, left_table: Table, right_table: Table) -> Option<Expr> {
        let expr = self.gen_bool_with_tables(vec![left_table, right_table]);

        // FIXME(noel): This is a hack to reduce streaming nested loop join occurrences.
        // ... JOIN ON x=y AND false => ... JOIN ON x=y
        // We can use const folding, then remove the right expression,
        // if it evaluates to `false` after const folding.
        // Have to first bind `Expr`, since it is AST form.
        // Then if successfully bound, use `eval_row_const` to constant fold it.
        // Take a look at <https://github.com/risingwavelabs/risingwave/pull/7541/files#diff-08400d774a613753da25dcb45e905e8fe3d20acaccca846f39a86834f4c01656>.
        if expr != Expr::Value(Value::Boolean(false)) {
            Some(expr)
        } else {
            None
        }
    }

    /// Generates the `ON` clause in `t JOIN t2 ON ...`
    /// It will generate at least one equi join condition
    /// This will reduce chance of nested loop join from being generated.
    fn gen_join_on_expr(
        &mut self,
        left_columns: Vec<Column>,
        left_table: Table,
        right_columns: Vec<Column>,
        right_table: Table,
    ) -> Option<Expr> {
        // We always generate an equi join, to avoid stream nested loop join.
        let Some((base_join_on_expr, remaining_equi_columns)) =
            self.gen_single_equi_join_expr(left_columns, right_columns)
        else {
            return None;
        };

        // Add more expressions
        let extra_expr = match self.rng.gen_range(1..=100) {
            1..=25 => None,
            26..=50 => Some(self.gen_non_equi_expr(remaining_equi_columns)),
            51..=75 => Some(self.gen_more_equi_join_exprs(remaining_equi_columns)),
            76..=100 => self.gen_arbitrary_bool(left_table, right_table),
            _ => unreachable!(),
        };
        if let Some(extra_expr) = extra_expr {
            Some(Expr::BinaryOp {
                left: Box::new(base_join_on_expr),
                op: BinaryOperator::And,
                right: Box::new(extra_expr),
            })
        } else {
            Some(base_join_on_expr)
        }
    }

    fn gen_join_constraint(
        &mut self,
        left_columns: Vec<Column>,
        left_table: Table,
        right_columns: Vec<Column>,
        right_table: Table,
    ) -> Option<JoinConstraint> {
        let expr = self.gen_join_on_expr(left_columns, left_table, right_columns, right_table)?;
        Some(JoinConstraint::On(expr))
    }

    /// Generates t1 JOIN t2 ON ...
    fn gen_join_operator(
        &mut self,
        left_columns: Vec<Column>,
        left_table: Table,
        right_columns: Vec<Column>,
        right_table: Table,
    ) -> Option<JoinOperator> {
        let Some(join_constraint) =
            self.gen_join_constraint(left_columns, left_table, right_columns, right_table)
        else {
            return None;
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

        Some(join_operator)
    }

    /// Generates t1 JOIN t2 ON ...
    fn gen_simple_join_clause(&mut self) -> Option<(TableWithJoins, Vec<Table>)> {
        let (left_factor, left_table) = self.gen_table_factor();
        let left_columns = left_table.get_qualified_columns();
        let (right_factor, right_table) = self.gen_table_factor();
        let right_columns = right_table.get_qualified_columns();
        let Some(join_operator) = self.gen_join_operator(
            left_columns,
            left_table.clone(),
            right_columns,
            right_table.clone(),
        ) else {
            return None;
        };

        let right_factor_with_join = Join {
            relation: right_factor,
            join_operator,
        };
        Some((
            TableWithJoins {
                relation: left_factor,
                joins: vec![right_factor_with_join],
            },
            vec![left_table, right_table],
        ))
    }

    fn gen_more_joins(&mut self) -> (TableWithJoins, Vec<Table>) {
        // gen left
        let Some((left_table_with_join, mut left_tables)) = self.gen_simple_join_clause() else {
            return self.gen_no_join();
        };
        let left_columns = left_tables
            .iter()
            .flat_map(|t| t.get_qualified_columns())
            .collect();

        // gen right
        let (right_factor, right_table) = self.gen_table_factor();
        let right_columns = right_table.get_qualified_columns();

        // gen join
        let left_table = left_tables.choose(&mut self.rng).unwrap();
        let Some(join_operator) = self.gen_join_operator(
            left_columns,
            left_table.clone(),
            right_columns,
            right_table.clone(),
        ) else {
            return (left_table_with_join, left_tables);
        };

        // build result
        let mut tables = vec![];
        tables.append(&mut left_tables);
        tables.push(right_table);

        let right_join = Join {
            relation: right_factor,
            join_operator,
        };

        (
            TableWithJoins {
                relation: TableFactor::NestedJoin(Box::new(left_table_with_join)),
                joins: vec![right_join],
            },
            tables,
        )
    }

    fn gen_table_subquery(&mut self) -> (TableFactor, Table) {
        let (subquery, columns) = self.gen_local_query();
        let alias = self.gen_table_name_with_prefix("sq");
        let table = Table::new(alias.clone(), columns);
        let factor = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(subquery),
            alias: Some(TableAlias {
                name: Ident::new_unchecked(alias),
                columns: vec![],
            }),
        };

        (factor, table)
    }
}
