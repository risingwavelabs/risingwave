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

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};

use crate::{
    BinaryOperator, Column, Expr, Join, JoinConstraint, JoinOperator, SqlGenerator, Table,
};

fn create_join_on_clause(left: String, right: String) -> Expr {
    let left = Box::new(Expr::Identifier(Ident::new(left)));
    let right = Box::new(Expr::Identifier(Ident::new(right)));
    Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> TableWithJoins {
        match self.rng.gen_range(0..=9) {
            0..=9 => self.gen_simple_table(),
            // TODO: Enable after resolving: <https://github.com/singularity-data/risingwave/issues/2771>.
            10..=10 => self.gen_equijoin_clause(),
            // TODO: Currently `gen_subquery` will cause panic due to some wrong assertions.
            11..=11 => self.gen_subquery(),
            _ => unreachable!(),
        }
    }

    fn gen_simple_table(&mut self) -> TableWithJoins {
        let (relation, _) = self.gen_simple_table_factor();

        TableWithJoins {
            relation,
            joins: vec![],
        }
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Vec<Column>) {
        let alias = format!("t{}", self.bound_relations.len());
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new(&table.name)]),
            alias: Some(TableAlias {
                name: Ident::new(alias.clone()),
                columns: vec![],
            }),
            args: vec![],
        };
        table.name = alias; // Rename the table.
        let columns = table.get_qualified_columns();
        self.add_relation_to_context(table);
        (table_factor, columns)
    }

    /// Generates a table factor, and provides bound columns.
    /// Generated column names should be qualified by table name.
    fn gen_table_factor(&mut self) -> (TableFactor, Vec<Column>) {
        // TODO: TableFactor::Derived, TableFactor::TableFunction, TableFactor::NestedJoin
        self.gen_simple_table_factor()
    }

    fn gen_equijoin_clause(&mut self) -> TableWithJoins {
        let (left_factor, left_columns) = self.gen_table_factor();
        let (right_factor, right_columns) = self.gen_table_factor();

        let mut available_join_on_columns = vec![];
        for left_column in &left_columns {
            for right_column in &right_columns {
                // NOTE: We can support some composite types if we wish to in the future.
                // see: https://www.postgresql.org/docs/14/functions-comparison.html.
                // For simplicity only support scalar types for now.
                let left_ty = left_column.data_type;
                let right_ty = right_column.data_type;
                if left_ty.is_scalar() && right_ty.is_scalar() && (left_ty == right_ty) {
                    available_join_on_columns.push((left_column, right_column))
                }
            }
        }
        let i = self.rng.gen_range(0..available_join_on_columns.len());
        let (left_column, right_column) = available_join_on_columns[i];
        let join_on_expr =
            create_join_on_clause(left_column.name.clone(), right_column.name.clone());

        let right_factor_with_join = Join {
            relation: right_factor,
            join_operator: JoinOperator::Inner(JoinConstraint::On(join_on_expr)),
        };

        TableWithJoins {
            relation: left_factor,
            joins: vec![right_factor_with_join],
        }
    }

    fn gen_subquery(&mut self) -> TableWithJoins {
        let (subquery, columns) = self.gen_query();
        let alias = format!("t{}", self.bound_relations.len());
        let table = Table {
            name: alias.clone(),
            columns,
        };
        let relation = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(subquery),
                alias: Some(TableAlias {
                    name: Ident::new(alias),
                    columns: vec![],
                }),
            },
            joins: vec![],
        };
        self.add_relation_to_context(table);
        relation
    }
}
