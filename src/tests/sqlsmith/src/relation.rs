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

use crate::Column;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};

use crate::{SqlGenerator, Table};

impl<'a, R: Rng> SqlGenerator<'a, R> {

    pub(crate) fn gen_from_relation(&mut self) -> TableWithJoins {
        let (from_relation, _) = self.gen_from_relation_with_cols();
        from_relation
    }

    /// A relation specified in the FROM clause.
    fn gen_from_relation_with_cols(&mut self) -> (TableWithJoins, Vec<Column>) {
        if self.can_recurse() {
            return match self.rng.gen_range(0..=9) {
                0..=8 => self.gen_simple_table(),
                9..=9 => self.gen_equijoin_expr(),
                // TODO: unreachable, should change to 9..=9,
                // but currently it will cause panic due to some wrong assertions.
                10..=10 => self.gen_subquery(),
                _ => unreachable!(),
            };
        }

        self.gen_simple_table()
    }

    fn gen_simple_table(&mut self) -> (TableWithJoins, Vec<Column>) {
        let (relation, columns) = self.gen_simple_table_factor();
        let simple_table = TableWithJoins {
            relation,
            joins: vec![],
        };
        (simple_table, columns)
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Vec<Column>) {
        let alias = format!("t{}", self.bound_relations.len());
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new(table.name.clone())]),
            alias: Some(TableAlias {
                name: Ident::new(alias.clone()),
                columns: vec![],
            }),
            args: vec![],
        };
        table.name = alias; // Rename the table.
        let columns = table.columns.clone();
        self.bound_relations.push(table);
        (table_factor, columns)
    }

    fn gen_nested_join_factor(&mut self) -> (TableFactor, Vec<Column>) {
        todo!()
    }

    fn gen_table_factor(&mut self) -> (TableFactor, Vec<Column>) {
        match self.rng.gen_range(0..3) {
            0 => self.gen_simple_table_factor(),
            // TODO: TableFactor::Derived, TableFactor::TableFunction
            _ => self.gen_nested_join_factor(),
        }
    }

    fn gen_equijoin_expr(&mut self) -> (TableWithJoins, Vec<Column>) {
        let left = self.gen_table_factor();
        let right = self.gen_table_factor();
        todo!()
        // find available columns to join on.
        // shuffle, pick columns to join on
    }

    fn gen_subquery(&mut self) -> (TableWithJoins, Vec<Column>) {
        let (subquery, columns) = self.gen_query();
        let alias = format!("t{}", self.bound_relations.len());
        let table = Table {
            name: alias.clone(),
            columns: columns.clone(),
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
        self.bound_relations.push(table);
        (relation, columns)
    }
}
