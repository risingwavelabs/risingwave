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

use crate::{SqlGenerator, Table};

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> TableWithJoins {
        match self.rng.gen_range(0..=9) {
            0..=9 => self.gen_table(),
            // TODO: unreachable, should change to 9..=9,
            // but currently it will cause panic due to some wrong assertions.
            10..=10 => self.gen_subquery(),
            _ => unreachable!(),
        }
    }

    fn gen_table(&mut self) -> TableWithJoins {
        if self.can_recurse() {
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_table(),
                false => self.gen_join_table(),
            };
        }
        self.gen_simple_table() // use this as base-case
    }

    fn gen_simple_table(&mut self) -> TableWithJoins {
        let alias = format!("t{}", self.bound_relations.len());
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let relation = TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new(table.name.clone())]),
                alias: Some(TableAlias {
                    name: Ident::new(alias.clone()),
                    columns: vec![],
                }),
                args: vec![],
            },
            joins: vec![],
        };
        table.name = alias; // Rename the table.
        self.bound_relations.push(table);
        relation
    }

    fn gen_join_table(&mut self) -> TableWithJoins {
        todo!()
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
        self.bound_relations.push(table);
        relation
    }
}
