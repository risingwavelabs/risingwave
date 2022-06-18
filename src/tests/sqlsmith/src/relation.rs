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

use crate::SqlGenerator;

impl<'a> SqlGenerator<'a> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> TableWithJoins {
        let alias = format!("t{}", self.bound_relations.len());
        match self.rng.gen_range(0..=9) {
            0..=9 => self.gen_simple_table(alias),
            _ => unreachable!(),
        }
    }

    fn gen_simple_table(&mut self, alias: String) -> TableWithJoins {
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
        self.bound_relations.push(table.clone());
        relation
    }
}
