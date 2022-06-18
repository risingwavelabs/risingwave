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
use risingwave_sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};

use crate::{SqlGenerator, Table};

impl<'a> SqlGenerator<'a> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> TableWithJoins {
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let alias = format!("t{}", self.bound_relations.len());
        let relation = make_table_relation(&table, alias.clone());
        table.name = alias; // Rename the table.
        self.bound_relations.push(table);
        relation
    }
}

fn make_table_relation(table: &Table, alias: String) -> TableWithJoins {
    TableWithJoins {
        relation: TableFactor::Table {
            name: ObjectName(vec![Ident::new(table.name.clone())]),
            alias: Some(TableAlias {
                name: Ident::new(alias),
                columns: vec![],
            }),
            args: vec![],
        },
        joins: vec![],
    }
}
