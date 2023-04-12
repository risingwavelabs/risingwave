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

use itertools::Itertools;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, ObjectName, Query, SetExpr, Statement, Values};

use crate::sql_gen::SqlGenerator;
use crate::Table;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_insert_stmt(&mut self, table: Table, row_count: usize) -> Statement {
        let table_name = ObjectName(vec![table.name.as_str().into()]);
        let data_types = table
            .columns
            .iter()
            .cloned()
            .map(|c| c.data_type)
            .collect_vec();
        let values = self.gen_values(&data_types, row_count);
        let source = Query {
            with: None,
            body: SetExpr::Values(Values(values)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        };
        Statement::Insert {
            table_name,
            columns: vec![],
            source: Box::new(source),
            returning: vec![],
        }
    }

    fn gen_values(&mut self, data_types: &[DataType], row_count: usize) -> Vec<Vec<Expr>> {
        (0..row_count).map(|_| self.gen_row(data_types)).collect()
    }

    fn gen_row(&mut self, data_types: &[DataType]) -> Vec<Expr> {
        data_types
            .iter()
            .map(|typ| self.gen_simple_scalar(typ))
            .collect()
    }
}
