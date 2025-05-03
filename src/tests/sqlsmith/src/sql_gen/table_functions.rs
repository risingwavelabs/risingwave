// Copyright 2025 RisingWave Labs
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

use rand::Rng;
use risingwave_sqlparser::ast::{Expr, FunctionArg, ObjectName, TableAlias, TableFactor, Value};
use crate::sql_gen::{utils::create_args, SqlGenerator, Table};

use super::utils::create_table_alias;

impl<R: Rng> SqlGenerator<'_, R> {
    /// Generates table functions
    pub(crate) fn gen_table_func(&mut self) -> (TableFactor, Table) {
        match self.rng.random_range(0..=1) {
            0..=0 => self.gen_generate_series(),
            1..=1 => self.gen_range(),
            _ => unreachable!(),
        }
    }

    /// Generates `GENERATE_SERIES`
    fn gen_generate_series(&mut self) -> (TableFactor, Table) {
        let table_name= self.gen_table_name_with_prefix("generate_series");
        let alias = create_table_alias(&table_name);

        let (start, end, step) = self.gen_start_end_step();
        let args = vec![Some(start), Some(end), step]
            .into_iter()
            .flatten()
            .collect();

        let table = Table::new(table_name, vec![]);

        let relation = create_tvf("generate_series", alias, create_args(args), false);

        (relation, table)
    }

    /// Generates `RANGE`
    fn gen_range(&mut self) -> (TableFactor, Table) {
        let table_name= self.gen_table_name_with_prefix("range");
        let alias = create_table_alias(&table_name);

        let (start, end, step) = self.gen_start_end_step();
        let args = vec![Some(start), Some(end), step]
            .into_iter()
            .flatten()
            .collect();

        let table = Table::new(table_name, vec![]);

        let relation = create_tvf("range", alias, create_args(args), false);

        (relation, table)
    }

    fn integer_to_expr(num: i32) -> Expr {
        Expr::Value(Value::Number(num.to_string()))
    }

    fn gen_start_end_step(&mut self) -> (Expr, Expr, Option<Expr>) {
        let mut values: Vec<i32> = (0..3)
            .map(|_| self.rng.random_range(0..100))
            .collect();
        values.sort_unstable();

        let start = Self::integer_to_expr(values[0]);
        let end = Self::integer_to_expr(values[2]);
        let step = Some(Self::integer_to_expr(values[1]));

        if self.flip_coin() {
            (start, end, step)
        } else {
            (start, end, None)
        }
    }
}

/// Create a table view function.
fn create_tvf(
    name: &str,
    alias: TableAlias,
    args: Vec<FunctionArg>,
    with_ordinality: bool,
) -> TableFactor {
    TableFactor::TableFunction {
        name: ObjectName(vec![name.into()]),
        alias: Some(alias),
        args,
        with_ordinality,
    }
}
