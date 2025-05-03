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

use chrono::{Duration, NaiveDateTime};
use rand::Rng;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, Expr, FunctionArg, ObjectName, TableAlias, TableFactor, Value,
};

use crate::sql_gen::utils::{create_args, create_table_alias};
use crate::sql_gen::{SqlGenerator, Table};

impl<R: Rng> SqlGenerator<'_, R> {
    /// Generates table functions.
    pub(crate) fn gen_table_func(&mut self) -> (TableFactor, Table) {
        match self.rng.random_range(0..=1) {
            0..=0 => self.gen_generate_series(),
            1..=1 => self.gen_range(),
            _ => unreachable!(),
        }
    }

    /// Generates `GENERATE_SERIES`.
    /// GENERATE_SERIES(start: INT | TIMESTAMP, end: INT | TIMESTAMP, step?: INT | INTERVAL)
    /// - When type is INT: step is optional.
    /// - When type is TIMESTAMP: step (INTERVAL) is required.
    fn gen_generate_series(&mut self) -> (TableFactor, Table) {
        let table_name = self.gen_table_name_with_prefix("generate_series");
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

    /// Generates `RANGE`.
    /// RANGE(start: INT | TIMESTAMP, end: INT | TIMESTAMP, step?: INT | INTERVAL)
    /// - When type is INT: step is optional.
    /// - When type is TIMESTAMP: step (INTERVAL) is required.
    fn gen_range(&mut self) -> (TableFactor, Table) {
        let table_name = self.gen_table_name_with_prefix("range");
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

    fn integer_to_value_expr(num: i32) -> Expr {
        Expr::Value(Value::Number(num.to_string()))
    }

    fn gen_simple_integer_range(&mut self) -> (Expr, Expr, Option<Expr>) {
        let mut values: Vec<i32> = (0..3).map(|_| self.rng.random_range(0..100)).collect();
        values.sort_unstable();

        let start = Self::integer_to_value_expr(values[0]);
        let end = Self::integer_to_value_expr(values[2]);
        let step = Some(Self::integer_to_value_expr(values[1]));

        if self.flip_coin() {
            (start, end, step)
        } else {
            (start, end, None)
        }
    }

    fn integer_to_timestamp_expr(offset_secs: i32) -> Expr {
        let base =
            NaiveDateTime::parse_from_str("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let ts = base + Duration::seconds(offset_secs as i64);
        Expr::TypedString {
            data_type: AstDataType::Timestamp(false),
            value: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
        }
    }

    fn integer_to_interval_expr(num: i32) -> Expr {
        Expr::TypedString {
            data_type: AstDataType::Interval,
            value: format!("{} seconds", num),
        }
    }

    fn gen_simple_timestamp_range(&mut self) -> (Expr, Expr, Option<Expr>) {
        let mut secs: Vec<i32> = (0..3).map(|_| self.rng.random_range(0..3600)).collect();
        secs.sort_unstable();

        let start = Self::integer_to_timestamp_expr(secs[0]);
        let end = Self::integer_to_timestamp_expr(secs[2]);
        let step = Some(Self::integer_to_interval_expr(secs[1]));

        (start, end, step)
    }

    fn gen_start_end_step(&mut self) -> (Expr, Expr, Option<Expr>) {
        match self.rng.random_range(0..=1) {
            0..=0 => self.gen_simple_integer_range(),
            1..=1 => self.gen_simple_timestamp_range(),
            _ => unreachable!(),
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
