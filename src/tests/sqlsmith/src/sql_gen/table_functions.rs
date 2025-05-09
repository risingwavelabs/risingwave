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
use rand::distr::Alphanumeric;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, Expr, FunctionArg, ObjectName, TableAlias, TableFactor, Value,
};

use crate::sql_gen::utils::{create_args, create_table_alias};
use crate::sql_gen::{SqlGenerator, Table};

#[derive(Clone, Copy)]
enum JsonTopLevelKind {
    Any,
    Array,
    Object,
}

impl<R: Rng> SqlGenerator<'_, R> {
    /// Generates table functions.
    pub(crate) fn gen_table_func(&mut self) -> (TableFactor, Table) {
        match self.rng.random_range(0..=7) {
            0 => self.gen_generate_series(),
            1 => self.gen_range(),
            2 => self.gen_unnest(),
            3..=7 => self.gen_jsonb_func(),
            _ => unreachable!(),
        }
    }

    /// Generates `GENERATE_SERIES`.
    /// `GENERATE_SERIES(start: INT | TIMESTAMP, end: INT | TIMESTAMP, step?: INT | INTERVAL)`
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
    /// `RANGE(start: INT | TIMESTAMP, end: INT | TIMESTAMP, step?: INT | INTERVAL)`
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

    /// Generates `UNNEST`.
    /// `UNNEST(arr1 [, arr2, ...])`
    fn gen_unnest(&mut self) -> (TableFactor, Table) {
        let table_name = self.gen_table_name_with_prefix("unnest");
        let alias = create_table_alias(&table_name);

        let depth = self.rng.random_range(0..=5);
        let list_type = self.gen_list_data_type(depth);

        let array_expr = self.gen_simple_scalar(&list_type);

        let table = Table::new(table_name, vec![]);
        let relation = create_tvf("unnest", alias, create_args(vec![array_expr]), false);

        (relation, table)
    }

    /// Generates one of the JSONB-related table functions,
    /// including:
    /// - `JSON_ARRAY_ELEMENTS(JSONB)`
    /// - `JSON_ARRAY_ELEMENTS_TEXT(JSONB)`
    /// - `JSON_EACH(JSONB)`
    /// - `JSON_EACH_TEXT(JSONB)`
    /// - `JSON_OBJECT_KEYS(JSONB)`
    ///
    /// These functions require specific top-level JSONB types:
    /// - `JSON_ARRAY_ELEMENTS[_TEXT]` expects a JSON array
    /// - `JSON_EACH[_TEXT]` and `JSON_OBJECT_KEYS` expect a JSON object
    fn gen_jsonb_func(&mut self) -> (TableFactor, Table) {
        match self.rng.random_range(0..=4) {
            0 => self.gen_jsonb_tvf("jsonb_array_elements", JsonTopLevelKind::Array),
            1 => self.gen_jsonb_tvf("jsonb_array_elements_text", JsonTopLevelKind::Array),
            2 => self.gen_jsonb_tvf("jsonb_each", JsonTopLevelKind::Object),
            3 => self.gen_jsonb_tvf("jsonb_each_text", JsonTopLevelKind::Object),
            4 => self.gen_jsonb_tvf("jsonb_object_keys", JsonTopLevelKind::Object),
            _ => unreachable!(),
        }
    }

    fn gen_jsonb_tvf(&mut self, name: &str, kind: JsonTopLevelKind) -> (TableFactor, Table) {
        let table_name = self.gen_table_name_with_prefix(name);
        let alias = create_table_alias(&table_name);

        let depth = self.rng.random_range(1..=5);
        let jsonb_expr = self.gen_jsonb(depth, kind);

        let table = Table::new(table_name, vec![]);
        let relation = create_tvf(name, alias, create_args(vec![jsonb_expr]), false);

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

    fn gen_jsonb(&mut self, n: usize, kind: JsonTopLevelKind) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::Value(Value::SingleQuotedString(
                self.gen_json_value(0, n, kind),
            ))),
            data_type: AstDataType::Jsonb,
        }
    }

    fn gen_json_value(&mut self, depth: usize, max_depth: usize, kind: JsonTopLevelKind) -> String {
        if depth >= max_depth {
            return match self.rng.random_range(0..=3) {
                0 => format!("\"{}\"", self.gen_random_string()),
                1 => self.rng.random_range(-1000..1000).to_string(),
                2 => self.flip_coin().to_string(),
                3 => "null".into(),
                _ => unreachable!(),
            };
        }

        match if depth == 0 {
            match kind {
                JsonTopLevelKind::Array => 4,
                JsonTopLevelKind::Object => 5,
                JsonTopLevelKind::Any => self.rng.random_range(0..=5),
            }
        } else {
            self.rng.random_range(0..=5)
        } {
            0 => "null".into(),
            1 => self.flip_coin().to_string(),
            2 => self.rng.random_range(-1000..1000).to_string(),
            3 => format!("\"{}\"", self.gen_random_string()),
            4 => {
                let len = self.rng.random_range(1..=3);
                let elems: Vec<String> = (0..len)
                    .map(|_| self.gen_json_value(depth + 1, max_depth, JsonTopLevelKind::Any))
                    .collect();
                format!("[{}]", elems.join(","))
            }
            5 => {
                let len = self.rng.random_range(1..=3);
                let fields: Vec<String> = (0..len)
                    .map(|_| {
                        let key = self.gen_random_string();
                        let val = self.gen_json_value(depth + 1, max_depth, JsonTopLevelKind::Any);
                        format!("\"{}\":{}", key, val)
                    })
                    .collect();
                format!("{{{}}}", fields.join(","))
            }
            _ => unreachable!(),
        }
    }

    fn gen_random_string(&mut self) -> String {
        let len = self.rng.random_range(3..8);
        (0..len)
            .map(|_| self.rng.sample(Alphanumeric) as char)
            .collect()
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
