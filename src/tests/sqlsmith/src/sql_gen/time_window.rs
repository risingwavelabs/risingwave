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
use rand::prelude::IndexedRandom;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, FunctionArg, ObjectName, TableAlias, TableFactor,
};

use crate::sql_gen::utils::{create_args, create_table_alias};
use crate::sql_gen::{Column, Expr, SqlGenerator, Table};

impl<R: Rng> SqlGenerator<'_, R> {
    /// Generates time window functions.
    pub(crate) fn gen_time_window_func(&mut self) -> (TableFactor, Table) {
        match self.flip_coin() {
            true => self.gen_hop(),
            false => self.gen_tumble(),
        }
    }

    /// Generates `TUMBLE`.
    /// TUMBLE(data: TABLE, timecol: COLUMN, size: INTERVAL, offset?: INTERVAL)
    fn gen_tumble(&mut self) -> (TableFactor, Table) {
        let tables: Vec<_> = find_tables_with_timestamp_cols(self.tables.clone());
        let (source_table_name, time_cols, schema) = tables
            .choose(&mut self.rng)
            .expect("seeded tables all do not have timestamp");
        let table_name = self.gen_table_name_with_prefix("tumble");
        let alias = create_table_alias(&table_name);

        let name = Expr::Identifier(source_table_name.as_str().into());
        let size = self.gen_size(1);
        let time_col = time_cols.choose(&mut self.rng).unwrap();
        let time_col = Expr::Identifier(time_col.name.as_str().into());
        let args = create_args(vec![name, time_col, size]);
        let relation = create_tvf("tumble", alias, args, false);

        let table = Table::new(table_name, schema.clone());

        (relation, table)
    }

    /// Generates `HOP`.
    /// HOP(data: TABLE, timecol: COLUMN, slide: INTERVAL, size: INTERVAL, offset?: INTERVAL)
    fn gen_hop(&mut self) -> (TableFactor, Table) {
        let tables = find_tables_with_timestamp_cols(self.tables.clone());
        let (source_table_name, time_cols, schema) = tables
            .choose(&mut self.rng)
            .expect("seeded tables all do not have timestamp");
        let table_name = self.gen_table_name_with_prefix("hop");
        let alias = create_table_alias(&table_name);

        let time_col = time_cols.choose(&mut self.rng).unwrap();

        let name = Expr::Identifier(source_table_name.as_str().into());
        // We fix slide to "1" here, as slide needs to be divisible by size.
        let (slide_secs, slide) = self.gen_slide();
        let size = self.gen_size(slide_secs);
        let time_col = Expr::Identifier(time_col.name.as_str().into());
        let args = create_args(vec![name, time_col, slide, size]);

        let relation = create_tvf("hop", alias, args, false);

        let table = Table::new(table_name, schema.clone());

        (relation, table)
    }

    fn gen_secs(&mut self) -> u64 {
        self.rng.random_range(1..100)
    }

    // TODO(kwannoel): Disable for now, otherwise time window may take forever
    // fn gen_secs(&mut self) -> u64 {
    //     let minute = 60;
    //     let hour = 60 * minute;
    //     let day = 24 * hour;
    //     let week = 7 * day;
    //     let rand_secs = self.rng.random_range(1..week);
    //     let choices = [1, minute, hour, day, week, rand_secs];
    //     let secs = choices.choose(&mut self.rng).unwrap();
    //     *secs
    // }

    fn secs_to_interval_expr(i: u64) -> Expr {
        Expr::TypedString {
            data_type: AstDataType::Interval,
            value: i.to_string(),
        }
    }

    fn gen_slide(&mut self) -> (u64, Expr) {
        let slide_secs = self.gen_secs();
        let expr = Self::secs_to_interval_expr(slide_secs);
        (slide_secs, expr)
    }

    /// Size must be divisible by slide.
    /// i.e.
    /// `size_secs` = k * `slide_secs`.
    /// k cannot be too large, to avoid overflow.
    fn gen_size(&mut self, slide_secs: u64) -> Expr {
        let k = self.rng.random_range(1..20);
        let size_secs = k * slide_secs;
        Self::secs_to_interval_expr(size_secs)
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

fn is_timestamp_col(c: &Column) -> bool {
    c.data_type == DataType::Timestamp || c.data_type == DataType::Timestamptz
}

fn find_tables_with_timestamp_cols(tables: Vec<Table>) -> Vec<(String, Vec<Column>, Vec<Column>)> {
    tables
        .into_iter()
        .filter_map(|table| {
            if !table.is_base_table {
                return None;
            }
            let name = table.name.clone();
            let columns = table.get_qualified_columns();
            let mut timestamp_cols = vec![];
            for col in columns {
                let col_name = col.name.clone();
                if col_name.contains("window_start") || col_name.contains("window_end") {
                    return None;
                }
                if is_timestamp_col(&col) {
                    timestamp_cols.push(col);
                }
            }
            if timestamp_cols.is_empty() {
                None
            } else {
                Some((name, timestamp_cols, table.columns))
            }
        })
        .collect()
}
