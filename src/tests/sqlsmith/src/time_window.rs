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

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_frontend::expr::DataTypeName;
use risingwave_sqlparser::ast::{
    DataType, FunctionArg, FunctionArgExpr, ObjectName, TableAlias, TableFactor, TableWithJoins,
};

use crate::{Column, Expr, SqlGenerator, Table};

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// Generates time window functions.
    pub(crate) fn gen_time_window_func(&mut self) -> TableWithJoins {
        match self.flip_coin() {
            true => self.gen_hop(),
            false => self.gen_tumble(),
        }
    }

    /// Generates `TUMBLE`.
    /// TUMBLE(data: TABLE, timecol: COLUMN, size: INTERVAL, offset?: INTERVAL)
    fn gen_tumble(&mut self) -> TableWithJoins {
        let tables = find_tables_with_timestamp_cols(self.tables.clone());
        let (source_table_name, time_cols, schema) = tables
            .choose(&mut self.rng)
            .expect("seeded tables all do not have timestamp");
        let table_name = self.create_table_name_with_prefix("tumble");
        let alias = create_alias(&table_name);

        let name = Expr::Identifier(source_table_name.as_str().into());
        // TODO: Currently only literal size expr supported.
        // Tracked in: <https://github.com/singularity-data/risingwave/issues/3896>
        let size = self.gen_simple_scalar(DataTypeName::Interval);
        let time_col = time_cols.choose(&mut self.rng).unwrap();
        let time_col = Expr::Identifier(time_col.name.as_str().into());
        let args = create_args(vec![name, time_col, size]);
        let relation = create_tvf("tumble", alias, args);

        let table = create_sqlsmith_table(table_name, schema.clone());
        self.add_relation_to_context(table);

        relation
    }

    /// Generates `HOP`.
    /// HOP(data: TABLE, timecol: COLUMN, slide: INTERVAL, size: INTERVAL, offset?: INTERVAL)
    fn gen_hop(&mut self) -> TableWithJoins {
        let tables = find_tables_with_timestamp_cols(self.tables.clone());
        let (source_table_name, time_cols, schema) = tables
            .choose(&mut self.rng)
            .expect("seeded tables all do not have timestamp");
        let table_name = self.create_table_name_with_prefix("hop");
        let alias = create_alias(&table_name);

        let time_col = time_cols.choose(&mut self.rng).unwrap();

        let name = Expr::Identifier(source_table_name.as_str().into());
        // TODO: Currently only literal slide/size expr supported.
        // Tracked in: <https://github.com/singularity-data/risingwave/issues/3896>
        let slide = Expr::TypedString {
            data_type: DataType::Interval,
            value: "1".to_string(),
        };
        let size = self.gen_simple_scalar(DataTypeName::Interval);
        let time_col = Expr::Identifier(time_col.name.as_str().into());
        let args = create_args(vec![name, time_col, slide, size]);

        let relation = create_tvf("hop", alias, args);

        let table = create_sqlsmith_table(table_name, schema.clone());
        self.add_relation_to_context(table);

        relation
    }

    fn create_table_name_with_prefix(&self, prefix: &str) -> String {
        format!("{}_{}", prefix, &self.bound_relations.len())
    }
}

fn create_args(arg_exprs: Vec<Expr>) -> Vec<FunctionArg> {
    arg_exprs
        .into_iter()
        .map(create_function_arg_from_expr)
        .collect()
}

fn create_alias(table_name: &str) -> TableAlias {
    TableAlias {
        name: table_name.into(),
        columns: vec![],
    }
}

fn create_tvf(name: &str, alias: TableAlias, args: Vec<FunctionArg>) -> TableWithJoins {
    let factor = TableFactor::Table {
        name: ObjectName(vec![name.into()]),
        alias: Some(alias),
        args,
    };
    TableWithJoins {
        relation: factor,
        joins: vec![],
    }
}

fn create_sqlsmith_table(name: String, columns: Vec<Column>) -> Table {
    Table { name, columns }
}

/// Create `FunctionArg` from an `Expr`.
fn create_function_arg_from_expr(expr: Expr) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
}

fn is_timestamp_col(c: &Column) -> bool {
    c.data_type == DataTypeName::Timestamp || c.data_type == DataTypeName::Timestampz
}

fn get_table_name_and_cols_with_timestamp(table: Table) -> (String, Vec<Column>, Vec<Column>) {
    let name = table.name.clone();
    let cols_with_timestamp = table
        .get_qualified_columns()
        .iter()
        .cloned()
        .filter(is_timestamp_col)
        .collect_vec();
    (name, cols_with_timestamp, table.columns)
}

fn find_tables_with_timestamp_cols(tables: Vec<Table>) -> Vec<(String, Vec<Column>, Vec<Column>)> {
    tables
        .into_iter()
        .map(get_table_name_and_cols_with_timestamp)
        .filter(|(_name, timestamp_cols, _schema)| !timestamp_cols.is_empty())
        .collect()
}
