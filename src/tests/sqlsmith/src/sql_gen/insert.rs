use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Statement, Values};

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_insert_stmt(&mut self, row_count: usize) -> Statement {
        let table = self.tables.choose(&mut self.rng).unwrap();
        let table_name = ObjectName(vec![table.clone().name.as_str().into()]);
        let data_types = table.columns.iter().cloned().map(|c| c.data_type).collect_vec();
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
