use rand::Rng;
use risingwave_sqlparser::ast::{ObjectName, Statement};
use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_insert(&mut self) -> Statement {
        Statement::Insert {
            table_name: (),
            columns: (),
            source: (),
            returning: (),
        }
    }
}
