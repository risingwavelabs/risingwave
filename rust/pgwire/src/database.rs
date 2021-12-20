use crate::pg_result::{PgResult, StatementType};

pub struct Database {}

impl Database {
    pub fn run_statement(&self, _sql_stmt: &str) -> PgResult {
        // Hack a empty pg results. Replace it with real execution.
        PgResult::new(StatementType::SELECT, 0, vec![], vec![])
    }
}
