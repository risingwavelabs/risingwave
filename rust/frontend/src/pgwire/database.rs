use crate::parser::parse;
use crate::pgwire::pg_result::{PgResult, StatementType};
pub struct Database {}

impl Database {
    pub fn run_statement(&self, sql_stmt: &str) -> PgResult {
        // Parse sql.
        let _ast = parse(sql_stmt).unwrap();
        // Hack a empty pg results. Replace it with real execution.
        PgResult::new(StatementType::SELECT, 0, vec![], vec![])
    }
}
