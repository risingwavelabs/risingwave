use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::{Parser, ParserError};

use crate::handler;
use crate::pgwire::pg_result::PgResult;
pub struct Database {}

/// Parse the SQL string into a list of ASTs.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(sql)
}

impl Database {
    pub fn run_statement(&self, sql: &str) -> PgResult {
        // Parse sql.
        let stmts = parse(sql).unwrap();
        // With pgwire, there would be at most 1 statement in the vec.
        stmts.into_iter().map(handler::handle).next().unwrap()
    }
}
