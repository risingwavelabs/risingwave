//! The parser module directly uses the [`sqlparser`] crate
//! and re-exports its AST types.

pub use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;

/// Parse the SQL string into a list of ASTs.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}

#[cfg(test)]
mod tests {
    use crate::parser::parse;

    #[test]
    fn test_basic_ddl() {
        assert!(parse("select * from t1;").is_ok());
        assert!(parse("create materialized view mv1 as select * from t1;").is_ok());
        assert!(parse("create stream s (v1 int not null, v2 char(8) not null);").is_err());
    }
}
