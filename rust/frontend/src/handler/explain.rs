use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::pgwire::pg_response::PgResponse;

pub(super) fn handle_explain(stmt: Statement, _verbose: bool) -> Result<PgResponse> {
    // bind, plan, optimize, and serialize here
    let mut binder = Binder::new();
    let bound = binder.bind(stmt)?;
    Ok(format!("{:?}", bound).into())
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::parser::Parser;

    #[test]
    fn test_handle_explain() {
        let sql = "values (11, 22), (33+(1+2), 44);";
        let stmt = Parser::parse_sql(sql).unwrap().into_iter().next().unwrap();
        let result = super::handle_explain(stmt, false).unwrap();
        let row = result.iter().next().unwrap();
        let s = row[0].as_ref().unwrap().as_utf8();
        assert!(s.contains("11"));
        assert!(s.contains("22"));
        assert!(s.contains("33"));
        assert!(s.contains("44"));
    }
}
