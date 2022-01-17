use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::pgwire::pg_result::PgResult;

pub(super) fn handle_explain(stmt: Statement, _verbose: bool) -> PgResult {
    // bind, plan, optimize, and serialize here
    let mut binder = Binder::new();
    let bound = binder.bind(stmt).unwrap();
    format!("{:?}", bound).into()
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::parser::Parser;

    #[test]
    fn test_handle_explain() {
        let sql = "values (11, 22), (33, 44);";
        let stmt = Parser::parse_sql(sql).unwrap().into_iter().next().unwrap();
        let result = super::handle_explain(stmt, false);
        let row = result.iter().next().unwrap();
        let s = row[0].as_ref().unwrap().as_utf8();
        assert!(s.contains("Values"));
    }
}
