use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::RwSession;

pub(super) async fn handle_explain(
    session: &RwSession,
    stmt: Statement,
    _verbose: bool,
) -> Result<PgResponse> {
    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .unwrap();
    // bind, plan, optimize, and serialize here
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(stmt)?;
    let mut planner = Planner::new();
    let plan = planner.plan(bound)?;
    let mut output = String::new();
    plan.explain(0, &mut output)
        .map_err(|e| ErrorCode::InternalError(e.to_string()))?;
    Ok(output.into())
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::parser::Parser;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handle_explain() {
        let meta = risingwave_meta::test_utils::LocalMeta::start_in_tempdir().await;
        let frontend = crate::test_utils::LocalFrontend::new().await;

        let sql = "values (11, 22), (33+(1+2), 44);";
        let stmt = Parser::parse_sql(sql).unwrap().into_iter().next().unwrap();
        let result = super::handle_explain(frontend.session(), stmt, false)
            .await
            .unwrap();
        let row = result.iter().next().unwrap();
        let s = row[0].as_ref().unwrap();
        assert!(s.contains("11"));
        assert!(s.contains("22"));
        assert!(s.contains("33"));
        assert!(s.contains("44"));

        meta.stop().await;
    }
}
