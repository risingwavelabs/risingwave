use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::RwSession;

pub(super) fn handle_explain(
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
        let meta = risingwave_meta::test_utils::LocalMeta::start().await;
        let frontend = crate::test_utils::LocalFrontend::new().await;

        let sql = "values (11, 22), (33+(1+2), 44);";
        let stmt = Parser::parse_sql(sql).unwrap().into_iter().next().unwrap();
        let result = super::handle_explain(frontend.session(), stmt, false).unwrap();
        let row = result.iter().next().unwrap();
        let s = row[0].as_ref().unwrap();
        assert!(s.contains("11"));
        assert!(s.contains("22"));
        assert!(s.contains("33"));
        assert!(s.contains("44"));

        meta.stop().await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handle_explain_scan() {
        let meta = risingwave_meta::test_utils::LocalMeta::start().await;
        let frontend = crate::test_utils::LocalFrontend::new().await;

        let sql_scan = "explain select * from t";

        let err_str = frontend.run_sql(sql_scan).await.err().unwrap().to_string();
        assert_eq!(err_str, "Item not found: relation \"t\"");

        frontend
            .run_sql("create table t (v1 bigint, v2 double precision)")
            .await
            .unwrap();

        let response = frontend.run_sql(sql_scan).await.unwrap();
        let row = response.iter().next().unwrap();
        let s = row[0].as_ref().unwrap();
        assert!(s.contains("v1"));
        assert!(s.contains("Int64"));
        assert!(s.contains("v2"));
        assert!(s.contains("Float64"));

        meta.stop().await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handle_explain_insert() {
        let meta = risingwave_meta::test_utils::LocalMeta::start().await;
        let frontend = crate::test_utils::LocalFrontend::new().await;

        frontend
            .run_sql("create table t (v1 int, v2 int)")
            .await
            .unwrap();

        let sql = "explain insert into t values (22, 33), (44, 55)";

        let response = frontend.run_sql(sql).await.unwrap();
        let row = response.iter().next().unwrap();
        let s = row[0].as_ref().unwrap();
        assert!(s.contains("Insert"));
        assert!(s.contains("22"));
        assert!(s.contains("33"));
        assert!(s.contains("44"));
        assert!(s.contains("55"));

        meta.stop().await;
    }
}
