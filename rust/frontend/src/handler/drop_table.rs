use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::session::QueryContext;

pub async fn handle_drop_table(
    context: QueryContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let str_table_name = table_name.to_string();

    let catalog_mgr = session.env().catalog_mgr();
    catalog_mgr
        .drop_table(session.database(), DEFAULT_SCHEMA_NAME, &str_table_name)
        .await?;

    Ok(PgResponse::new(
        StatementType::DROP_TABLE,
        0,
        vec![],
        vec![],
    ))
}

#[cfg(test)]
mod tests {

    use crate::catalog::catalog_service::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_table_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_drop_table = "drop table t;";
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_drop_table).await.unwrap();

        let catalog_manager = frontend.session().env().catalog_mgr();

        assert!(catalog_manager
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .is_none());
    }
}
