use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_sqlparser::ast::{CreateSecretStatement, Value};

use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::Binder;

pub async fn handle_create_secret(
    handler_args: HandlerArgs,
    stmt: CreateSecretStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = session.database();
    let (schema_name, connection_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.secret_name.clone())?;

    if let Err(e) = session.check_secret_name_duplicated(stmt.secret_name) {
        return if stmt.if_not_exists {
            Ok(PgResponse::builder(StatementType::CREATE_SECRET)
                .notice(format!("secret \"{}\" exists, skipping", connection_name))
                .into())
        } else {
            Err(e)
        };
    }
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;
    let secret_payload = match &stmt.credential {
        &Value::SingleQuotedString(s) => s.as_bytes().to_vec(),
        _ => {
            return Err(ErrorCode::InvalidParameterValue(
                "secret payload must be a string".to_string(),
            )
            .into())
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_secret(
            secret_name,
            database_id,
            schema_id,
            session.user_id(),
            secret_payload,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SECRET))
}
