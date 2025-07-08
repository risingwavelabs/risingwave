// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_connector::WithPropertiesExt;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
/// Handle `ALTER SOURCE <source_name> RESET` statements.
pub async fn handle_alter_source_reset(
    handler_args: HandlerArgs,
    source_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let source_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        // For `CREATE TABLE WITH (connector = '...')`, users should call `ALTER TABLE` instead.
        if source.associated_table_id.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Use `ALTER TABLE` to alter a table with connector.".to_owned(),
            )
            .into());
        }

        (**source).clone()
    };

    // Check if this is a CDC source
    if !source_catalog.with_properties.is_cdc_connector() {
        return Err(ErrorCode::InvalidInputSyntax(
            "RESET is only supported for CDC sources".to_owned(),
        )
        .into());
    }

    // TODO: Implement the actual reset logic
    // For now, we'll perform the reset operation
    perform_cdc_source_reset(&session, source_catalog.id).await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SOURCE))
}

async fn perform_cdc_source_reset(
    session: &crate::session::SessionImpl,
    source_id: u32,
) -> Result<()> {
    let meta_client = session.env().meta_client();

    // Call meta service to perform the CDC source reset
    meta_client.reset_cdc_source(source_id).await?;

    Ok(())
}
