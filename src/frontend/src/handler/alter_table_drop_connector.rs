// Copyright 2024 RisingWave Labs
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

use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, ObjectName, PgResponse, RwPgResponse, StatementType};
use crate::Binder;

pub async fn handle_alter_table_drop_connector(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let reader = session.env().catalog_reader().read_guard();

    let (table_def, schema_name) =
        reader.get_any_table_by_name(db_name, schema_path, &real_table_name)?;
    session.check_privilege_for_drop_alter(schema_name, &**table_def)?;

    if table_def.associated_source_id.is_none() {
        return Err(ErrorCode::ProtocolError(format!(
            "Table {} is not associated with a connector",
            real_table_name
        ))
        .into());
    }

    table_def.definition

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
