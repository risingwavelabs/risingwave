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
use risingwave_pb::catalog::connection::Info::ConnectionParams;

use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, ObjectName, RwPgResponse, SqlOption};

pub async fn handle_alter_source_connector_props(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    alter_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let source_def = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

        // For `CREATE TABLE WITH (connector = '...')`, users should call `ALTER TABLE` instead.
        if source.associated_table_id.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Use `ALTER TABLE` to alter a table with connector.".to_owned(),
            )
            .into());
        }

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        if let Some(conn_id) = source.connection_id {
            let conn = reader.get_connection_by_id(db_name, conn_id)?;
            if let ConnectionParams(params) = &conn.info {
                for prop in &alter_props {
                    let prop_key = prop.name.real_value();
                    if params.properties.contains_key(&prop_key)
                        || params.secret_refs.contains_key(&prop_key)
                    {
                        return Err(ErrorCode::InvalidInputSyntax(
                            "Cannot alter connector properties that are set by CONNECTION. Use `ALTER CONNECTION` to alter them.".to_owned(),
                        )
                        .into());
                    }
                }
            }
        }
        source.clone()
    };

    todo!()
}
