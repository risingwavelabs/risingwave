// Copyright 2023 RisingWave Labs
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
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::HandlerArgs;

pub async fn handle_drop_source(
    handler_args: HandlerArgs,
    name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, source_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (source, schema_name) = {
        let catalog_reader = session.env().catalog_reader().read_guard();

        if let Ok((table, _)) = catalog_reader.get_table_by_name(db_name, schema_path, &source_name)
        {
            return Err(table.bad_drop_error());
        }

        match catalog_reader.get_source_by_name(db_name, schema_path, &source_name) {
            Ok((s, schema)) => (s.clone(), schema),
            Err(e) => {
                return if if_exists {
                    Ok(RwPgResponse::empty_result_with_notice(
                        StatementType::DROP_SOURCE,
                        format!("source \"{}\" does not exist, skipping", source_name),
                    ))
                } else {
                    Err(e.into())
                }
            }
        }
    };

    session.check_privilege_for_drop_alter(schema_name, &*source)?;

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_source(source.id).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_SOURCE))
}
