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

use risingwave_sqlparser::ast::ObjectName;

use crate::error::{ErrorCode, Result};
use crate::handler::alter_source_with_sr::fetch_source_catalog_with_db_schema_id;
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_resume_source(
    handler_args: HandlerArgs,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    // Fetch the source catalog
    let source = fetch_source_catalog_with_db_schema_id(&session, &name)?;

    // Check if this is a table source (not supported)
    if source.associated_table_id.is_some() {
        return Err(ErrorCode::NotSupported(
            "resume CDC table using RESUME SOURCE statement".to_owned(),
            "try to use RESUME TABLE instead".to_owned(),
        )
        .into());
    }

    // Call catalog writer to resume the source
    let catalog_writer = session.catalog_writer()?;
    catalog_writer.resume_source(source.id).await?;

    Ok(pgwire::pg_response::PgResponse::empty_result(
        pgwire::pg_response::StatementType::ALTER_SOURCE,
    ))
}
