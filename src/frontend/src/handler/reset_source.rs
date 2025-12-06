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

use risingwave_connector::WithPropertiesExt;
use risingwave_sqlparser::ast::ObjectName;

use super::alter_source_with_sr::fetch_source_catalog_with_db_schema_id;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

/// Handle `RESET SOURCE source_name` command
/// This command is used to reset CDC source offset to the latest position
/// when the original offset has expired in the upstream binlog/oplog.
pub async fn handle_reset_source(
    handler_args: HandlerArgs,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    // Fetch source catalog and check privileges
    let source = fetch_source_catalog_with_db_schema_id(&session, &name)?;

    // Check if the source has an associated table (CDC table)
    if source.associated_table_id.is_some() {
        return Err(ErrorCode::NotSupported(
            "reset CDC table using RESET SOURCE statement".to_owned(),
            "try to use RESET TABLE instead".to_owned(),
        )
        .into());
    }

    // Only CDC sources (MySQL/MongoDB) support RESET operation
    if !source.with_properties.is_cdc_connector() {
        return Err(ErrorCode::NotSupported(
            "RESET SOURCE only supports CDC sources (MySQL, MongoDB)".to_owned(),
            "this operation is only for CDC sources with binlog/oplog based synchronization"
                .to_owned(),
        )
        .into());
    }

    tracing::info!(
        "RESET SOURCE {} (id: {}) - Calling meta service...",
        source.name,
        source.id
    );

    // Call meta service to reset the source
    let catalog_writer = session.catalog_writer()?;
    catalog_writer.reset_source(source.id).await?;

    Ok(pgwire::pg_response::PgResponse::empty_result(
        pgwire::pg_response::StatementType::ALTER_SOURCE,
    ))
}
