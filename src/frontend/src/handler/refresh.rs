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
use risingwave_pb::meta::RefreshRequest;
use risingwave_sqlparser::ast::ObjectName;

use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::handler::util::get_table_catalog_by_table_name;
use crate::handler::{HandlerArgs, RwPgResponse};

/// Handle REFRESH statement
///
/// This function processes the REFRESH statement by:
/// 1. Validating the table exists and is refreshable
/// 2. Sending a refresh command to the meta service
/// 3. Returning appropriate response to the client
pub async fn handle_refresh(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    // Get table catalog to validate table exists
    let (table_catalog, schema_name) =
        get_table_catalog_by_table_name(session.as_ref(), &table_name)?;

    // Check if table supports refresh operations
    if !table_catalog.refreshable {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "Table '{}.{}' is not refreshable. Only tables created with REFRESHABLE flag support manual refresh.",
            schema_name, table_name
        )).into());
    }

    // Only allow refresh on tables, not views or materialized views
    match table_catalog.table_type() {
        TableType::Table => {
            // This is valid
        }
        t @ (TableType::MaterializedView | TableType::Index | TableType::Internal) => {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "REFRESH is only supported for tables, got {:?}.",
                t
            ))
            .into());
        }
    }

    let table_id = table_catalog.id();

    // Create refresh request
    let refresh_request = RefreshRequest {
        table_id: table_id.table_id(),
    };

    // Send refresh command to meta service via stream manager
    let meta_client = session.env().meta_client();
    match meta_client.refresh(refresh_request).await {
        Ok(_) => {
            // Refresh command sent successfully
            tracing::info!(
                table_id = %table_id,
                table_name = %table_name,
                "Manual refresh initiated"
            );

            // Return success response
            Ok(PgResponse::builder(StatementType::OTHER)
                .notice(format!(
                    "REFRESH initiated for table '{}.{}'",
                    schema_name, table_name
                ))
                .into())
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                table_id = %table_id,
                table_name = %table_name,
                "Failed to initiate refresh"
            );

            Err(ErrorCode::InternalError(format!(
                "Failed to refresh table '{}.{}': {}",
                schema_name, table_name, e
            ))
            .into())
        }
    }
}
