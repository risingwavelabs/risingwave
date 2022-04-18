// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::session::OptimizerContext;

pub async fn handle_drop_mv(
    context: OptimizerContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader();

    {
        let reader = catalog_reader.read_guard();
        if let Ok(s) = reader.get_source_by_name(session.database(), &schema_name, &table_name) {
            if s.source_type == SourceType::Source {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP SOURCE` to drop a source.".to_owned(),
                )));
            }
        }
    }

    let table_id = {
        let reader = catalog_reader.read_guard();
        let table = reader.get_table_by_name(session.database(), &schema_name, &table_name)?;

        // If associated source is `Some`, then it is a actually a materialized source / table v2.
        if table.associated_source_id().is_some() {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Use `DROP TABLE` to drop a table.".to_owned(),
            )));
        }
        table.id()
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_materialized_view(table_id).await?;

    Ok(PgResponse::empty_result(
        StatementType::DROP_MATERIALIZED_VIEW,
    ))
}
