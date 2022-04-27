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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{AstOption, DropMode, Ident};

use crate::session::OptimizerContext;

pub async fn handle_drop_schema(
    context: OptimizerContext,
    schema_name: Ident,
    if_exist: bool,
    mode: AstOption<DropMode>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader();

    let schema = {
        let reader = catalog_reader.read_guard();
        match reader.get_schema_by_name(session.database(), &schema_name.value) {
            Ok(schema) => schema.clone(),
            Err(err) => {
                if if_exist {
                    return Ok(PgResponse::empty_result(StatementType::DROP_SCHEMA));
                } else {
                    return Err(err);
                }
            }
        }
    };
    let schema_id = {
        if AstOption::Some(DropMode::Restrict) == mode || AstOption::None == mode {
            if !schema.is_empty() {
                return Err(ErrorCode::InternalError(
                    "Please drop tables and sources in this schema before drop it".to_string(),
                )
                .into());
            }
            schema.id()
        } else {
            todo!();
        }
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_schema(schema_id).await?;
    Ok(PgResponse::empty_result(StatementType::DROP_SCHEMA))
}
