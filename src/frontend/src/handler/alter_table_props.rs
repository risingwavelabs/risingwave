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
use risingwave_sqlparser::ast::{Ident, ObjectName, SqlOption};

use super::alter_sink_props::{AlterSinkObject, handle_alter_sink_props};
use super::alter_table_column::fetch_table_catalog_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

pub async fn handle_alter_table_props(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    changed_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let original_table = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;
    if let Some(sink_name) = original_table.iceberg_sink_name()
        && let Some(source_name) = original_table.iceberg_source_name()
    {
        let mut source_names = table_name.0.clone();
        let mut sink_names = table_name.0.clone();
        source_names.pop();
        sink_names.pop();
        source_names.push(Ident::new_unchecked(source_name));
        sink_names.push(Ident::new_unchecked(sink_name));
        handle_alter_sink_props(
            handler_args,
            AlterSinkObject::IcebergTable(
                table_name,
                ObjectName(sink_names),
                ObjectName(source_names),
            ),
            changed_props,
        )
        .await?;
    } else {
        return Err(ErrorCode::NotSupported(
            "ALTER TABLE With is only supported for iceberg tables".to_owned(),
            "Try `ALTER TABLE .. ADD/DROP COLUMN ...`".to_owned(),
        )
        .into());
    }
    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
