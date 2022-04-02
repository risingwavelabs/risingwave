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

use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::handler::show_source::col_descs_to_rows;
use crate::session::OptimizerContext;

pub async fn handle_describe_table(
    context: OptimizerContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, source_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();

    // Get column descs from table catalog
    let columns: Vec<ColumnDesc> = catalog_reader
        .get_table_by_name(session.database(), &schema_name, &source_name)?
        .columns
        .iter()
        .filter(|c| !c.is_hidden)
        .map(|c| c.column_desc.clone())
        .collect();

    // Convert all column descs to rows
    let rows = col_descs_to_rows(columns);

    Ok(PgResponse::new(
        StatementType::DESCRIBE_TABLE,
        rows.len() as i32,
        rows,
        vec![
            PgFieldDescriptor::new("name".to_owned(), TypeOid::Varchar),
            PgFieldDescriptor::new("type".to_owned(), TypeOid::Varchar),
        ],
    ))
}
