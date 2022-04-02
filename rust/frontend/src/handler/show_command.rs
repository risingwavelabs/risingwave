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

use itertools::Itertools;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ShowCommandObject;

use crate::session::OptimizerContext;

pub async fn handle_show_command(
    context: OptimizerContext,
    command: ShowCommandObject,
) -> Result<PgResponse> {
    let session = context.session_ctx;

    let catalog_reader = session.env().catalog_reader().read_guard();
    let row_descs = vec![PgFieldDescriptor::new("name".to_owned(), TypeOid::Varchar)];
    let rows = match command {
        ShowCommandObject::Table(Some(name)) => catalog_reader
            .get_all_table_names(session.database(), &name)?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::Table(None) => catalog_reader
            .get_all_table_names(session.database(), DEFAULT_SCHEMA_NAME)?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::Database => catalog_reader
            .get_all_database_names()?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::Schema => catalog_reader
            .get_all_schema_names(session.database())?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::View(Some(name)) => catalog_reader
            .get_all_mv_names(session.database(), &name)?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::View(None) => catalog_reader
            .get_all_mv_names(session.database(), DEFAULT_SCHEMA_NAME)?
            .iter()
            .map(|n| Row::new(vec![Some(n.clone())]))
            .collect_vec(),
        ShowCommandObject::Column(_table_name) => {
            todo!()
        }
    };
    Ok(PgResponse::new(
        StatementType::SHOW_COMMAND,
        rows.len() as i32,
        rows,
        row_descs,
    ))
}
