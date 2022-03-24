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
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::session::QueryContext;

pub async fn handle_show_table(
    context: QueryContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();

    let catalogs = catalog_reader
        .get_table_by_name(session.database(), &schema_name, &table_name)?
        .columns();

    let mut rows = vec![];
    for catalog in catalogs {
        rows.append(
            &mut catalog
                .column_desc
                .get_column_descs()
                .into_iter()
                .map(col_desc_to_row)
                .collect_vec(),
        );
    }

    Ok(PgResponse::new(
        StatementType::SHOW_TABLE,
        rows.len() as i32,
        rows,
        vec![
            PgFieldDescriptor::new("column_name".to_owned(), TypeOid::Varchar),
            PgFieldDescriptor::new("data_type".to_owned(), TypeOid::Varchar),
        ],
    ))
}

fn col_desc_to_row(col: ColumnDesc) -> Row {
    let type_name = {
        // if datatype is struct, use type name as struct name
        if let DataType::Struct { fields: _f } = col.data_type {
            col.type_name.clone()
        } else {
            format!("{:?}", &col.data_type)
        }
    };
    Row::new(vec![Some(col.name), Some(type_name)])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_show_table_handler() {
        let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "show table t";
        let pg_response = frontend.run_sql(sql).await.unwrap();

        let columns = pg_response
            .iter()
            .map(|row| {
                (
                    row.index(0).as_ref().unwrap().as_str(),
                    row.index(1).as_ref().unwrap().as_str(),
                )
            })
            .collect::<HashMap<&str, &str>>();

        let expected_columns = maplit::hashmap! {
            "_row_id" => "Int64",
            "v1" => "Int16",
            "v2" => "Int32",
            "v3" => "Int64",
            "v4" => "Float64",
            "v5" => "Float64",
        };

        assert_eq!(columns, expected_columns);
    }
}
