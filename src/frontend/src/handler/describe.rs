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
use risingwave_sqlparser::ast::{display_comma_separated, ObjectName};

use crate::binder::Binder;
use crate::catalog::table_catalog::TableCatalog;
use crate::handler::util::col_descs_to_rows;
use crate::session::OptimizerContext;

pub fn handle_describe(context: OptimizerContext, table_name: ObjectName) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();

    // For Source, it doesn't have table catalog so use get source to get column descs.
    let (columns, indices): (Vec<ColumnDesc>, Vec<TableCatalog>) = {
        let (catalogs, indices) = match catalog_reader
            .get_schema_by_name(session.database(), &schema_name)?
            .get_table_by_name(&table_name)
        {
            Some(table) => (
                &table.columns,
                catalog_reader
                    .get_schema_by_name(session.database(), &schema_name)?
                    .iter_index()
                    .filter(|x| x.is_index_on == Some(table.id))
                    .cloned()
                    .collect_vec(),
            ),
            None => (
                &catalog_reader
                    .get_source_by_name(session.database(), &schema_name, &table_name)?
                    .columns,
                vec![],
            ),
        };
        (
            catalogs
                .iter()
                .filter(|c| !c.is_hidden)
                .map(|c| c.column_desc.clone())
                .collect(),
            indices,
        )
    };

    // Convert all column descs to rows
    let mut rows = col_descs_to_rows(columns);

    // Convert all indexs to rows
    rows.extend(indices.iter().map(|i| {
        let s = i
            .distribution_keys
            .iter()
            .map(|c| i.columns[*c].name().to_string())
            .collect_vec();
        Row::new(vec![
            Some(i.name.clone()),
            Some(format!("index({})", display_comma_separated(&s))),
        ])
    }));

    // TODO: recover the original user statement
    Ok(PgResponse::new(
        StatementType::DESCRIBE_TABLE,
        rows.len() as i32,
        rows,
        vec![
            PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar),
            PgFieldDescriptor::new("Type".to_owned(), TypeOid::Varchar),
        ],
        true,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_describe_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create table t (v1 int, v2 int);")
            .await
            .unwrap();

        frontend
            .run_sql("create index idx1 on t (v1,v2);")
            .await
            .unwrap();

        let sql = "describe t";
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
            "v1" => "Int32",
            "v2" => "Int32",
            "idx1" => "index(v1, v2)",
        };

        assert_eq!(columns, expected_columns);
    }
}
