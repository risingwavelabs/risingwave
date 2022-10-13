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

use std::collections::HashSet;

use itertools::Itertools;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{display_comma_separated, ObjectName};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::{CatalogError, IndexCatalog};
use crate::handler::util::col_descs_to_rows;
use crate::session::OptimizerContext;

pub fn handle_describe(context: OptimizerContext, table_name: ObjectName) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_table_or_source_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = match schema_name.as_deref() {
        Some(schema_name) => SchemaPath::Name(schema_name),
        None => SchemaPath::Path(&search_path, user_name),
    };

    let catalog_reader = session.env().catalog_reader().read_guard();

    // For Source, it doesn't have table catalog so use get source to get column descs.
    let (columns, indices): (Vec<ColumnDesc>, Vec<IndexCatalog>) = {
        let (catalogs, indices) =
            match catalog_reader.get_table_by_name(db_name, schema_path, &table_name) {
                Ok((table, schema_name)) => (
                    &table.columns,
                    catalog_reader
                        .get_schema_by_name(session.database(), schema_name)?
                        .iter_index()
                        .filter(|index| index.primary_table.id == table.id)
                        .cloned()
                        .collect_vec(),
                ),
                Err(_) => {
                    match catalog_reader.get_source_by_name(db_name, schema_path, &table_name) {
                        Ok((source, _)) => (&source.columns, vec![]),
                        Err(_) => {
                            return Err(CatalogError::NotFound(
                                "table or source",
                                table_name.to_string(),
                            )
                            .into());
                        }
                    }
                }
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

    // Convert all indexes to rows
    rows.extend(indices.iter().map(|index| {
        let index_table = index.index_table.clone();

        let index_columns = index_table
            .pk
            .iter()
            .filter(|x| !index_table.columns[x.index].is_hidden)
            .map(|x| index_table.columns[x.index].name().to_string())
            .collect_vec();

        let pk_column_index_set = index_table
            .pk
            .iter()
            .map(|x| x.index)
            .collect::<HashSet<_>>();

        let include_columns = index_table
            .columns
            .iter()
            .enumerate()
            .filter(|(i, _)| !pk_column_index_set.contains(i))
            .filter(|(_, x)| !x.is_hidden)
            .map(|(_, x)| x.name().to_string())
            .collect_vec();

        let distributed_by_columns = index_table
            .distribution_key
            .iter()
            .map(|&x| index_table.columns[x].name().to_string())
            .collect_vec();

        Row::new(vec![
            Some(index.name.clone().into()),
            if include_columns.is_empty() {
                Some(
                    format!(
                        "index({}) distributed by({})",
                        display_comma_separated(&index_columns),
                        display_comma_separated(&distributed_by_columns),
                    )
                    .into(),
                )
            } else {
                Some(
                    format!(
                        "index({}) include({}) distributed by({})",
                        display_comma_separated(&index_columns),
                        display_comma_separated(&include_columns),
                        display_comma_separated(&distributed_by_columns),
                    )
                    .into(),
                )
            },
        ])
    }));

    // TODO: recover the original user statement
    Ok(PgResponse::new_for_stream(
        StatementType::DESCRIBE_TABLE,
        Some(rows.len() as i32),
        rows.into(),
        vec![
            PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar),
            PgFieldDescriptor::new("Type".to_owned(), TypeOid::Varchar),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

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
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = HashMap::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.insert(
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                );
            }
        }

        let expected_columns: HashMap<String, String> = maplit::hashmap! {
            "v1".into() => "Int32".into(),
            "v2".into() => "Int32".into(),
            "idx1".into() => "index(v1, v2) distributed by(v1, v2)".into(),
        };

        assert_eq!(columns, expected_columns);
    }
}
