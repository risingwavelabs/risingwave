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
use risingwave_common::catalog::{ColumnDesc, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, ObjectName, ShowObject};

use crate::binder::Binder;
use crate::handler::util::col_descs_to_rows;
use crate::session::{OptimizerContext, SessionImpl};

pub fn get_columns_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<ColumnDesc>> {
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();
    let catalogs = match catalog_reader
        .get_schema_by_name(session.database(), &schema_name)?
        .get_table_by_name(&table_name)
    {
        Some(table) => &table.columns,
        None => {
            &catalog_reader
                .get_source_by_name(session.database(), &schema_name, &table_name)?
                .columns
        }
    };
    Ok(catalogs
        .iter()
        .filter(|c| !c.is_hidden)
        .map(|c| c.column_desc.clone())
        .collect())
}

fn schema_or_default(schema: &Option<Ident>) -> String {
    schema
        .as_ref()
        .map_or_else(|| DEFAULT_SCHEMA_NAME.to_string(), |s| s.real_value())
}

pub fn handle_show_object(context: OptimizerContext, command: ShowObject) -> Result<PgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader().read_guard();

    let names = match command {
        // If not include schema name, use default schema name
        ShowObject::Table { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_table()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Database => catalog_reader.get_all_database_names(),
        ShowObject::Schema => catalog_reader.get_all_schema_names(session.database())?,
        // If not include schema name, use default schema name
        ShowObject::MaterializedView { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_mv()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Source { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_source()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::MaterializedSource { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_materialized_source()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Sink { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_sink()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Columns { table } => {
            let columns = get_columns_from_table(&session, table)?;
            let rows = col_descs_to_rows(columns);

            return Ok(PgResponse::new(
                StatementType::SHOW_COMMAND,
                Some(rows.len() as i32),
                rows,
                vec![
                    PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar),
                    PgFieldDescriptor::new("Type".to_owned(), TypeOid::Varchar),
                ],
            ));
        }
    };

    let rows = names
        .into_iter()
        .map(|n| Row::new(vec![Some(n.into())]))
        .collect_vec();

    Ok(PgResponse::new(
        StatementType::SHOW_COMMAND,
        Some(rows.len() as i32),
        rows,
        vec![PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar)],
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_show_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = r#"CREATE SOURCE t1
        WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
        ROW FORMAT JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE MATERIALIZED SOURCE t2
    WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let mut rows = frontend.query_formatted_result("SHOW SOURCES").await;
        rows.sort();
        assert_eq!(
            rows,
            vec![
                "Row([Some(b\"t1\")])".to_string(),
                "Row([Some(b\"t2\")])".to_string()
            ]
        );

        let rows = frontend
            .query_formatted_result("SHOW MATERIALIZED SOURCES")
            .await;
        assert_eq!(rows, vec!["Row([Some(b\"t2\")])".to_string()]);
    }

    #[tokio::test]
    async fn test_show_column() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "show columns from t";
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
            "id".into() => "Int32".into(),
            "country.zipcode".into() => "Varchar".into(),
            "zipcode".into() => "Int64".into(),
            "country.city.address".into() => "Varchar".into(),
            "country.address".into() => "Varchar".into(),
            "country.city".into() => ".test.City".into(),
            "country.city.zipcode".into() => "Varchar".into(),
            "rate".into() => "Float32".into(),
            "country".into() => ".test.Country".into(),

        };

        assert_eq!(columns, expected_columns);
    }
}
