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
use risingwave_sqlparser::ast::{Ident, ShowObject};

use crate::session::OptimizerContext;

fn schema_or_default(schema: &Option<Ident>) -> &str {
    schema
        .as_ref()
        .map_or_else(|| DEFAULT_SCHEMA_NAME, |s| &s.value)
}

pub async fn handle_show_object(
    context: OptimizerContext,
    command: ShowObject,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader().read_guard();

    let names = match command {
        // If not include schema name, use default schema name
        ShowObject::Table { schema } => catalog_reader
            .get_schema_by_name(session.database(), schema_or_default(&schema))?
            .iter_table()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Database => catalog_reader.get_all_database_names(),
        ShowObject::Schema => catalog_reader.get_all_schema_names(session.database())?,
        // If not include schema name, use default schema name
        ShowObject::MaterializedView { schema } => catalog_reader
            .get_schema_by_name(session.database(), schema_or_default(&schema))?
            .iter_mv()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Source { schema } => catalog_reader
            .get_schema_by_name(session.database(), schema_or_default(&schema))?
            .iter_source()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::MaterializedSource { schema } => catalog_reader
            .get_schema_by_name(session.database(), schema_or_default(&schema))?
            .iter_materialized_source()
            .map(|t| t.name.clone())
            .collect(),
    };

    let rows = names
        .into_iter()
        .map(|n| Row::new(vec![Some(n)]))
        .collect_vec();

    Ok(PgResponse::new(
        StatementType::SHOW_COMMAND,
        rows.len() as i32,
        rows,
        vec![PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar)],
    ))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_show_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = r#"CREATE SOURCE t1
        WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
        ROW FORMAT JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE MATERIALIZED SOURCE t2
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let mut rows = frontend.query_formatted_result("SHOW SOURCES").await;
        rows.sort();
        assert_eq!(
            rows,
            vec![
                "Row([Some(\"t1\")])".to_string(),
                "Row([Some(\"t2\")])".to_string()
            ]
        );

        let rows = frontend
            .query_formatted_result("SHOW MATERIALIZED SOURCES")
            .await;
        assert_eq!(rows, vec!["Row([Some(\"t2\")])".to_string()]);
    }
}
