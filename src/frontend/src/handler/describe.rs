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
use crate::catalog::table_catalog::TableCatalog;
use crate::session::OptimizerContext;

/// Convert column descs to rows which conclude name and type
pub fn col_descs_to_rows(columns: Vec<ColumnDesc>) -> Vec<Row> {
    let mut rows = vec![];
    for col in columns {
        rows.extend(col.flatten().into_iter().map(|c| {
            let type_name = {
                // If datatype is struct, use type name as struct name
                if let DataType::Struct { fields: _f } = c.data_type {
                    c.type_name.clone()
                } else {
                    format!("{:?}", &c.data_type)
                }
            };
            Row::new(vec![Some(c.name), Some(type_name)])
        }));
    }
    rows
}

pub async fn handle_describe(
    context: OptimizerContext,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();

    // For Source, it doesn't have table catalog so use get source to get column descs.
    let (columns, indexs): (Vec<ColumnDesc>, Vec<TableCatalog>) = {
        let (catalogs, indexs) = match catalog_reader
            .get_schema_by_name(session.database(), &schema_name)?
            .get_table_by_name(&table_name)
        {
            Some(table) => (
                &table.columns,
                catalog_reader
                    .get_schema_by_name(session.database(), &schema_name)?
                    .get_index_by_id(&table.id),
            ),
            None => (
                &catalog_reader
                    .get_source_by_name(session.database(), &schema_name, &table_name)?
                    .columns,
                None,
            ),
        };
        let indexs = match indexs {
            None => {
                vec![]
            }
            Some(v) => v.clone(),
        };
        (
            catalogs
                .iter()
                .filter(|c| !c.is_hidden)
                .map(|c| c.column_desc.clone())
                .collect(),
            indexs,
        )
    };

    // Convert all column descs to rows
    let mut rows = col_descs_to_rows(columns);

    rows.append(
        &mut indexs
            .iter()
            .map(|i| {
                let mut s = "".to_string();
                i.distribution_keys.iter().for_each(|c| {
                    s = s.clone() + i.columns[*c].name() + ",";
                });
                s = s[0..s.len() - 1].to_string();
                Row::new(vec![Some(i.name.clone()), Some(format!("index({})", s))])
            })
            .collect_vec(),
    );

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_describe_handler_source() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

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
            "id" => "Int32",
            "country.zipcode" => "Varchar",
            "zipcode" => "Int64",
            "country.city.address" => "Varchar",
            "country.address" => "Varchar",
            "country.city" => ".test.City",
            "country.city.zipcode" => "Varchar",
            "rate" => "Float32",
            "country" => ".test.Country",
        };

        assert_eq!(columns, expected_columns);
    }

    #[tokio::test]
    async fn test_describe_handler_table() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql("create table t (v1 int, v2 int);").await.unwrap();

        frontend.run_sql("create index idx1 on t (v1,v2);").await.unwrap();

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
            "idx1" => "index(v1,v2)",
        };

        assert_eq!(columns, expected_columns);

    }
}
