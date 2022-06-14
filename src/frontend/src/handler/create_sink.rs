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

use std::collections::HashMap;

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::sink::Info;
use risingwave_pb::catalog::{Sink as ProstSink, StreamSinkInfo};
use risingwave_pb::plan_common::{ColumnCatalog as ProstColumnCatalog, RowFormatType};
use risingwave_sqlparser::ast::{CreateSinkStatement, ObjectName, SqlOption, Value};

use super::create_table::bind_sql_columns;
use crate::binder::Binder;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::session::{OptimizerContext, SessionImpl};
use crate::stream_fragmenter::StreamFragmenter;

pub(crate) fn make_prost_sink(
    session: &SessionImpl,
    name: ObjectName,
    sink_info: Info,
) -> Result<ProstSink> {
    let (schema_name, name) = Binder::resolve_table_name(name)?;

    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, &name)?;

    Ok(ProstSink {
        id: 0,
        schema_id,
        database_id,
        name,
        info: Some(sink_info),
    })
}

pub async fn handle_create_sink(
    context: OptimizerContext,
    stmt: CreateSinkStatement,
) -> Result<PgResponse> {
    todo!();
    // let sink = match &stmt.sink_schema {
    //     SinkSchema::Protobuf(protobuf_schema) => {
    //         let mut columns = vec![ColumnCatalog::row_id_column().to_protobuf()];
    //         columns.extend(extract_protobuf_table_schema(protobuf_schema)?.into_iter());
    //         StreamSinkInfo {
    //             properties: handle_sink_with_properties(stmt.with_properties.0)?,
    //             row_format: RowFormatType::Protobuf as i32,
    //             row_schema_location: protobuf_schema.row_schema_location.0.clone(),
    //             row_id_index: 0,
    //             columns,
    //             pk_column_ids: vec![0],
    //         }
    //     }
    //     SinkSchema::Json => StreamSinkInfo {
    //         properties: handle_sink_with_properties(stmt.with_properties.0)?,
    //         row_format: RowFormatType::Json as i32,
    //         row_schema_location: "".to_string(),
    //         row_id_index: 0,
    //         columns: bind_sql_columns(stmt.columns)?,
    //         pk_column_ids: vec![0],
    //     },
    // };

    // let session = context.session_ctx.clone();
    // let sink = make_prost_sink(&session, stmt.sink_name, Info::StreamSink(sink))?;
    // let catalog_writer = session.env().catalog_writer();

    // catalog_writer.create_sink(sink).await?;

    // Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::row_id_column_name;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    // #[tokio::test]
    // async fn test_create_sink_handler() {
    //     let proto_file = create_proto_file(PROTO_FILE_DATA);
    //     let sql = r#"CREATE SINK sink FROM mv
    //     WITH ('sink' = 'mysql', 'mysql.endpoint' = '127.0.0.1:3306', 'mysql.table' =
    // '<table_name>', 'mysql.database' = '<database_name>', 'mysql.user' = '<user_name>',
    // 'mysql.password' = '<password>')"#.into();     let frontend =
    // LocalFrontend::new(Default::default()).await;     frontend.run_sql(sql).await.unwrap();

    //     let session = frontend.session_ref();
    //     let catalog_reader = session.env().catalog_reader();

    //     // Check sink exists.
    //     let sink = catalog_reader
    //         .read_guard()
    //         .get_sink_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
    //         .unwrap()
    //         .clone();
    //     assert_eq!(sink.name, "t");

    //     // Only check stream sink
    //     let catalogs = sink.columns;
    //     let mut columns = vec![];

    //     // Get all column descs
    //     for catalog in catalogs {
    //         columns.append(&mut catalog.column_desc.flatten());
    //     }
    //     let columns = columns
    //         .iter()
    //         .map(|col| (col.name.as_str(), col.data_type.clone()))
    //         .collect::<HashMap<&str, DataType>>();

    //     let city_type = DataType::Struct {
    //         fields: vec![DataType::Varchar, DataType::Varchar].into(),
    //     };
    //     let row_id_col_name = row_id_column_name();
    //     let expected_columns = maplit::hashmap! {
    //         row_id_col_name.as_str() => DataType::Int64,
    //         "id" => DataType::Int32,
    //         "country.zipcode" => DataType::Varchar,
    //         "zipcode" => DataType::Int64,
    //         "country.city.address" => DataType::Varchar,
    //         "country.address" => DataType::Varchar,
    //         "country.city" => city_type.clone(),
    //         "country.city.zipcode" => DataType::Varchar,
    //         "rate" => DataType::Float32,
    //         "country" => DataType::Struct
    // {fields:vec![DataType::Varchar,city_type,DataType::Varchar].into()},     };
    //     assert_eq!(columns, expected_columns);
    // }
}
