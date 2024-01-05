// Copyright 2024 RisingWave Labs
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

use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::{EncodeType, FormatType};
use risingwave_sqlparser::ast::{
    CompatibleSourceSchema, ConnectorSchema, CreateSourceStatement, Encode, Format, ObjectName,
    SqlOption, Statement,
};
use risingwave_sqlparser::parser::Parser;

use super::alter_table_column::schema_has_schema_registry;
use super::create_source::{bind_columns_from_source, validate_compatibility};
use super::util::is_cdc_connector;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::{Binder, WithOptions};

fn format_type_to_format(from: FormatType) -> Option<Format> {
    Some(match from {
        FormatType::Unspecified => return None,
        FormatType::Native => Format::Native,
        FormatType::Debezium => Format::Debezium,
        FormatType::DebeziumMongo => Format::DebeziumMongo,
        FormatType::Maxwell => Format::Maxwell,
        FormatType::Canal => Format::Canal,
        FormatType::Upsert => Format::Upsert,
        FormatType::Plain => Format::Plain,
    })
}

fn encode_type_to_encode(from: EncodeType) -> Option<Encode> {
    Some(match from {
        EncodeType::Unspecified => return None,
        EncodeType::Native => Encode::Native,
        EncodeType::Avro => Encode::Avro,
        EncodeType::Csv => Encode::Csv,
        EncodeType::Protobuf => Encode::Protobuf,
        EncodeType::Json => Encode::Json,
        EncodeType::Bytes => Encode::Bytes,
        EncodeType::Template => Encode::Template,
    })
}

/// Returns the columns in `columns_a` but not in `columns_b`,
/// where the comparison is done by name and data type,
/// and hidden columns are ignored.
fn columns_diff(columns_a: &[ColumnCatalog], columns_b: &[ColumnCatalog]) -> Vec<ColumnCatalog> {
    columns_a
        .iter()
        .filter(|col_a| {
            !col_a.is_hidden()
                && !columns_b.iter().any(|col_b| {
                    col_a.name() == col_b.name() && col_a.data_type() == col_b.data_type()
                })
        })
        .cloned()
        .collect()
}

pub async fn handle_alter_source_with_sr(
    handler_args: HandlerArgs,
    name: ObjectName,
    connector_schema: ConnectorSchema,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (database_id, schema_id, mut source) = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;
        let db = reader.get_database_by_name(db_name)?;
        let schema = db.get_schema_by_name(schema_name).unwrap();

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        (db.id(), schema.id(), (**source).clone())
    };

    if source.associated_table_id.is_some() {
        bail_not_implemented!("altering source associated with table is not supported yet");
    }

    let StreamSourceInfo {
        format, row_encode, ..
    } = source.info;
    let (Some(old_format), Some(old_row_encode)) = (
        format_type_to_format(FormatType::try_from(format).unwrap()),
        encode_type_to_encode(EncodeType::try_from(row_encode).unwrap()),
    ) else {
        return Err(ErrorCode::NotSupported(
            "altering a legacy source which is not created using `FORMAT .. ENCODE ..` Clause"
                .to_string(),
            "try this feature by creating a fresh source".to_string(),
        )
        .into());
    };

    if connector_schema.format != old_format || connector_schema.row_encode != old_row_encode {
        bail_not_implemented!(
            "the original definition is FORMAT {:?} ENCODE {:?}, and altering them is not supported yet",
            &old_format,
            &old_row_encode,
        );
    }

    if !schema_has_schema_registry(&connector_schema) {
        return Err(ErrorCode::NotSupported(
            "altering a source without schema registry".to_string(),
            "try `ALTER SOURCE .. ADD COLUMNS ...`".to_string(),
        )
        .into());
    }

    let mut with_properties = source.with_properties.clone().into_iter().collect();
    validate_compatibility(&connector_schema, &mut with_properties)?;

    if is_cdc_connector(&with_properties) {
        bail_not_implemented!("altering a cdc source is not supported");
    }

    let (Some(columns_from_resolve_source), source_info) =
        bind_columns_from_source(&session, &connector_schema, &with_properties, false).await?
    else {
        // Source without schema registry is rejected.
        unreachable!()
    };

    let added_columns = columns_diff(&columns_from_resolve_source, &source.columns);
    let dropped_columns = columns_diff(&source.columns, &columns_from_resolve_source);

    if !dropped_columns.is_empty() {
        bail_not_implemented!(
            "this altering statement will drop columns, which is not supported yet: {}",
            dropped_columns
                .iter()
                .map(|col| format!("({}: {})", col.name(), col.data_type()))
                .join(", ")
        );
    }

    source.info = source_info;
    source.columns.extend(added_columns);
    source.definition =
        alter_definition_format_encode(&source.definition, connector_schema.row_options.clone())?;

    let format_encode_options = WithOptions::try_from(connector_schema.row_options())?.into_inner();
    source
        .info
        .format_encode_options
        .extend(format_encode_options);

    let mut pb_source = source.to_prost(schema_id, database_id);

    // update version
    pb_source.version += 1;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_source_format_encode(pb_source).await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_SOURCE))
}

pub fn alter_definition_format_encode(
    definition: &str,
    row_options: Vec<SqlOption>,
) -> Result<String> {
    let ast = Parser::parse_sql(definition).expect("failed to parse relation definition");
    let mut stmt = ast
        .into_iter()
        .exactly_one()
        .expect("should contains only one statement");

    match &mut stmt {
        Statement::CreateSource {
            stmt: CreateSourceStatement { source_schema, .. },
        } => {
            match source_schema {
                CompatibleSourceSchema::V2(schema) => {
                    schema.row_options = row_options;
                }
                // TODO: Confirm the behavior of legacy source schema.
                // Legacy source schema should be rejected by the handler and never reaches here.
                CompatibleSourceSchema::RowFormat(_schema) => unreachable!(),
            }
        }
        _ => unreachable!(),
    }

    Ok(stmt.to_string())
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_connector::source::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_alter_source_column_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE src
            WITH (
                connector = 'kafka',
                topic = 'test-topic',
                properties.bootstrap.server = 'localhost:29092'
            )
            FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        frontend.run_sql(sql).await.unwrap();

        let get_source = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "src")
                .unwrap()
                .0
                .clone()
        };

        let sql = format!(
            r#"ALTER SOURCE src FORMAT UPSERT ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        assert!(frontend
            .run_sql(sql)
            .await
            .unwrap_err()
            .to_string()
            .contains("the original definition is FORMAT Plain ENCODE Protobuf"));

        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordAlterType',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let res_str = frontend.run_sql(sql).await.unwrap_err().to_string();
        assert!(res_str.contains("id: integer"));
        assert!(res_str.contains("zipcode: bigint"));

        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordExt',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        frontend.run_sql(sql).await.unwrap();

        let altered_source = get_source();

        let name_column = altered_source
            .columns
            .iter()
            .find(|col| col.column_desc.name == "name")
            .unwrap();
        assert_eq!(name_column.column_desc.data_type, DataType::Varchar);

        let altered_sql = format!(
            r#"CREATE SOURCE src WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecordExt', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        assert_eq!(altered_sql, altered_source.definition);
    }
}
