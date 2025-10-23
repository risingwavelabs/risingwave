// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{ColumnCatalog, max_column_id};
use risingwave_common::types::DataType;
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::{EncodeType, FormatType};
use risingwave_sqlparser::ast::{
    CompatibleFormatEncode, CreateSourceStatement, Encode, Format, FormatEncodeOptions, ObjectName,
    SqlOption, Statement,
};

use super::create_source::{
    generate_stream_graph_for_source, schema_has_schema_registry, validate_compatibility,
};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::create_source::{CreateSourceType, bind_columns_from_source};
use crate::session::SessionImpl;
use crate::utils::resolve_secret_ref_in_with_options;
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
        FormatType::None => Format::None,
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
        EncodeType::Parquet => Encode::Parquet,
        EncodeType::None => Encode::None,
        EncodeType::Text => Encode::Text,
    })
}

/// Categorized column changes between two schemas.
#[derive(Debug)]
pub struct ColumnChanges {
    /// Columns that exist in the new schema but not in the old schema.
    pub added: Vec<ColumnCatalog>,
    /// Columns that exist in the old schema but not in the new schema.
    pub dropped: Vec<ColumnCatalog>,
    /// Columns that exist in both schemas but with incompatible type changes.
    /// Each tuple is (`old_column`, `new_column`).
    pub modified: Vec<(ColumnCatalog, ColumnCatalog)>,
}

/// Checks if `new_type` is backwards compatible with `old_type` according to Protobuf wire-format rules.
///
/// This implements the "binary wire-safe changes" as specified in the Protobuf Language Guide:
/// <https://protobuf.dev/programming-guides/proto3/#updating>
///
/// Key compatibility rules:
/// - For struct types: new struct can have additional optional fields, but all fields from
///   the old struct must be present with compatible types
/// - For simple types: must match exactly
/// - For list/map types: element types must be recursively compatible
///
/// Returns `Ok(())` if compatible, `Err(reason)` if incompatible.
fn is_backwards_compatible(
    old_type: &DataType,
    new_type: &DataType,
) -> std::result::Result<(), String> {
    use DataType::*;

    match (old_type, new_type) {
        // Exact matches for simple types
        (Boolean, Boolean)
        | (Int16, Int16)
        | (Int32, Int32)
        | (Int64, Int64)
        | (Float32, Float32)
        | (Float64, Float64)
        | (Decimal, Decimal)
        | (Date, Date)
        | (Varchar, Varchar)
        | (Time, Time)
        | (Timestamp, Timestamp)
        | (Timestamptz, Timestamptz)
        | (Interval, Interval)
        | (Bytea, Bytea)
        | (Jsonb, Jsonb)
        | (Serial, Serial)
        | (Int256, Int256) => Ok(()),

        // Struct type compatibility: new struct can have additional fields
        (Struct(old_struct), Struct(new_struct)) => {
            // Build a map of field names to types for efficient lookup
            let new_fields: HashMap<&str, &DataType> = new_struct.iter().collect();

            // Check that all old fields exist in the new struct with compatible types
            for (old_field_name, old_field_type) in old_struct.iter() {
                match new_fields.get(old_field_name) {
                    Some(new_field_type) => {
                        // Recursively check field type compatibility
                        if let Err(reason) = is_backwards_compatible(old_field_type, new_field_type)
                        {
                            return Err(format!(
                                "field '{}' has incompatible type change: {}",
                                old_field_name, reason
                            ));
                        }
                    }
                    None => {
                        return Err(format!(
                            "field '{}' was removed (type was {})",
                            old_field_name, old_field_type
                        ));
                    }
                }
            }

            // Adding new fields is allowed (they are optional in Protobuf)
            Ok(())
        }

        // List type compatibility: element types must be compatible
        (List(old_inner), List(new_inner)) => {
            is_backwards_compatible(old_inner.elem(), new_inner.elem())
                .map_err(|reason| format!("list element type incompatible: {}", reason))
        }

        // Map type compatibility: key and value types must be compatible
        (Map(old_map), Map(new_map)) => {
            // Check key type compatibility
            is_backwards_compatible(old_map.key(), new_map.key())
                .map_err(|reason| format!("map key type incompatible: {}", reason))?;

            // Check value type compatibility
            is_backwards_compatible(old_map.value(), new_map.value())
                .map_err(|reason| format!("map value type incompatible: {}", reason))
        }

        // Type category mismatch
        _ => Err(format!(
            "incompatible type change from {} to {}",
            old_type, new_type
        )),
    }
}

/// Categorizes column changes between old and new schemas.
///
/// This function distinguishes between:
/// - Added columns: present in new schema but not in old
/// - Dropped columns: present in old schema but not in new
/// - Modified columns: present in both but with incompatible type changes
///
/// Columns with compatible type changes (e.g., adding optional fields to structs)
/// are not considered modified.
///
/// Notes:
/// - Hidden columns, `INCLUDE ... AS ...` columns, and generated columns are ignored.
///   For the newly resolved `columns_from_resolve_source` (created by [`bind_columns_from_source`]),
///   it doesn't contain hidden columns (`_row_id`) and `INCLUDE ... AS ...` columns.
///   This is fragile and we should really refactor it later.
/// - The comparison is done by name and data type compatibility, without checking `ColumnId`.
fn categorize_column_changes(
    old_columns: &[ColumnCatalog],
    new_columns: &[ColumnCatalog],
) -> ColumnChanges {
    // Filter out columns we should ignore
    let old_relevant: Vec<_> = old_columns
        .iter()
        .filter(|col| {
            !col.is_hidden() && !col.is_connector_additional_column() && !col.is_generated()
        })
        .collect();

    let new_relevant: Vec<_> = new_columns
        .iter()
        .filter(|col| {
            !col.is_hidden() && !col.is_connector_additional_column() && !col.is_generated()
        })
        .collect();

    // Build maps for efficient lookup
    let old_by_name: HashMap<&str, &ColumnCatalog> =
        old_relevant.iter().map(|col| (col.name(), *col)).collect();
    let new_by_name: HashMap<&str, &ColumnCatalog> =
        new_relevant.iter().map(|col| (col.name(), *col)).collect();

    let mut added = Vec::new();
    let mut dropped = Vec::new();
    let mut modified = Vec::new();

    // Find added and modified columns
    for new_col in &new_relevant {
        match old_by_name.get(new_col.name()) {
            Some(old_col) => {
                // Column exists in both schemas, check compatibility
                if let Err(_reason) =
                    is_backwards_compatible(old_col.data_type(), new_col.data_type())
                {
                    // Incompatible change
                    modified.push(((*old_col).clone(), (*new_col).clone()));
                }
                // If compatible, no action needed (includes exact matches and compatible evolutions)
            }
            None => {
                // Column only exists in new schema
                added.push((*new_col).clone());
            }
        }
    }

    // Find dropped columns
    for old_col in &old_relevant {
        if !new_by_name.contains_key(old_col.name()) {
            dropped.push((*old_col).clone());
        }
    }

    ColumnChanges {
        added,
        dropped,
        modified,
    }
}

/// Fetch the source catalog.
pub fn fetch_source_catalog_with_db_schema_id(
    session: &SessionImpl,
    name: &ObjectName,
) -> Result<Arc<SourceCatalog>> {
    let db_name = &session.database();
    let (schema_name, real_source_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let reader = session.env().catalog_reader().read_guard();
    let (source, schema_name) =
        reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

    session.check_privilege_for_drop_alter(schema_name, &**source)?;

    Ok(Arc::clone(source))
}

/// Check if the original source is created with `FORMAT .. ENCODE ..` clause,
/// and if the FORMAT and ENCODE are modified.
pub fn check_format_encode(
    original_source: &SourceCatalog,
    new_format_encode: &FormatEncodeOptions,
) -> Result<()> {
    let StreamSourceInfo {
        format, row_encode, ..
    } = original_source.info;
    let (Some(old_format), Some(old_row_encode)) = (
        format_type_to_format(FormatType::try_from(format).unwrap()),
        encode_type_to_encode(EncodeType::try_from(row_encode).unwrap()),
    ) else {
        return Err(ErrorCode::NotSupported(
            "altering a legacy source which is not created using `FORMAT .. ENCODE ..` Clause"
                .to_owned(),
            "try this feature by creating a fresh source".to_owned(),
        )
        .into());
    };

    if new_format_encode.format != old_format || new_format_encode.row_encode != old_row_encode {
        bail_not_implemented!(
            "the original definition is FORMAT {:?} ENCODE {:?}, and altering them is not supported yet",
            &old_format,
            &old_row_encode,
        );
    }

    Ok(())
}

/// Refresh the source registry and get the categorized column changes.
///
/// Returns:
/// - `StreamSourceInfo`: The new source info from the schema registry
/// - `ColumnChanges`: Categorized changes (added, dropped, modified)
/// - `Vec<ColumnCatalog>`: The complete resolved columns from the schema registry
pub async fn refresh_sr_and_get_columns_diff(
    original_source: &SourceCatalog,
    format_encode: &FormatEncodeOptions,
    session: &Arc<SessionImpl>,
) -> Result<(StreamSourceInfo, ColumnChanges, Vec<ColumnCatalog>)> {
    let mut with_properties = original_source.with_properties.clone();
    validate_compatibility(format_encode, &mut with_properties)?;

    if with_properties.is_cdc_connector() {
        bail_not_implemented!("altering a cdc source is not supported");
    }

    let (Some(columns_from_resolve_source), source_info) = bind_columns_from_source(
        session,
        format_encode,
        Either::Right(&with_properties),
        CreateSourceType::for_replace(original_source),
    )
    .await?
    else {
        // Source without schema registry is rejected.
        unreachable!("source without schema registry is rejected")
    };

    let mut changes =
        categorize_column_changes(&original_source.columns, &columns_from_resolve_source);

    // The newly resolved columns' column IDs also start from 1. They cannot be used directly.
    // Assign proper column IDs to added columns.
    let mut next_col_id = max_column_id(&original_source.columns).next();
    for col in &mut changes.added {
        col.column_desc.column_id = next_col_id;
        next_col_id = next_col_id.next();
    }

    tracing::debug!(
        added_columns = ?changes.added,
        dropped_columns = ?changes.dropped,
        modified_columns = ?changes.modified,
        ?columns_from_resolve_source,
        original_source = ?original_source.columns
    );

    Ok((source_info, changes, columns_from_resolve_source))
}

fn get_format_encode_from_source(source: &SourceCatalog) -> Result<FormatEncodeOptions> {
    let stmt = source.create_sql_ast()?;
    let Statement::CreateSource {
        stmt: CreateSourceStatement { format_encode, .. },
    } = stmt
    else {
        unreachable!()
    };
    Ok(format_encode.into_v2_with_warning())
}

pub async fn handler_refresh_schema(
    handler_args: HandlerArgs,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let source = fetch_source_catalog_with_db_schema_id(&handler_args.session, &name)?;
    let format_encode = get_format_encode_from_source(&source)?;
    handle_alter_source_with_sr(handler_args, name, format_encode).await
}

pub async fn handle_alter_source_with_sr(
    handler_args: HandlerArgs,
    name: ObjectName,
    format_encode: FormatEncodeOptions,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let source = fetch_source_catalog_with_db_schema_id(&session, &name)?;
    let mut source = source.as_ref().clone();

    if source.associated_table_id.is_some() {
        return Err(ErrorCode::NotSupported(
            "alter table with connector using ALTER SOURCE statement".to_owned(),
            "try to use ALTER TABLE instead".to_owned(),
        )
        .into());
    };

    check_format_encode(&source, &format_encode)?;

    if !schema_has_schema_registry(&format_encode) {
        return Err(ErrorCode::NotSupported(
            "altering a source without schema registry".to_owned(),
            "try `ALTER SOURCE .. ADD COLUMN ...` instead".to_owned(),
        )
        .into());
    }

    let (source_info, changes, columns_from_resolve_source) =
        refresh_sr_and_get_columns_diff(&source, &format_encode, &session).await?;

    // Check for incompatible changes and provide detailed error messages
    // Note: changes.modified only contains INCOMPATIBLE changes because
    // categorize_column_changes filters out compatible ones
    if !changes.modified.is_empty() {
        let modified_details = changes
            .modified
            .iter()
            .map(|(old_col, new_col)| {
                let reason =
                    is_backwards_compatible(old_col.data_type(), new_col.data_type()).unwrap_err();
                format!("column '{}': {}", old_col.name(), reason)
            })
            .join("; ");

        bail_not_implemented!(
            "this altering statement contains incompatible type changes, which are not supported: {}",
            modified_details
        );
    }

    if !changes.dropped.is_empty() {
        bail_not_implemented!(
            "this altering statement will drop columns, which is not supported yet: {}",
            changes
                .dropped
                .iter()
                .map(|col| format!("'{}'", col.name()))
                .join(", ")
        );
    }

    source.info = source_info;

    // Add new columns to the source
    source.columns.extend(changes.added);

    // Update existing columns that have compatible type changes (e.g., nested struct evolution).
    // The columns_from_resolve_source contains the complete new schema from the schema registry.
    // We update existing columns with evolved types while preserving their column IDs.
    for resolved_col in columns_from_resolve_source {
        if let Some(existing_col) = source
            .columns
            .iter_mut()
            .find(|c| c.name() == resolved_col.name())
        {
            // Preserve the column ID but update the type
            let col_id = existing_col.column_desc.column_id;
            existing_col.column_desc = resolved_col.column_desc;
            existing_col.column_desc.column_id = col_id;
        }
    }
    source.definition = alter_definition_format_encode(
        source.create_sql_ast_purified()?,
        format_encode.row_options.clone(),
    )?;

    let (format_encode_options, format_encode_secret_ref) = resolve_secret_ref_in_with_options(
        WithOptions::try_from(format_encode.row_options())?,
        session.as_ref(),
    )?
    .into_parts();
    source
        .info
        .format_encode_options
        .extend(format_encode_options);

    source
        .info
        .format_encode_secret_refs
        .extend(format_encode_secret_ref);

    // update version
    source.version += 1;

    let pb_source = source.to_prost();
    let catalog_writer = session.catalog_writer()?;
    if source.info.is_shared() {
        let graph = generate_stream_graph_for_source(handler_args, source.clone())?;
        catalog_writer.replace_source(pb_source, graph).await?
    } else {
        catalog_writer.alter_source(pb_source).await?;
    }
    Ok(RwPgResponse::empty_result(StatementType::ALTER_SOURCE))
}

/// Apply the new `format_encode_options` to the source/table definition.
pub fn alter_definition_format_encode(
    mut stmt: Statement,
    format_encode_options: Vec<SqlOption>,
) -> Result<String> {
    match &mut stmt {
        Statement::CreateSource {
            stmt: CreateSourceStatement { format_encode, .. },
        }
        | Statement::CreateTable {
            format_encode: Some(format_encode),
            ..
        } => {
            match format_encode {
                CompatibleFormatEncode::V2(schema) => {
                    schema.row_options = format_encode_options;
                }
                // TODO: Confirm the behavior of legacy source schema.
                // Legacy source schema should be rejected by the handler and never reaches here.
                CompatibleFormatEncode::RowFormat(_schema) => unreachable!(),
            }
        }
        _ => unreachable!(),
    }

    Ok(stmt.to_string())
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[tokio::test]
    async fn test_alter_source_with_sr_handler() {
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

        frontend
            .run_sql_with_session(session.clone(), "SET streaming_use_shared_source TO false;")
            .await
            .unwrap();
        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();

        let get_source = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "src")
                .unwrap()
                .0
                .clone()
        };

        let source = get_source();
        expect_test::expect!["CREATE SOURCE src (id INT, country STRUCT<address CHARACTER VARYING, city STRUCT<address CHARACTER VARYING, zipcode CHARACTER VARYING>, zipcode CHARACTER VARYING>, zipcode BIGINT, rate REAL) WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://')"].assert_eq(&source.create_sql_purified().replace(proto_file.path().to_str().unwrap(), ""));

        let sql = format!(
            r#"ALTER SOURCE src FORMAT UPSERT ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        assert!(
            frontend
                .run_sql(sql)
                .await
                .unwrap_err()
                .to_string()
                .contains("the original definition is FORMAT Plain ENCODE Protobuf")
        );

        // Test incompatible type changes - TestRecordAlterType changes id from int32 to string
        // and zipcode from int64 to int32, which should be rejected with clear error messages
        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordAlterType',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let res_str = frontend.run_sql(sql).await.unwrap_err().to_string();
        // Should now have clear error messages about incompatible type changes
        assert!(
            res_str.contains("incompatible type changes"),
            "Error message should mention incompatible type changes, got: {}",
            res_str
        );
        assert!(
            res_str.contains("column 'id'") || res_str.contains("column 'zipcode'"),
            "Error message should mention the modified columns, got: {}",
            res_str
        );

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

        expect_test::expect!["CREATE SOURCE src (id INT, country STRUCT<address CHARACTER VARYING, city STRUCT<address CHARACTER VARYING, zipcode CHARACTER VARYING>, zipcode CHARACTER VARYING>, zipcode BIGINT, rate REAL, name CHARACTER VARYING) WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecordExt', schema.location = 'file://')"].assert_eq(&altered_source.create_sql_purified().replace(proto_file.path().to_str().unwrap(), ""));

        // Test that adding fields to nested structs is properly handled as a compatible change
        // TestRecordAddNestedColumns uses CountryExt and CityExt which have additional 'name' fields
        // compared to the original Country and City types. This should be accepted as a
        // compatible schema evolution since we're only adding optional fields to nested structs.
        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordAddNestedColumns',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );

        // This should succeed because adding fields to nested structs (Country -> CountryExt,
        // City -> CityExt) is a compatible change according to Protobuf rules
        frontend.run_sql(sql).await.unwrap();

        let altered_source_nested = get_source();

        // Verify the country column is now a struct with the additional nested fields
        let country_column = altered_source_nested
            .columns
            .iter()
            .find(|col| col.column_desc.name == "country")
            .unwrap();

        // It should still be a struct type, but now with the extended nested structure
        if let DataType::Struct(country_struct) = &country_column.column_desc.data_type {
            // Country/CountryExt should have: address, city, zipcode, and now 'name'
            let field_names: Vec<&str> = country_struct.iter().map(|(name, _)| name).collect();
            assert!(
                field_names.contains(&"name"),
                "CountryExt should have 'name' field, got fields: {:?}",
                field_names
            );

            // Check that the nested City has also been extended to CityExt with 'name' field
            let city_field = country_struct.iter().find(|(name, _)| name == &"city");
            if let Some((_, DataType::Struct(city_struct))) = city_field {
                let city_field_names: Vec<&str> =
                    city_struct.iter().map(|(name, _)| name).collect();
                assert!(
                    city_field_names.contains(&"name"),
                    "CityExt should have 'name' field, got fields: {:?}",
                    city_field_names
                );
            } else {
                panic!("city field should be a struct type");
            }
        } else {
            panic!("country should be a struct type");
        }
    }

    #[test]
    fn test_protobuf_compatibility_simple_types() {
        use risingwave_common::types::DataType;

        use super::is_backwards_compatible;

        // Exact matches should be compatible
        assert!(is_backwards_compatible(&DataType::Int32, &DataType::Int32).is_ok());
        assert!(is_backwards_compatible(&DataType::Varchar, &DataType::Varchar).is_ok());
        assert!(is_backwards_compatible(&DataType::Boolean, &DataType::Boolean).is_ok());

        // Type changes should be incompatible
        assert!(is_backwards_compatible(&DataType::Int32, &DataType::Int64).is_err());
        assert!(is_backwards_compatible(&DataType::Int32, &DataType::Varchar).is_err());
        assert!(is_backwards_compatible(&DataType::Varchar, &DataType::Int32).is_err());
    }

    #[test]
    fn test_protobuf_compatibility_struct_adding_fields() {
        use risingwave_common::types::{DataType, StructType};

        use super::is_backwards_compatible;

        // Old struct: {a: int32, b: varchar}
        let old_struct = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
        ]));

        // New struct: {a: int32, b: varchar, c: int64} - adding field c
        let new_struct_with_extra = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
            ("c", DataType::Int64),
        ]));

        // Adding fields should be compatible (Protobuf allows this)
        assert!(
            is_backwards_compatible(&old_struct, &new_struct_with_extra).is_ok(),
            "Adding optional fields to struct should be compatible"
        );
    }

    #[test]
    fn test_protobuf_compatibility_struct_removing_fields() {
        use risingwave_common::types::{DataType, StructType};

        use super::is_backwards_compatible;

        // Old struct: {a: int32, b: varchar, c: int64}
        let old_struct = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
            ("c", DataType::Int64),
        ]));

        // New struct: {a: int32, b: varchar} - removed field c
        let new_struct_missing = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
        ]));

        // Removing fields should be incompatible
        let result = is_backwards_compatible(&old_struct, &new_struct_missing);
        assert!(result.is_err(), "Removing fields should be incompatible");
        assert!(
            result.unwrap_err().contains("field 'c' was removed"),
            "Error should mention the removed field"
        );
    }

    #[test]
    fn test_protobuf_compatibility_struct_changing_field_type() {
        use risingwave_common::types::{DataType, StructType};

        use super::is_backwards_compatible;

        // Old struct: {a: int32, b: varchar}
        let old_struct = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
        ]));

        // New struct: {a: int64, b: varchar} - changed field a type
        let new_struct_changed = DataType::Struct(StructType::new([
            ("a", DataType::Int64),
            ("b", DataType::Varchar),
        ]));

        // Changing field types should be incompatible
        let result = is_backwards_compatible(&old_struct, &new_struct_changed);
        assert!(
            result.is_err(),
            "Changing field types should be incompatible"
        );
        assert!(
            result.unwrap_err().contains("field 'a'"),
            "Error should mention the changed field"
        );
    }

    #[test]
    fn test_protobuf_compatibility_nested_struct_adding_fields() {
        use risingwave_common::types::{DataType, StructType};

        use super::is_backwards_compatible;

        // Old struct: {outer: {inner: int32}}
        let old_inner = DataType::Struct(StructType::new([("inner", DataType::Int32)]));
        let old_struct = DataType::Struct(StructType::new([("outer", old_inner)]));

        // New struct: {outer: {inner: int32, extra: varchar}} - added field to nested struct
        let new_inner = DataType::Struct(StructType::new([
            ("inner", DataType::Int32),
            ("extra", DataType::Varchar),
        ]));
        let new_struct = DataType::Struct(StructType::new([("outer", new_inner)]));

        // Adding fields to nested structs should be compatible
        assert!(
            is_backwards_compatible(&old_struct, &new_struct).is_ok(),
            "Adding fields to nested structs should be compatible"
        );
    }

    #[test]
    fn test_protobuf_compatibility_list_types() {
        use risingwave_common::types::{DataType, ListType};

        use super::is_backwards_compatible;

        // Same list types should be compatible
        let old_list = DataType::List(ListType::new(DataType::Int32));
        let new_list = DataType::List(ListType::new(DataType::Int32));
        assert!(is_backwards_compatible(&old_list, &new_list).is_ok());

        // Different list element types should be incompatible
        let old_list_int = DataType::List(ListType::new(DataType::Int32));
        let new_list_varchar = DataType::List(ListType::new(DataType::Varchar));
        assert!(is_backwards_compatible(&old_list_int, &new_list_varchar).is_err());
    }

    #[test]
    fn test_protobuf_compatibility_list_of_structs_adding_fields() {
        use risingwave_common::types::{DataType, ListType, StructType};

        use super::is_backwards_compatible;

        // List of structs where struct gains a field
        let old_struct = DataType::Struct(StructType::new([("a", DataType::Int32)]));
        let old_list = DataType::List(ListType::new(old_struct));

        let new_struct = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("b", DataType::Varchar),
        ]));
        let new_list = DataType::List(ListType::new(new_struct));

        // Adding fields to struct elements in list should be compatible
        assert!(
            is_backwards_compatible(&old_list, &new_list).is_ok(),
            "Adding fields to struct elements in list should be compatible"
        );
    }

    #[test]
    fn test_categorize_column_changes() {
        use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
        use risingwave_common::types::{DataType, StructType};

        use super::categorize_column_changes;

        // Old columns: a (int32), b (varchar), c (struct{x: int32})
        let old_cols = vec![
            ColumnCatalog::visible(ColumnDesc::named("a", ColumnId::new(1), DataType::Int32)),
            ColumnCatalog::visible(ColumnDesc::named("b", ColumnId::new(2), DataType::Varchar)),
            ColumnCatalog::visible(ColumnDesc::named(
                "c",
                ColumnId::new(3),
                DataType::Struct(StructType::new([("x", DataType::Int32)])),
            )),
        ];

        // New columns: a (int32), b (int64), c (struct{x: int32, y: varchar}), d (int64)
        // - a: unchanged
        // - b: type changed (incompatible) - varchar to int64
        // - c: struct gained field (compatible)
        // - d: new column
        let new_cols = vec![
            ColumnCatalog::visible(ColumnDesc::named("a", ColumnId::new(1), DataType::Int32)),
            ColumnCatalog::visible(ColumnDesc::named("b", ColumnId::new(2), DataType::Int64)),
            ColumnCatalog::visible(ColumnDesc::named(
                "c",
                ColumnId::new(3),
                DataType::Struct(StructType::new([
                    ("x", DataType::Int32),
                    ("y", DataType::Varchar),
                ])),
            )),
            ColumnCatalog::visible(ColumnDesc::named("d", ColumnId::new(4), DataType::Int64)),
        ];

        let changes = categorize_column_changes(&old_cols, &new_cols);

        // Should have 1 added column (d)
        assert_eq!(changes.added.len(), 1);
        assert_eq!(changes.added[0].name(), "d");

        // Should have 0 dropped columns
        assert_eq!(changes.dropped.len(), 0);

        // Should have 1 modified column (b - type changed incompatibly)
        assert_eq!(changes.modified.len(), 1);
        assert_eq!(changes.modified[0].0.name(), "b");

        // Column c should not be in modified (struct field addition is compatible)
        assert!(!changes.modified.iter().any(|(old, _)| old.name() == "c"));
    }

    #[test]
    fn test_categorize_column_changes_with_dropped() {
        use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
        use risingwave_common::types::DataType;

        use super::categorize_column_changes;

        // Old columns: a, b, c
        let old_cols = vec![
            ColumnCatalog::visible(ColumnDesc::named("a", ColumnId::new(1), DataType::Int32)),
            ColumnCatalog::visible(ColumnDesc::named("b", ColumnId::new(2), DataType::Varchar)),
            ColumnCatalog::visible(ColumnDesc::named("c", ColumnId::new(3), DataType::Int64)),
        ];

        // New columns: a, c - dropped b
        let new_cols = vec![
            ColumnCatalog::visible(ColumnDesc::named("a", ColumnId::new(1), DataType::Int32)),
            ColumnCatalog::visible(ColumnDesc::named("c", ColumnId::new(3), DataType::Int64)),
        ];

        let changes = categorize_column_changes(&old_cols, &new_cols);

        assert_eq!(changes.added.len(), 0);
        assert_eq!(changes.dropped.len(), 1);
        assert_eq!(changes.dropped[0].name(), "b");
        assert_eq!(changes.modified.len(), 0);
    }
}
