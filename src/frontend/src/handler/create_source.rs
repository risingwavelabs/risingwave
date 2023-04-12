// Copyright 2023 RisingWave Labs
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
use std::sync::LazyLock;

use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{
    columns_extend, is_column_ids_dedup, ColumnCatalog, ColumnDesc, TableId, ROW_ID_COLUMN_ID,
};
use risingwave_common::error::ErrorCode::{self, InvalidInputSyntax, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::parser::{
    AvroParserConfig, DebeziumAvroParserConfig, ProtobufParserConfig,
};
use risingwave_connector::source::cdc::{MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR};
use risingwave_connector::source::datagen::DATAGEN_CONNECTOR;
use risingwave_connector::source::filesystem::S3_CONNECTOR;
use risingwave_connector::source::nexmark::source::{get_event_data_types_with_names, EventType};
use risingwave_connector::source::{
    GOOGLE_PUBSUB_CONNECTOR, KAFKA_CONNECTOR, KINESIS_CONNECTOR, NEXMARK_CONNECTOR,
    PULSAR_CONNECTOR,
};
use risingwave_pb::catalog::{PbSource, StreamSourceInfo, WatermarkDesc};
use risingwave_pb::plan_common::RowFormatType;
use risingwave_sqlparser::ast::{
    AvroSchema, CreateSourceStatement, DebeziumAvroSchema, ProtobufSchema, SourceSchema,
    SourceWatermark,
};

use super::create_table::bind_sql_table_column_constraints;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::ColumnId;
use crate::expr::Expr;
use crate::handler::create_table::{
    bind_sql_column_constraints, bind_sql_columns, ColumnIdGenerator,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::KAFKA_TIMESTAMP_COLUMN_NAME;
use crate::session::SessionImpl;

pub(crate) const UPSTREAM_SOURCE_KEY: &str = "connector";

/// Map an Avro schema to a relational schema.
async fn extract_avro_table_schema(
    schema: &AvroSchema,
    with_properties: &HashMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let conf = AvroParserConfig::new(
        with_properties,
        schema.row_schema_location.0.as_str(),
        schema.use_schema_registry,
        false,
        None,
    )
    .await?;
    let vec_column_desc = conf.map_to_columns()?;
    Ok(vec_column_desc
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

/// Map an Avro schema to a relational schema. And extract primary key columns.
async fn extract_upsert_avro_table_schema(
    schema: &AvroSchema,
    with_properties: &HashMap<String, String>,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>)> {
    let conf = AvroParserConfig::new(
        with_properties,
        schema.row_schema_location.0.as_str(),
        schema.use_schema_registry,
        true,
        None,
    )
    .await?;
    let vec_column_desc = conf.map_to_columns()?;
    let vec_pk_desc = conf.extract_pks()?;
    let pks = vec_pk_desc
        .into_iter()
        .map(|desc| {
            vec_column_desc
                .iter()
                .find(|x| x.name == desc.name)
                .ok_or_else(|| {
                    RwError::from(ErrorCode::InternalError(format!(
                        "Can not found primary key column {} in value schema",
                        desc.name
                    )))
                })
        })
        .map_ok(|desc| ColumnId::new(desc.column_id))
        .collect::<Result<Vec<_>>>()?;
    Ok((
        vec_column_desc
            .into_iter()
            .map(|col| ColumnCatalog {
                column_desc: col.into(),
                is_hidden: false,
            })
            .collect_vec(),
        pks,
    ))
}
async fn extract_debezium_avro_table_pk_columns(
    schema: &DebeziumAvroSchema,
    with_properties: &HashMap<String, String>,
) -> Result<Vec<String>> {
    let conf =
        DebeziumAvroParserConfig::new(with_properties, schema.row_schema_location.0.as_str())
            .await?;
    conf.get_pk_names()
}

// Map an Avro schema to a relational schema and return the pk_column_ids.
async fn extract_debezium_avro_table_schema(
    schema: &DebeziumAvroSchema,
    with_properties: &HashMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let conf =
        DebeziumAvroParserConfig::new(with_properties, schema.row_schema_location.0.as_str())
            .await?;
    let vec_column_desc = conf.map_to_columns()?;
    let column_catalog = vec_column_desc
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec();
    Ok(column_catalog)
}

/// Map a protobuf schema to a relational schema.
async fn extract_protobuf_table_schema(
    schema: &ProtobufSchema,
    with_properties: HashMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let parser = ProtobufParserConfig::new(
        &with_properties,
        &schema.row_schema_location.0,
        &schema.message_name.0,
        schema.use_schema_registry,
    )
    .await?;
    let column_descs = parser.map_to_columns()?;

    Ok(column_descs
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

#[inline(always)]
fn get_connector(with_properties: &HashMap<String, String>) -> Option<String> {
    with_properties
        .get(UPSTREAM_SOURCE_KEY)
        .map(|s| s.to_lowercase())
}

#[inline(always)]
pub(crate) fn is_kafka_source(with_properties: &HashMap<String, String>) -> bool {
    let Some(connector) = get_connector(with_properties) else {
        return false;
    };

    connector == KAFKA_CONNECTOR
}

pub(crate) async fn resolve_source_schema(
    source_schema: SourceSchema,
    columns: &mut Vec<ColumnCatalog>,
    with_properties: &mut HashMap<String, String>,
    row_id_index: &mut Option<usize>,
    pk_column_ids: &mut Vec<ColumnId>,
    is_materialized: bool,
) -> Result<StreamSourceInfo> {
    validate_compatibility(&source_schema, with_properties)?;
    check_nexmark_schema(with_properties, *row_id_index, columns)?;

    let is_kafka = is_kafka_source(with_properties);

    let source_info = match &source_schema {
        SourceSchema::Protobuf(protobuf_schema) => {
            let (expected_column_len, expected_row_id_index) = if is_kafka && !is_materialized {
                // The first column is `_rw_kafka_timestamp`.
                (2, 1)
            } else {
                (1, 0)
            };
            if columns.len() != expected_column_len
                || *pk_column_ids != vec![ROW_ID_COLUMN_ID]
                || *row_id_index != Some(expected_row_id_index)
            {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format protobuf. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#protobuf for more information.".to_string(),
                )));
            }

            columns_extend(
                columns,
                extract_protobuf_table_schema(protobuf_schema, with_properties.clone()).await?,
            );

            StreamSourceInfo {
                row_format: RowFormatType::Protobuf as i32,
                row_schema_location: protobuf_schema.row_schema_location.0.clone(),
                use_schema_registry: protobuf_schema.use_schema_registry,
                proto_message_name: protobuf_schema.message_name.0.clone(),
                ..Default::default()
            }
        }

        SourceSchema::Avro(avro_schema) => {
            let (expected_column_len, expected_row_id_index) = if is_kafka && !is_materialized {
                // The first column is `_rw_kafka_timestamp`.
                (2, 1)
            } else {
                (1, 0)
            };
            if columns.len() != expected_column_len
                || *pk_column_ids != vec![ROW_ID_COLUMN_ID]
                || *row_id_index != Some(expected_row_id_index)
            {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format avro. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#avro for more information.".to_string(),
                )));
            }

            columns_extend(
                columns,
                extract_avro_table_schema(avro_schema, with_properties).await?,
            );
            StreamSourceInfo {
                row_format: RowFormatType::Avro as i32,

                row_schema_location: avro_schema.row_schema_location.0.clone(),
                use_schema_registry: avro_schema.use_schema_registry,
                proto_message_name: "".to_owned(),
                ..Default::default()
            }
        }

        SourceSchema::UpsertAvro(avro_schema) => {
            let mut upsert_avro_primary_key = Default::default();
            if row_id_index.is_none() {
                // user specify pk(s)
                if columns.len() != pk_column_ids.len() || pk_column_ids.len() != 1 {
                    return Err(RwError::from(ProtocolError(
                        "You can specify single primary key column or leave the columns and primary key fields empty.".to_string(),
                    )));
                }
                let pk_name = columns[0].column_desc.name.clone();

                *columns = extract_avro_table_schema(avro_schema, with_properties).await?;
                let pk_col = columns
                    .iter()
                    .find(|col| col.column_desc.name == pk_name)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(format!(
                            "Primary key {pk_name} is not found."
                        )))
                    })?;

                *pk_column_ids = vec![pk_col.column_id()];
                upsert_avro_primary_key = pk_name;
            } else {
                // contains row_id, user specify some columns without pk
                if !(*pk_column_ids == vec![ROW_ID_COLUMN_ID] && columns.len() == 1) {
                    return Err(RwError::from(ProtocolError(
                        "UPSERT AVRO will automatically extract the schema from the Avro schema. You can specify single primary key column or leave the columns and primary key fields empty.".to_string(),
                )));
                }
                let (columns_extracted, pks_extracted) =
                    extract_upsert_avro_table_schema(avro_schema, with_properties).await?;

                *columns = columns_extracted;
                *pk_column_ids = pks_extracted;
            }

            StreamSourceInfo {
                row_format: RowFormatType::UpsertAvro as i32,
                row_schema_location: avro_schema.row_schema_location.0.clone(),
                use_schema_registry: avro_schema.use_schema_registry,
                upsert_avro_primary_key,
                ..Default::default()
            }
        }

        SourceSchema::Json => StreamSourceInfo {
            row_format: RowFormatType::Json as i32,
            ..Default::default()
        },
        SourceSchema::UpsertJson => StreamSourceInfo {
            row_format: RowFormatType::UpsertJson as i32,
            ..Default::default()
        },

        SourceSchema::Maxwell => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format maxwell."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::Maxwell as i32,
                ..Default::default()
            }
        }

        SourceSchema::DebeziumJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::DebeziumJson as i32,
                ..Default::default()
            }
        }

        SourceSchema::CanalJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format cannal_json."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::CanalJson as i32,
                ..Default::default()
            }
        }

        SourceSchema::Csv(csv_info) => StreamSourceInfo {
            row_format: RowFormatType::Csv as i32,
            csv_delimiter: csv_info.delimiter as i32,
            csv_has_header: csv_info.has_header,
            ..Default::default()
        },

        SourceSchema::Native => StreamSourceInfo {
            row_format: RowFormatType::Native as i32,
            ..Default::default()
        },

        SourceSchema::DebeziumAvro(avro_schema) => {
            // no row_id, so user specify pk(s)
            if row_id_index.is_none() {
                // user specify no only pk columns
                if columns.len() != pk_column_ids.len() {
                    return Err(RwError::from(ProtocolError(
                        "User can only specify primary key columns when creating table with row
                        format debezium_avro."
                            .to_owned(),
                    )));
                }

                let full_columns =
                    extract_debezium_avro_table_schema(avro_schema, with_properties).await?;
                pk_column_ids.clear();
                for pk_column in columns.as_slice() {
                    let real_pk_column = full_columns
                        .iter()
                        .find(|c| c.name().eq(pk_column.name()))
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(format!(
                                "pk column {} not exists in avro schema",
                                pk_column.name()
                            )))
                        })?;
                    pk_column_ids.push(real_pk_column.column_id());
                }
                columns.clear();
                columns.extend(full_columns);
            } else {
                // user specify some columns without pk
                if columns.len() != 1 || *pk_column_ids != vec![ROW_ID_COLUMN_ID] {
                    return Err(RwError::from(ProtocolError(
                        "User can only specify primary key columns when creating table with row
                        format debezium_avro."
                            .to_owned(),
                    )));
                }

                *row_id_index = None;
                columns.clear();
                pk_column_ids.clear();

                let full_columns =
                    extract_debezium_avro_table_schema(avro_schema, with_properties).await?;
                let pk_names =
                    extract_debezium_avro_table_pk_columns(avro_schema, with_properties).await?;
                // extract pk(s) from schema registry
                for pk_name in &pk_names {
                    let pk_column_id = full_columns
                        .iter()
                        .find(|c| c.name().eq(pk_name))
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(format!(
                                "pk column {} not exists in avro schema",
                                pk_name
                            )))
                        })?
                        .column_desc
                        .column_id;
                    pk_column_ids.push(pk_column_id);
                }
                columns.extend(full_columns);
            }

            StreamSourceInfo {
                row_format: RowFormatType::DebeziumAvro as i32,
                row_schema_location: avro_schema.row_schema_location.0.clone(),
                ..Default::default()
            }
        }
    };

    Ok(source_info)
}

// Add a hidden column `_rw_kafka_timestamp` to each message from Kafka source.
fn check_and_add_timestamp_column(
    with_properties: &HashMap<String, String>,
    column_descs: &mut Vec<ColumnDesc>,
    col_id_gen: &mut ColumnIdGenerator,
) {
    if is_kafka_source(with_properties) {
        let kafka_timestamp_column = ColumnDesc {
            data_type: DataType::Timestamptz,
            column_id: col_id_gen.generate(KAFKA_TIMESTAMP_COLUMN_NAME),
            name: KAFKA_TIMESTAMP_COLUMN_NAME.to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
            generated_column: None,
        };
        column_descs.push(kafka_timestamp_column);
    }
}

pub(super) fn bind_source_watermark(
    session: &SessionImpl,
    name: String,
    source_watermarks: Vec<SourceWatermark>,
    column_catalogs: &[ColumnCatalog],
) -> Result<Vec<WatermarkDesc>> {
    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(name.clone(), column_catalogs.to_vec())?;

    let watermark_descs = source_watermarks
        .into_iter()
        .map(|source_watermark| {
            let col_name = source_watermark.column.real_value();
            let watermark_idx = binder.get_column_binding_index(name.clone(), &col_name)?;

            let expr = binder.bind_expr(source_watermark.expr)?.to_expr_proto();

            Ok::<_, RwError>(WatermarkDesc {
                watermark_idx: watermark_idx as u32,
                expr: Some(expr),
            })
        })
        .try_collect()?;
    Ok(watermark_descs)
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, Vec<RowFormatType>>> = LazyLock::new(
    || {
        convert_args!(hashmap!(
                KAFKA_CONNECTOR => vec![RowFormatType::Json, RowFormatType::Protobuf, RowFormatType::DebeziumJson, RowFormatType::Avro, RowFormatType::Maxwell, RowFormatType::CanalJson, RowFormatType::DebeziumAvro, RowFormatType::UpsertJson, RowFormatType::UpsertAvro],
                PULSAR_CONNECTOR => vec![RowFormatType::Json, RowFormatType::Protobuf, RowFormatType::DebeziumJson, RowFormatType::Avro, RowFormatType::Maxwell, RowFormatType::CanalJson],
                KINESIS_CONNECTOR => vec![RowFormatType::Json, RowFormatType::Protobuf, RowFormatType::DebeziumJson, RowFormatType::Avro, RowFormatType::Maxwell, RowFormatType::CanalJson],
                GOOGLE_PUBSUB_CONNECTOR => vec![RowFormatType::Json, RowFormatType::Protobuf, RowFormatType::DebeziumJson, RowFormatType::Avro, RowFormatType::Maxwell, RowFormatType::CanalJson],
                NEXMARK_CONNECTOR => vec![RowFormatType::Native],
                DATAGEN_CONNECTOR => vec![RowFormatType::Native, RowFormatType::Json],
                S3_CONNECTOR => vec![RowFormatType::Csv, RowFormatType::Json],
                MYSQL_CDC_CONNECTOR => vec![RowFormatType::DebeziumJson],
                POSTGRES_CDC_CONNECTOR => vec![RowFormatType::DebeziumJson],
        ))
    },
);

fn source_shema_to_row_format(source_schema: &SourceSchema) -> RowFormatType {
    match source_schema {
        SourceSchema::Avro(_) => RowFormatType::Avro,
        SourceSchema::Protobuf(_) => RowFormatType::Protobuf,
        SourceSchema::Json => RowFormatType::Json,
        SourceSchema::DebeziumJson => RowFormatType::DebeziumJson,
        SourceSchema::DebeziumAvro(_) => RowFormatType::DebeziumAvro,
        SourceSchema::UpsertJson => RowFormatType::UpsertJson,
        SourceSchema::UpsertAvro(_) => RowFormatType::UpsertAvro,
        SourceSchema::Maxwell => RowFormatType::Maxwell,
        SourceSchema::CanalJson => RowFormatType::CanalJson,
        SourceSchema::Csv(_) => RowFormatType::Csv,
        SourceSchema::Native => RowFormatType::Native,
    }
}

fn validate_compatibility(
    source_schema: &SourceSchema,
    props: &mut HashMap<String, String>,
) -> Result<()> {
    let connector = get_connector(props)
        .ok_or_else(|| RwError::from(ProtocolError("missing field 'connector'".to_string())))?;
    let row_format = source_shema_to_row_format(source_schema);

    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(&connector)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {} is not supported",
                connector
            )))
        })?;
    if connector != KAFKA_CONNECTOR
        && matches!(
            &source_schema,
            SourceSchema::Protobuf(ProtobufSchema {
                use_schema_registry: true,
                ..
            }) | SourceSchema::Avro(AvroSchema {
                use_schema_registry: true,
                ..
            }) | SourceSchema::DebeziumAvro(_)
        )
    {
        return Err(RwError::from(ProtocolError(format!(
            "The {} must be kafka when schema registry is used",
            UPSTREAM_SOURCE_KEY
        ))));
    }
    if !compatible_formats.contains(&row_format) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} does not support row format {:?}",
            connector, row_format
        ))));
    }

    if connector == POSTGRES_CDC_CONNECTOR {
        if !props.contains_key("slot.name") {
            // Build a random slot name with UUID
            // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
            props.insert("slot.name".into(), format!("rw_cdc_{}", uuid));
        }
        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
    }
    Ok(())
}

fn check_nexmark_schema(
    props: &HashMap<String, String>,
    row_id_index: Option<usize>,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let Some(connector) = get_connector(props) else {
        return Ok(());
    };

    if connector != NEXMARK_CONNECTOR {
        return Ok(());
    }

    let table_type = props
        .get("nexmark.table.type")
        .map(|t| t.to_ascii_lowercase());

    let event_type = match table_type.as_deref() {
        None => None,
        Some("bid") => Some(EventType::Bid),
        Some("auction") => Some(EventType::Auction),
        Some("person") => Some(EventType::Person),
        Some(t) => {
            return Err(RwError::from(ProtocolError(format!(
                "unsupported table type for nexmark source: {}",
                t
            ))))
        }
    };

    let expected = get_event_data_types_with_names(event_type, row_id_index);
    let user_defined = columns
        .iter()
        .map(|c| {
            (
                c.column_desc.name.to_ascii_lowercase(),
                c.column_desc.data_type.to_owned(),
            )
        })
        .collect_vec();

    if expected != user_defined {
        return Err(RwError::from(ProtocolError(format!(
            "The shema of the nexmark source must specify all columns in order, expected {:?}, but get {:?}",
            expected, user_defined
        ))));
    }
    Ok(())
}

pub async fn handle_create_source(
    handler_args: HandlerArgs,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    session.check_relation_name_duplicated(stmt.source_name.clone())?;

    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, stmt.source_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    if handler_args.with_options.is_empty() {
        return Err(RwError::from(InvalidInputSyntax(
            "missing WITH clause".to_string(),
        )));
    }

    let mut with_properties = handler_args.with_options.into_inner().into_iter().collect();

    let mut col_id_gen = ColumnIdGenerator::new_initial();

    let mut column_descs = bind_sql_columns(stmt.columns.clone(), &mut col_id_gen)?;

    check_and_add_timestamp_column(&with_properties, &mut column_descs, &mut col_id_gen);

    let (mut columns, mut pk_column_ids, mut row_id_index) =
        bind_sql_table_column_constraints(column_descs, stmt.columns.clone(), stmt.constraints)?;

    if row_id_index.is_none() {
        return Err(ErrorCode::InvalidInputSyntax(
            "Source does not support PRIMARY KEY constraint, please use \"CREATE TABLE\" instead"
                .to_owned(),
        )
        .into());
    }

    let source_info = resolve_source_schema(
        stmt.source_schema,
        &mut columns,
        &mut with_properties,
        &mut row_id_index,
        &mut pk_column_ids,
        false,
    )
    .await?;

    debug_assert!(is_column_ids_dedup(&columns));

    let watermark_descs =
        bind_source_watermark(&session, name.clone(), stmt.source_watermarks, &columns)?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);

    bind_sql_column_constraints(&session, name.clone(), &mut columns, stmt.columns)?;

    if row_id_index.is_none() && columns.iter().any(|c| c.is_generated()) {
        // TODO(yuhao): allow delete from a non append only source
        return Err(RwError::from(ErrorCode::BindError(
            "Generated columns are only allowed in an append only source.".to_string(),
        )));
    }

    let row_id_index = row_id_index.map(|index| index as _);
    let pk_column_ids = pk_column_ids.into_iter().map(Into::into).collect();

    let columns = columns.into_iter().map(|c| c.to_protobuf()).collect_vec();

    let connection_name = with_properties.get("connection.name").map(|s| s.as_str());
    let connection_id = match connection_name {
        Some(connection_name) => {
            let connection_id = session
                .get_connection_id_for_create(schema_name, connection_name)
                .map_err(|_| ErrorCode::ItemNotFound(connection_name.to_string()))?;
            if !is_kafka_source(&with_properties) {
                return Err(RwError::from(ErrorCode::ProtocolError(
                    "Create source with connection is only supported for kafka connectors."
                        .to_string(),
                )));
            }
            Some(connection_id)
        }
        None => None,
    };
    let definition = handler_args.normalized_sql;

    let source = PbSource {
        id: TableId::placeholder().table_id,
        schema_id,
        database_id,
        name,
        row_id_index,
        columns,
        pk_column_ids,
        properties: with_properties.into_iter().collect(),
        info: Some(source_info),
        owner: session.user_id(),
        watermark_descs,
        definition,
        connection_id,
        optional_associated_table_id: None,
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_source(source).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{
        row_id_column_name, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kinesis')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = DataType::new_struct(
            vec![DataType::Varchar, DataType::Varchar],
            vec!["address".to_string(), "zipcode".to_string()],
        );
        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Serial,
            "id" => DataType::Int32,
            "zipcode" => DataType::Int64,
            "rate" => DataType::Float32,
            "country" => DataType::new_struct(
                vec![DataType::Varchar,city_type,DataType::Varchar],
                vec!["address".to_string(), "city".to_string(), "zipcode".to_string()],
            ),
        };
        assert_eq!(columns, expected_columns);
    }
}
