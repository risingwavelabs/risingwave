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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId, max_column_id};
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::{
    AdditionalCollectionName, AdditionalColumn, AdditionalColumnFilename, AdditionalColumnHeader,
    AdditionalColumnHeaders, AdditionalColumnKey, AdditionalColumnOffset,
    AdditionalColumnPartition, AdditionalColumnPayload, AdditionalColumnPulsarMessageIdData,
    AdditionalColumnRabbitmqAckData, AdditionalColumnTimestamp, AdditionalDatabaseName,
    AdditionalSchemaName, AdditionalSubject, AdditionalTableName,
};

use crate::error::ConnectorResult;
use crate::source::cdc::MONGODB_CDC_CONNECTOR;
use crate::source::{
    AZBLOB_CONNECTOR, GCS_CONNECTOR, KAFKA_CONNECTOR, KINESIS_CONNECTOR, MQTT_CONNECTOR,
    NATS_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR, PULSAR_CONNECTOR, RABBITMQ_CONNECTOR,
};

// Hidden source-state columns used by connectors that do not support `INCLUDE`.
pub static COMMON_SOURCE_STATE_ADDITIONAL_COLUMNS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["partition", "offset"]));

// Public columns supported by the SQL `INCLUDE` syntax. Do not add a column here
// only because the source executor needs it internally: user-facing metadata must
// have source-level semantics, not expose RisingWave's checkpoint plumbing.
pub static COMPATIBLE_ADDITIONAL_COLUMNS: LazyLock<HashMap<&'static str, HashSet<&'static str>>> =
    LazyLock::new(|| {
        HashMap::from([
            (
                KAFKA_CONNECTOR,
                HashSet::from([
                    "key",
                    "timestamp",
                    "partition",
                    "offset",
                    "header",
                    "payload",
                ]),
            ),
            (
                PULSAR_CONNECTOR,
                HashSet::from([
                    "key",
                    "partition",
                    "offset",
                    "payload",
                    "message_id_data",
                    "header",
                ]),
            ),
            (
                KINESIS_CONNECTOR,
                HashSet::from(["key", "partition", "offset", "timestamp", "payload"]),
            ),
            (
                NATS_CONNECTOR,
                HashSet::from(["partition", "offset", "payload", "subject"]),
            ),
            (
                OPENDAL_S3_CONNECTOR,
                HashSet::from(["file", "offset", "payload"]),
            ),
            (GCS_CONNECTOR, HashSet::from(["file", "offset", "payload"])),
            (
                AZBLOB_CONNECTOR,
                HashSet::from(["file", "offset", "payload"]),
            ),
            (
                POSIX_FS_CONNECTOR,
                HashSet::from(["file", "offset", "payload"]),
            ),
            // mongodb-cdc doesn't support cdc backfill table
            (
                MONGODB_CDC_CONNECTOR,
                HashSet::from([
                    "timestamp",
                    "partition",
                    "offset",
                    "database_name",
                    "collection_name",
                ]),
            ),
            (MQTT_CONNECTOR, HashSet::from(["offset", "partition"])),
            (RABBITMQ_CONNECTOR, HashSet::from(["payload"])),
        ])
    });

// For CDC backfill table, the additional columns are added to the schema of `StreamCdcScan`
pub static CDC_BACKFILL_TABLE_ADDITIONAL_COLUMNS: LazyLock<Option<HashSet<&'static str>>> =
    LazyLock::new(|| {
        Some(HashSet::from([
            "timestamp",
            "database_name",
            "schema_name",
            "table_name",
        ]))
    });

pub fn get_supported_additional_columns(
    connector_name: &str,
    is_cdc_backfill: bool,
) -> Option<&HashSet<&'static str>> {
    if is_cdc_backfill {
        CDC_BACKFILL_TABLE_ADDITIONAL_COLUMNS.as_ref()
    } else {
        COMPATIBLE_ADDITIONAL_COLUMNS.get(connector_name)
    }
}

fn get_source_state_additional_columns(connector_name: &str) -> &HashSet<&'static str> {
    match connector_name {
        OPENDAL_S3_CONNECTOR | GCS_CONNECTOR | AZBLOB_CONNECTOR | POSIX_FS_CONNECTOR => {
            COMPATIBLE_ADDITIONAL_COLUMNS
                .get(connector_name)
                .expect("file source connectors must define file/offset additional columns")
        }
        _ => &COMMON_SOURCE_STATE_ADDITIONAL_COLUMNS,
    }
}

pub fn gen_default_addition_col_name(
    connector_name: &str,
    additional_col_type: &str,
    inner_field_name: Option<&str>,
    data_type: Option<&DataType>,
) -> String {
    let legacy_dt_name = data_type.map(|dt| format!("{:?}", dt).to_lowercase());
    let col_name = [
        Some(connector_name),
        Some(additional_col_type),
        inner_field_name,
        legacy_dt_name.as_deref(),
    ];
    col_name.iter().fold("_rw".to_owned(), |name, ele| {
        if let Some(ele) = ele {
            format!("{}_{}", name, ele)
        } else {
            name
        }
    })
}

pub fn build_additional_column_desc(
    column_id: ColumnId,
    connector_name: &str,
    additional_col_type: &str,
    column_alias: Option<String>,
    inner_field_name: Option<&str>,
    data_type: Option<&DataType>,
    reject_unknown_connector: bool,
    is_cdc_backfill_table: bool,
) -> ConnectorResult<ColumnDesc> {
    let compatible_columns = match (
        get_supported_additional_columns(connector_name, is_cdc_backfill_table),
        reject_unknown_connector,
    ) {
        (Some(compat_cols), _) => compat_cols,
        (None, false) => &COMMON_SOURCE_STATE_ADDITIONAL_COLUMNS,
        (None, true) => {
            bail!(
                "additional column is not supported for connector {}, acceptable connectors: {:?}",
                connector_name,
                COMPATIBLE_ADDITIONAL_COLUMNS.keys(),
            );
        }
    };
    build_additional_column_desc_with_compat(
        column_id,
        connector_name,
        additional_col_type,
        column_alias,
        inner_field_name,
        data_type,
        compatible_columns,
    )
}

fn build_additional_column_desc_with_compat(
    column_id: ColumnId,
    connector_name: &str,
    additional_col_type: &str,
    column_alias: Option<String>,
    inner_field_name: Option<&str>,
    data_type: Option<&DataType>,
    compatible_columns: &HashSet<&'static str>,
) -> ConnectorResult<ColumnDesc> {
    if !compatible_columns.contains(additional_col_type) {
        bail!(
            "additional column type {} is not supported for connector {}, acceptable column types: {:?}",
            additional_col_type,
            connector_name,
            compatible_columns
        );
    }

    let column_name = column_alias.unwrap_or_else(|| {
        gen_default_addition_col_name(
            connector_name,
            additional_col_type,
            inner_field_name,
            data_type,
        )
    });

    let col_desc = match additional_col_type {
        "key" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Bytea,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Key(AdditionalColumnKey {})),
            },
        ),

        "timestamp" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Timestamptz,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Timestamp(
                    AdditionalColumnTimestamp {},
                )),
            },
        ),
        "partition" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Partition(
                    AdditionalColumnPartition {},
                )),
            },
        ),
        "payload" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Jsonb,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Payload(AdditionalColumnPayload {})),
            },
        ),
        "offset" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Offset(AdditionalColumnOffset {})),
            },
        ),

        "file" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Filename(AdditionalColumnFilename {})),
            },
        ),
        "header" => build_header_catalog(column_id, &column_name, inner_field_name, data_type),
        "database_name" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::DatabaseName(
                    AdditionalDatabaseName {},
                )),
            },
        ),
        "schema_name" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::SchemaName(AdditionalSchemaName {})),
            },
        ),
        "table_name" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::TableName(AdditionalTableName {})),
            },
        ),
        "collection_name" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::CollectionName(
                    AdditionalCollectionName {},
                )),
            },
        ),
        "subject" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Varchar, // Assuming subject is a string
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Subject(AdditionalSubject {})),
            },
        ),
        "message_id_data" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Bytea,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::PulsarMessageIdData(
                    AdditionalColumnPulsarMessageIdData {},
                )),
            },
        ),
        "rabbitmq_ack_data" => ColumnDesc::named_with_additional_column(
            column_name,
            column_id,
            DataType::Bytea,
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::RabbitmqAckData(
                    AdditionalColumnRabbitmqAckData {},
                )),
            },
        ),
        _ => unreachable!(),
    };

    Ok(col_desc)
}

pub fn derive_pulsar_message_id_data_column(
    connector_name: &str,
    column_exist: &mut Vec<bool>,
    additional_columns: &mut Vec<ColumnDesc>,
) {
    // additional columns already check the max_column_id
    // so we can take the max column id of additional columns as the max column id of all columns
    let max_column_id = additional_columns
        .iter()
        .fold(ColumnId::first_user_column(), |a, b| a.max(b.column_id));

    // assume user does not include `message_id_data` column
    column_exist.push(false);
    additional_columns.push(
        build_additional_column_desc(
            max_column_id.next(),
            connector_name,
            "message_id_data",
            None,
            None,
            None,
            false,
            false,
        )
        .unwrap(),
    );
}

pub fn derive_rabbitmq_ack_data_column(
    connector_name: &str,
    column_exist: &mut Vec<bool>,
    additional_columns: &mut Vec<ColumnDesc>,
    skip_col_id: bool,
) {
    // `rabbitmq_ack_data` is an internal checkpoint column. Keep it out of
    // `COMMON_SOURCE_STATE_ADDITIONAL_COLUMNS`, which intentionally models only
    // partition/file plus offset state for existing callers.
    let max_column_id = additional_columns
        .iter()
        .fold(ColumnId::first_user_column(), |a, b| a.max(b.column_id));

    column_exist.push(false);
    additional_columns.push(ColumnDesc::named_with_additional_column(
        format!("_rw_{connector_name}_ack_data"),
        if skip_col_id {
            ColumnId::placeholder()
        } else {
            max_column_id.next()
        },
        DataType::Bytea,
        AdditionalColumn {
            column_type: Some(AdditionalColumnType::RabbitmqAckData(
                AdditionalColumnRabbitmqAckData {},
            )),
        },
    ));
}

/// Utility function for adding partition and offset columns to the columns, if not specified by the user.
///
/// ## Returns
/// - `columns_exist`: whether 1. `partition`/`file` and 2. `offset` columns are included in `columns`.
/// - `additional_columns`: The `ColumnCatalog` for `partition`/`file` and `offset` columns.
pub fn source_add_partition_offset_cols(
    columns: &[ColumnCatalog],
    connector_name: &str,
    skip_col_id: bool,
) -> (Vec<bool>, Vec<ColumnDesc>) {
    let mut columns_exist = vec![false; 2];

    let mut last_column_id = max_column_id(columns);
    let mut assign_col_id = || {
        if skip_col_id {
            // col id will be filled outside later. Here just use a placeholder.
            ColumnId::placeholder()
        } else {
            last_column_id = last_column_id.next();
            last_column_id
        }
    };

    let additional_columns: Vec<_> = {
        let compat_col_types = get_source_state_additional_columns(connector_name);
        ["partition", "file", "offset"]
            .iter()
            .filter_map(|col_type| {
                if compat_col_types.contains(col_type) {
                    Some(
                        build_additional_column_desc_with_compat(
                            assign_col_id(),
                            connector_name,
                            col_type,
                            None,
                            None,
                            None,
                            compat_col_types,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                }
            })
            .collect()
    };
    assert_eq!(additional_columns.len(), 2);
    use risingwave_pb::plan_common::additional_column::ColumnType;
    assert_matches::assert_matches!(
        additional_columns[0].additional_column,
        AdditionalColumn {
            column_type: Some(ColumnType::Partition(_) | ColumnType::Filename(_)),
        }
    );
    assert_matches::assert_matches!(
        additional_columns[1].additional_column,
        AdditionalColumn {
            column_type: Some(ColumnType::Offset(_)),
        }
    );

    // Check if partition/file/offset columns are included explicitly.
    for col in columns {
        match col.column_desc.additional_column {
            AdditionalColumn {
                column_type: Some(ColumnType::Partition(_) | ColumnType::Filename(_)),
            } => {
                columns_exist[0] = true;
            }
            AdditionalColumn {
                column_type: Some(ColumnType::Offset(_)),
            } => {
                columns_exist[1] = true;
            }
            _ => (),
        }
    }

    (columns_exist, additional_columns)
}

fn build_header_catalog(
    column_id: ColumnId,
    col_name: &str,
    inner_field_name: Option<&str>,
    data_type: Option<&DataType>,
) -> ColumnDesc {
    if let Some(inner) = inner_field_name {
        let data_type = data_type.unwrap_or(&DataType::Bytea);
        let pb_data_type = data_type.to_protobuf();
        ColumnDesc::named_with_additional_column(
            col_name,
            column_id,
            data_type.clone(),
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::HeaderInner(AdditionalColumnHeader {
                    inner_field: inner.to_owned(),
                    data_type: Some(pb_data_type),
                })),
            },
        )
    } else {
        ColumnDesc::named_with_additional_column(
            col_name,
            column_id,
            DataType::list(get_kafka_header_item_datatype()),
            AdditionalColumn {
                column_type: Some(AdditionalColumnType::Headers(AdditionalColumnHeaders {})),
            },
        )
    }
}

pub fn get_kafka_header_item_datatype() -> DataType {
    let struct_inner = vec![("key", DataType::Varchar), ("value", DataType::Bytea)];
    DataType::Struct(StructType::new(struct_inner))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_gen_default_addition_col_name() {
        assert_eq!(
            gen_default_addition_col_name("kafka", "key", None, None),
            "_rw_kafka_key"
        );
        assert_eq!(
            gen_default_addition_col_name("kafka", "header", Some("inner"), None),
            "_rw_kafka_header_inner"
        );
        assert_eq!(
            gen_default_addition_col_name(
                "kafka",
                "header",
                Some("inner"),
                Some(&DataType::Varchar)
            ),
            "_rw_kafka_header_inner_varchar"
        );
    }

    #[test]
    fn test_build_pulsar_header_additional_column() {
        let col = build_additional_column_desc(
            ColumnId::new(1),
            PULSAR_CONNECTOR,
            "header",
            Some("pulsar_header".to_owned()),
            Some("tenant"),
            Some(&DataType::Varchar),
            true,
            false,
        )
        .unwrap();

        assert_eq!(col.name, "pulsar_header");
        assert_eq!(col.data_type, DataType::Varchar);
        assert_matches::assert_matches!(
            col.additional_column.column_type,
            Some(AdditionalColumnType::HeaderInner(ref header))
                if header.inner_field == "tenant"
        );
    }

    #[test]
    fn test_rabbitmq_public_additional_columns_exclude_internal_state() {
        let supported = get_supported_additional_columns(RABBITMQ_CONNECTOR, false).unwrap();
        assert_eq!(supported, &HashSet::from(["payload"]));

        let payload_desc = build_additional_column_desc(
            ColumnId::placeholder(),
            RABBITMQ_CONNECTOR,
            "payload",
            None,
            None,
            None,
            true,
            false,
        )
        .unwrap();
        assert_eq!(payload_desc.data_type, DataType::Jsonb);

        for internal_col_type in ["offset", "partition"] {
            let err = build_additional_column_desc(
                ColumnId::placeholder(),
                RABBITMQ_CONNECTOR,
                internal_col_type,
                None,
                None,
                None,
                true,
                false,
            )
            .unwrap_err();
            assert!(
                err.to_string()
                    .contains("is not supported for connector rabbitmq")
            );
        }

        let (_, hidden_state_columns) =
            source_add_partition_offset_cols(&[], RABBITMQ_CONNECTOR, true);
        assert_eq!(hidden_state_columns.len(), 2);
        assert_eq!(hidden_state_columns[0].name, "_rw_rabbitmq_partition");
        assert_matches::assert_matches!(
            hidden_state_columns[0].additional_column.column_type,
            Some(AdditionalColumnType::Partition(_))
        );
        assert_eq!(hidden_state_columns[1].name, "_rw_rabbitmq_offset");
        assert_matches::assert_matches!(
            hidden_state_columns[1].additional_column.column_type,
            Some(AdditionalColumnType::Offset(_))
        );

        let mut column_exist = vec![true, true];
        let mut hidden_ack_columns = hidden_state_columns.clone();
        derive_rabbitmq_ack_data_column(
            RABBITMQ_CONNECTOR,
            &mut column_exist,
            &mut hidden_ack_columns,
            true,
        );
        assert_eq!(column_exist, vec![true, true, false]);
        let ack_column = hidden_ack_columns.last().unwrap();
        assert_eq!(ack_column.name, "_rw_rabbitmq_ack_data");
        assert_eq!(ack_column.data_type, DataType::Bytea);
        assert_eq!(ack_column.column_id, ColumnId::placeholder());
        assert_matches::assert_matches!(
            ack_column.additional_column.column_type,
            Some(AdditionalColumnType::RabbitmqAckData(_))
        );
    }

    #[test]
    fn test_file_connectors_keep_file_offset_state_columns() {
        for connector in [
            OPENDAL_S3_CONNECTOR,
            GCS_CONNECTOR,
            AZBLOB_CONNECTOR,
            POSIX_FS_CONNECTOR,
        ] {
            let supported = get_supported_additional_columns(connector, false).unwrap();
            assert!(supported.contains("file"));
            assert!(supported.contains("offset"));
            assert!(supported.contains("payload"));

            let (columns_exist, hidden_state_columns) =
                source_add_partition_offset_cols(&[], connector, true);
            assert_eq!(columns_exist, vec![false, false]);
            assert_eq!(
                hidden_state_columns[0].name,
                format!("_rw_{connector}_file")
            );
            assert_matches::assert_matches!(
                hidden_state_columns[0].additional_column.column_type,
                Some(AdditionalColumnType::Filename(_))
            );
            assert_eq!(
                hidden_state_columns[1].name,
                format!("_rw_{connector}_offset")
            );
            assert_matches::assert_matches!(
                hidden_state_columns[1].additional_column.column_type,
                Some(AdditionalColumnType::Offset(_))
            );

            let user_file = ColumnCatalog::visible(
                build_additional_column_desc(
                    ColumnId::placeholder(),
                    connector,
                    "file",
                    None,
                    None,
                    None,
                    true,
                    false,
                )
                .unwrap(),
            );
            let user_offset = ColumnCatalog::visible(
                build_additional_column_desc(
                    ColumnId::placeholder(),
                    connector,
                    "offset",
                    None,
                    None,
                    None,
                    true,
                    false,
                )
                .unwrap(),
            );
            let (columns_exist, _) =
                source_add_partition_offset_cols(&[user_file, user_offset], connector, true);
            assert_eq!(columns_exist, vec![true, true]);
        }
    }
}
