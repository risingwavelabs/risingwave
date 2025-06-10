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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use anyhow::anyhow;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId, max_column_id};
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::{
    AdditionalCollectionName, AdditionalColumn, AdditionalColumnFilename, AdditionalColumnHeader,
    AdditionalColumnHeaders, AdditionalColumnKey, AdditionalColumnOffset,
    AdditionalColumnPartition, AdditionalColumnPayload, AdditionalColumnTimestamp,
    AdditionalDatabaseName, AdditionalSchemaName, AdditionalSubject, AdditionalTableName,
};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::cdc::MONGODB_CDC_CONNECTOR;
use crate::source::{
    AZBLOB_CONNECTOR, GCS_CONNECTOR, KAFKA_CONNECTOR, KINESIS_CONNECTOR, MQTT_CONNECTOR,
    NATS_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR, PULSAR_CONNECTOR,
};

// Hidden additional columns connectors which do not support `include` syntax.
pub static COMMON_COMPATIBLE_ADDITIONAL_COLUMNS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["partition", "offset"]));

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
                HashSet::from(["key", "partition", "offset", "payload"]),
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
        (None, false) => &COMMON_COMPATIBLE_ADDITIONAL_COLUMNS,
        (None, true) => {
            bail!(
                "additional column is not supported for connector {}, acceptable connectors: {:?}",
                connector_name,
                COMPATIBLE_ADDITIONAL_COLUMNS.keys(),
            );
        }
    };
    if !compatible_columns.contains(additional_col_type) {
        bail!(
            "additional column type {} is not supported for connector {}, acceptable column types: {:?}",
            additional_col_type,
            connector_name,
            compatible_columns
        );
    }

    let Some(column_name) = column_alias else {
        return Err(ConnectorError::from(anyhow!(
            "additional column ({}) alias is required, you can use `INCLUDE ... AS <alias>` to specify the alias",
            additional_col_type
        )));
    };

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
        _ => unreachable!(),
    };

    Ok(col_desc)
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
) -> ([bool; 2], [ColumnDesc; 2]) {
    let mut columns_exist = [false; 2];

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
        let compat_col_types = COMPATIBLE_ADDITIONAL_COLUMNS
            .get(connector_name)
            .unwrap_or(&COMMON_COMPATIBLE_ADDITIONAL_COLUMNS);
        ["partition", "file", "offset"]
            .iter()
            .filter_map(|col_type| {
                if compat_col_types.contains(col_type) {
                    Some(
                        build_additional_column_desc(
                            assign_col_id(),
                            connector_name,
                            col_type,
                            None,
                            None,
                            None,
                            false,
                            false,
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

    (columns_exist, additional_columns.try_into().unwrap())
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
            DataType::List(get_kafka_header_item_datatype().into()),
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
}
