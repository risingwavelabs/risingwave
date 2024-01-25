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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as PbDataType;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::{
    AdditionalColumn, AdditionalColumnFilename, AdditionalColumnHeader, AdditionalColumnHeaders,
    AdditionalColumnKey, AdditionalColumnOffset, AdditionalColumnPartition,
    AdditionalColumnTimestamp,
};

use crate::source::{
    GCS_CONNECTOR, KAFKA_CONNECTOR, KINESIS_CONNECTOR, OPENDAL_S3_CONNECTOR, PULSAR_CONNECTOR,
    S3_CONNECTOR,
};

// Hidden additional columns connectors which do not support `include` syntax.
pub static COMMON_COMPATIBLE_ADDITIONAL_COLUMNS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["partition", "offset"]));

pub static COMPATIBLE_ADDITIONAL_COLUMNS: LazyLock<HashMap<&'static str, HashSet<&'static str>>> =
    LazyLock::new(|| {
        HashMap::from([
            (
                KAFKA_CONNECTOR,
                HashSet::from(["key", "timestamp", "partition", "offset", "header"]),
            ),
            (
                PULSAR_CONNECTOR,
                HashSet::from(["key", "partition", "offset"]),
            ),
            (
                KINESIS_CONNECTOR,
                HashSet::from(["key", "partition", "offset", "timestamp"]),
            ),
            (OPENDAL_S3_CONNECTOR, HashSet::from(["file", "offset"])),
            (S3_CONNECTOR, HashSet::from(["file", "offset"])),
            (GCS_CONNECTOR, HashSet::from(["file", "offset"])),
        ])
    });

pub fn gen_default_addition_col_name(
    connector_name: &str,
    additional_col_type: &str,
    inner_field_name: Option<&str>,
    data_type: Option<&str>,
) -> String {
    let col_name = [
        Some(connector_name),
        Some(additional_col_type),
        inner_field_name,
        data_type,
    ];
    col_name.iter().fold("_rw".to_string(), |name, ele| {
        if let Some(ele) = ele {
            format!("{}_{}", name, ele)
        } else {
            name
        }
    })
}

pub fn build_additional_column_catalog(
    column_id: ColumnId,
    connector_name: &str,
    additional_col_type: &str,
    column_alias: Option<String>,
    inner_field_name: Option<&str>,
    data_type: Option<&str>,
    reject_unknown_connector: bool,
) -> Result<ColumnCatalog> {
    let compatible_columns = match (
        COMPATIBLE_ADDITIONAL_COLUMNS.get(connector_name),
        reject_unknown_connector,
    ) {
        (Some(compat_cols), _) => compat_cols,
        (None, false) => &COMMON_COMPATIBLE_ADDITIONAL_COLUMNS,
        (None, true) => {
            return Err(format!(
                "additional column is not supported for connector {}, acceptable connectors: {:?}",
                connector_name,
                COMPATIBLE_ADDITIONAL_COLUMNS.keys(),
            )
            .into())
        }
    };
    if !compatible_columns.contains(additional_col_type) {
        return Err(format!(
            "additional column type {} is not supported for connector {}, acceptable column types: {:?}",
            additional_col_type, connector_name, compatible_columns
        ).into());
    }

    let column_name = column_alias.unwrap_or_else(|| {
        gen_default_addition_col_name(
            connector_name,
            additional_col_type,
            inner_field_name,
            data_type,
        )
    });

    let catalog = match additional_col_type {
        "key" => ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                column_name,
                column_id,
                DataType::Bytea,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Key(AdditionalColumnKey {})),
                },
            ),
            is_hidden: false,
        },
        "timestamp" => ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                column_name,
                column_id,
                DataType::Timestamptz,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Timestamp(
                        AdditionalColumnTimestamp {},
                    )),
                },
            ),
            is_hidden: false,
        },
        "partition" => ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                column_name,
                column_id,
                DataType::Varchar,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Partition(
                        AdditionalColumnPartition {},
                    )),
                },
            ),
            is_hidden: false,
        },
        "offset" => ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                column_name,
                column_id,
                DataType::Varchar,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Offset(AdditionalColumnOffset {})),
                },
            ),
            is_hidden: false,
        },
        "file" => ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                column_name,
                column_id,
                DataType::Varchar,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Filename(AdditionalColumnFilename {})),
                },
            ),
            is_hidden: false,
        },
        "header" => build_header_catalog(column_id, &column_name, inner_field_name, data_type),
        _ => unreachable!(),
    };

    Ok(catalog)
}

fn build_header_catalog(
    column_id: ColumnId,
    col_name: &str,
    inner_field_name: Option<&str>,
    data_type: Option<&str>,
) -> ColumnCatalog {
    if let Some(inner) = inner_field_name {
        let (data_type, pb_data_type) = {
            if let Some(type_name) = data_type {
                match type_name {
                    "bytea" => (
                        DataType::Bytea,
                        PbDataType {
                            type_name: TypeName::Bytea as i32,
                            ..Default::default()
                        },
                    ),
                    "varchar" => (
                        DataType::Varchar,
                        PbDataType {
                            type_name: TypeName::Varchar as i32,
                            ..Default::default()
                        },
                    ),
                    _ => unreachable!(),
                }
            } else {
                (
                    DataType::Bytea,
                    PbDataType {
                        type_name: TypeName::Bytea as i32,
                        ..Default::default()
                    },
                )
            }
        };
        ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                col_name,
                column_id,
                data_type,
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::HeaderInner(AdditionalColumnHeader {
                        inner_field: inner.to_string(),
                        data_type: Some(pb_data_type),
                    })),
                },
            ),
            is_hidden: false,
        }
    } else {
        ColumnCatalog {
            column_desc: ColumnDesc::named_with_additional_column(
                col_name,
                column_id,
                DataType::List(get_kafka_header_item_datatype().into()),
                AdditionalColumn {
                    column_type: Some(AdditionalColumnType::Headers(AdditionalColumnHeaders {})),
                },
            ),
            is_hidden: false,
        }
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
            gen_default_addition_col_name("kafka", "header", Some("inner"), Some("varchar")),
            "_rw_kafka_header_inner_varchar"
        );
    }
}
