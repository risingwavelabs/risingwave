use std::collections::HashMap;
use std::sync::LazyLock;

use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::AdditionalColumnType;

use crate::source::{
    KAFKA_CONNECTOR, KINESIS_CONNECTOR, PULSAR_CONNECTOR, S3_CONNECTOR, S3_V2_CONNECTOR,
};

pub type CompatibleAdditionalColumnsFn =
    Box<dyn Fn(ColumnId, &str) -> ColumnCatalog + Send + Sync + 'static>;

pub static CONNECTOR_COMPATIBLE_ADDITIONAL_COLUMNS: LazyLock<
    HashMap<String, Vec<(&'static str, CompatibleAdditionalColumnsFn)>>,
> = LazyLock::new(|| {
    let mut res: HashMap<String, Vec<(&'static str, CompatibleAdditionalColumnsFn)>> =
        HashMap::new();

    res.insert(
        KAFKA_CONNECTOR.to_string(),
        vec![
            (
                "key",
                Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                    ColumnCatalog {
                        column_desc: ColumnDesc::named_with_additional_column(
                            name,
                            id,
                            DataType::Bytea,
                            AdditionalColumnType::Key,
                        ),
                        is_hidden: false,
                    }
                }),
            ),
            (
                "timestamp",
                Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                    ColumnCatalog {
                        column_desc: ColumnDesc::named_with_additional_column(
                            name,
                            id,
                            DataType::Timestamptz,
                            AdditionalColumnType::Timestamp,
                        ),
                        is_hidden: false,
                    }
                }),
            ),
            (
                "partition",
                Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                    ColumnCatalog {
                        column_desc: ColumnDesc::named_with_additional_column(
                            name,
                            id,
                            DataType::Int64,
                            AdditionalColumnType::Partition,
                        ),
                        is_hidden: false,
                    }
                }),
            ),
            (
                "offset",
                Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                    ColumnCatalog {
                        column_desc: ColumnDesc::named_with_additional_column(
                            name,
                            id,
                            DataType::Int64,
                            AdditionalColumnType::Offset,
                        ),
                        is_hidden: false,
                    }
                }),
            ),
            // Todo(tabVersion): add header column desc
            // (
            //     "header",
            //     Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
            //         ColumnCatalog {
            //             column_desc: ColumnDesc::named(name, id, DataType::List(
            //
            //             )),
            //             is_hidden: false,
            //         }
            //     }),
            // ),
        ],
    );
    res.insert(
        PULSAR_CONNECTOR.to_string(),
        vec![(
            "key",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Bytea,
                        AdditionalColumnType::Key,
                    ),
                    is_hidden: false,
                }
            }),
        )],
    );
    res.insert(
        KINESIS_CONNECTOR.to_string(),
        vec![(
            "key",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Bytea,
                        AdditionalColumnType::Key,
                    ),
                    is_hidden: false,
                }
            }),
        )],
    );
    res.insert(
        S3_CONNECTOR.to_string(),
        vec![(
            "file",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Filename,
                    ),
                    is_hidden: false,
                }
            }),
        )],
    );
    res.insert(
        // TODO(tabVersion): change to Opendal S3 and GCS
        S3_V2_CONNECTOR.to_string(),
        vec![(
            "file",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Filename,
                    ),
                    is_hidden: false,
                }
            }),
        )],
    );

    res
});
