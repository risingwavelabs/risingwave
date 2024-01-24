use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use icelake::catalog::{Catalog, UpdateTable};
use icelake::types::{Field, PartitionField, Schema, Struct, TableMetadata};
use icelake::{Table, TableIdentifier};
use opendal::services::Memory;
use opendal::Operator;

/// A mock catalog for iceberg used for plan test.
pub struct MockCatalog;

#[async_trait]
impl Catalog for MockCatalog {
    fn name(&self) -> &str {
        "mock"
    }

    async fn load_table(self: Arc<Self>, table_name: &TableIdentifier) -> icelake::Result<Table> {
        // A mock table for test
        let table = Table::builder_from_catalog(
            {
                let mut builder = Memory::default();
                builder.root("/tmp");
                Operator::new(builder).unwrap().finish()
            },
            self.clone(),
            TableMetadata {
                format_version: icelake::types::TableFormatVersion::V2,
                table_uuid: "1".to_string(),
                location: "1".to_string(),
                last_sequence_number: 1,
                last_updated_ms: 1,
                last_column_id: 1,
                schemas: vec![Schema::new(
                    1,
                    None,
                    Struct::new(vec![
                        Field::required(
                            1,
                            "v1",
                            icelake::types::Any::Primitive(icelake::types::Primitive::Int),
                        )
                        .into(),
                        Field::required(
                            2,
                            "v2",
                            icelake::types::Any::Primitive(icelake::types::Primitive::Timestamp),
                        )
                        .into(),
                        Field::required(
                            3,
                            "v3",
                            icelake::types::Any::Primitive(icelake::types::Primitive::Timestampz),
                        )
                        .into(),
                    ]),
                )],
                current_schema_id: 1,
                partition_specs: vec![icelake::types::PartitionSpec {
                    spec_id: 1,
                    fields: vec![
                        PartitionField {
                            source_column_id: 1,
                            partition_field_id: 4,
                            transform: icelake::types::Transform::Identity,
                            name: "f1".to_string(),
                        },
                        PartitionField {
                            source_column_id: 1,
                            partition_field_id: 5,
                            transform: icelake::types::Transform::Bucket(1),
                            name: "f2".to_string(),
                        },
                        PartitionField {
                            source_column_id: 1,
                            partition_field_id: 6,
                            transform: icelake::types::Transform::Truncate(1),
                            name: "f3".to_string(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 7,
                            transform: icelake::types::Transform::Year,
                            name: "f4".to_string(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 8,
                            transform: icelake::types::Transform::Month,
                            name: "f5".to_string(),
                        },
                        PartitionField {
                            source_column_id: 3,
                            partition_field_id: 9,
                            transform: icelake::types::Transform::Day,
                            name: "f6".to_string(),
                        },
                        PartitionField {
                            source_column_id: 3,
                            partition_field_id: 10,
                            transform: icelake::types::Transform::Hour,
                            name: "f7".to_string(),
                        },
                        PartitionField {
                            source_column_id: 1,
                            partition_field_id: 11,
                            transform: icelake::types::Transform::Void,
                            name: "f8".to_string(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 12,
                            transform: icelake::types::Transform::Void,
                            name: "f9".to_string(),
                        },
                    ],
                }],
                default_spec_id: 1,
                last_partition_id: 1,
                properties: None,
                current_snapshot_id: None,
                snapshots: None,
                snapshot_log: None,
                metadata_log: None,
                sort_orders: vec![],
                default_sort_order_id: 0,
                refs: HashMap::new(),
            },
            table_name.clone(),
        )
        .build()
        .unwrap();
        Ok(table)
    }

    async fn update_table(self: Arc<Self>, _update_table: &UpdateTable) -> icelake::Result<Table> {
        unimplemented!()
    }
}
