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

impl MockCatalog {
    const RANGE_TABLE: &'static str = "range_table";
    const SPARSE_TABLE: &'static str = "sparse_table";

    fn sparse_table(self: &Arc<Self>) -> Table {
        Table::builder_from_catalog(
            {
                let builder = Memory::default().root("/tmp");
                Operator::new(builder).unwrap().finish()
            },
            self.clone(),
            TableMetadata {
                format_version: icelake::types::TableFormatVersion::V2,
                table_uuid: "1".to_owned(),
                location: "1".to_owned(),
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
                            icelake::types::Any::Primitive(icelake::types::Primitive::Long),
                        )
                        .into(),
                        Field::required(
                            3,
                            "v3",
                            icelake::types::Any::Primitive(icelake::types::Primitive::String),
                        )
                        .into(),
                        Field::required(
                            4,
                            "v4",
                            icelake::types::Any::Primitive(icelake::types::Primitive::Time),
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
                            partition_field_id: 5,
                            transform: icelake::types::Transform::Identity,
                            name: "f1".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 6,
                            transform: icelake::types::Transform::Bucket(1),
                            name: "f2".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 3,
                            partition_field_id: 7,
                            transform: icelake::types::Transform::Truncate(1),
                            name: "f3".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 4,
                            partition_field_id: 8,
                            transform: icelake::types::Transform::Void,
                            name: "f4".to_owned(),
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
            TableIdentifier::new(vec![Self::SPARSE_TABLE]).unwrap(),
        )
        .build()
        .unwrap()
    }

    fn range_table(self: &Arc<Self>) -> Table {
        Table::builder_from_catalog(
            {
                let builder = Memory::default().root("/tmp");
                Operator::new(builder).unwrap().finish()
            },
            self.clone(),
            TableMetadata {
                format_version: icelake::types::TableFormatVersion::V2,
                table_uuid: "1".to_owned(),
                location: "1".to_owned(),
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
                            icelake::types::Any::Primitive(icelake::types::Primitive::Date),
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
                        Field::required(
                            4,
                            "v4",
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
                            partition_field_id: 7,
                            transform: icelake::types::Transform::Year,
                            name: "f4".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 8,
                            transform: icelake::types::Transform::Month,
                            name: "f5".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 3,
                            partition_field_id: 9,
                            transform: icelake::types::Transform::Day,
                            name: "f6".to_owned(),
                        },
                        PartitionField {
                            source_column_id: 4,
                            partition_field_id: 10,
                            transform: icelake::types::Transform::Hour,
                            name: "f7".to_owned(),
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
            TableIdentifier::new(vec![Self::RANGE_TABLE]).unwrap(),
        )
        .build()
        .unwrap()
    }
}

#[async_trait]
impl Catalog for MockCatalog {
    fn name(&self) -> &str {
        "mock"
    }

    // Mock catalog load mock table according to table_name, there is 2 kinds of table for test:
    // 1. sparse partition table
    // 2. range partition table
    async fn load_table(self: Arc<Self>, table_name: &TableIdentifier) -> icelake::Result<Table> {
        match table_name.name.as_ref() {
            Self::SPARSE_TABLE => Ok(self.sparse_table()),
            Self::RANGE_TABLE => Ok(self.range_table()),
            _ => unimplemented!("table {} not found", table_name),
        }
    }

    async fn update_table(self: Arc<Self>, _update_table: &UpdateTable) -> icelake::Result<Table> {
        unimplemented!()
    }
}
