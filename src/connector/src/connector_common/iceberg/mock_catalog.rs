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
#[derive(Debug)]
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
                            name: "f1".to_string(),
                        },
                        PartitionField {
                            source_column_id: 2,
                            partition_field_id: 6,
                            transform: icelake::types::Transform::Bucket(1),
                            name: "f2".to_string(),
                        },
                        PartitionField {
                            source_column_id: 3,
                            partition_field_id: 7,
                            transform: icelake::types::Transform::Truncate(1),
                            name: "f3".to_string(),
                        },
                        PartitionField {
                            source_column_id: 4,
                            partition_field_id: 8,
                            transform: icelake::types::Transform::Void,
                            name: "f4".to_string(),
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
                            source_column_id: 4,
                            partition_field_id: 10,
                            transform: icelake::types::Transform::Hour,
                            name: "f7".to_string(),
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

mod v2 {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, TableMetadataBuilder, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use iceberg::table::Table as TableV2;
    use iceberg::{
        Catalog as CatalogV2, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    };

    use super::MockCatalog;

    impl MockCatalog {
        fn build_table_v2(
            name: &str,
            schema: Schema,
            partition_spec: UnboundPartitionSpec,
        ) -> TableV2 {
            let file_io = FileIO::from_path("memory://").unwrap().build().unwrap();
            let table_creation = TableCreation {
                name: "ignore".to_string(),
                location: Some("1".to_string()),
                schema,
                partition_spec: Some(partition_spec),
                sort_order: None,
                properties: HashMap::new(),
            };
            TableV2::builder()
                .identifier(TableIdent::new(
                    NamespaceIdent::new("mock_namespace".to_string()),
                    name.to_string(),
                ))
                .file_io(file_io)
                .metadata(
                    TableMetadataBuilder::from_table_creation(table_creation)
                        .unwrap()
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap()
        }

        fn sparse_table_v2() -> TableV2 {
            Self::build_table_v2(
                Self::SPARSE_TABLE,
                Schema::builder()
                    .with_fields(vec![
                        NestedField::new(1, "v1", Type::Primitive(PrimitiveType::Int), true).into(),
                        NestedField::new(2, "v2", Type::Primitive(PrimitiveType::Long), true)
                            .into(),
                        NestedField::new(3, "v3", Type::Primitive(PrimitiveType::String), true)
                            .into(),
                        NestedField::new(4, "v4", Type::Primitive(PrimitiveType::Time), true)
                            .into(),
                    ])
                    .build()
                    .unwrap(),
                UnboundPartitionSpec::builder()
                    .with_spec_id(1)
                    .add_partition_fields(vec![
                        UnboundPartitionField {
                            source_id: 1,
                            field_id: Some(5),
                            name: "f1".to_string(),
                            transform: Transform::Identity,
                        },
                        UnboundPartitionField {
                            source_id: 2,
                            field_id: Some(6),
                            name: "f2".to_string(),
                            transform: Transform::Bucket(1),
                        },
                        UnboundPartitionField {
                            source_id: 3,
                            field_id: Some(7),
                            name: "f3".to_string(),
                            transform: Transform::Truncate(1),
                        },
                        UnboundPartitionField {
                            source_id: 4,
                            field_id: Some(8),
                            name: "f4".to_string(),
                            transform: Transform::Void,
                        },
                    ])
                    .unwrap()
                    .build(),
            )
        }

        fn range_table_v2() -> TableV2 {
            Self::build_table_v2(
                Self::RANGE_TABLE,
                Schema::builder()
                    .with_fields(vec![
                        NestedField::new(1, "v1", Type::Primitive(PrimitiveType::Date), true)
                            .into(),
                        NestedField::new(2, "v2", Type::Primitive(PrimitiveType::Timestamp), true)
                            .into(),
                        NestedField::new(
                            3,
                            "v3",
                            Type::Primitive(PrimitiveType::Timestamptz),
                            true,
                        )
                        .into(),
                        NestedField::new(
                            4,
                            "v4",
                            Type::Primitive(PrimitiveType::Timestamptz),
                            true,
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
                UnboundPartitionSpec::builder()
                    .with_spec_id(1)
                    .add_partition_fields(vec![
                        UnboundPartitionField {
                            source_id: 1,
                            field_id: Some(5),
                            name: "f1".to_string(),
                            transform: Transform::Year,
                        },
                        UnboundPartitionField {
                            source_id: 2,
                            field_id: Some(6),
                            name: "f2".to_string(),
                            transform: Transform::Month,
                        },
                        UnboundPartitionField {
                            source_id: 3,
                            field_id: Some(7),
                            name: "f3".to_string(),
                            transform: Transform::Day,
                        },
                        UnboundPartitionField {
                            source_id: 4,
                            field_id: Some(8),
                            name: "f4".to_string(),
                            transform: Transform::Hour,
                        },
                    ])
                    .unwrap()
                    .build(),
            )
        }
    }

    #[async_trait]
    impl CatalogV2 for MockCatalog {
        /// List namespaces from table.
        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            todo!()
        }

        /// Create a new namespace inside the catalog.
        async fn create_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            todo!()
        }

        /// Get a namespace information from the catalog.
        async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
            todo!()
        }

        /// Check if namespace exists in catalog.
        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            todo!()
        }

        /// Drop a namespace from the catalog.
        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            todo!()
        }

        /// List tables from namespace.
        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            todo!()
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            todo!()
        }

        /// Create a new table inside the namespace.
        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<TableV2> {
            todo!()
        }

        /// Load table from the catalog.
        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<TableV2> {
            match table.name.as_ref() {
                Self::SPARSE_TABLE => Ok(Self::sparse_table_v2()),
                Self::RANGE_TABLE => Ok(Self::range_table_v2()),
                _ => unimplemented!("table {} not found", table.name()),
            }
        }

        /// Drop a table from the catalog.
        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            todo!()
        }

        /// Check if a table exists in the catalog.
        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            match table.name.as_ref() {
                Self::SPARSE_TABLE => Ok(true),
                Self::RANGE_TABLE => Ok(true),
                _ => Ok(false),
            }
        }

        /// Rename a table in the catalog.
        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            todo!()
        }

        /// Update a table to the catalog.
        async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<TableV2> {
            todo!()
        }
    }
}
