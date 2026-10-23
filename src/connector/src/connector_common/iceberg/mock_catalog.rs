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
use std::collections::hash_map::Entry;
use std::sync::LazyLock;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, TableMetadataBuilder, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{
    Catalog as CatalogV2, Namespace, NamespaceIdent, Runtime, TableCommit, TableCreation,
    TableIdent,
};
use parking_lot::Mutex;

static TABLES: LazyLock<Mutex<HashMap<TableIdent, Table>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A registered table that is removed from the mock catalog when dropped.
#[must_use]
pub struct MockTableGuard {
    ident: TableIdent,
}

impl MockTableGuard {
    pub fn update(&self, table: Table) {
        assert_eq!(table.identifier(), &self.ident);
        *TABLES.lock().get_mut(&self.ident).unwrap() = table;
    }
}

impl Drop for MockTableGuard {
    fn drop(&mut self) {
        TABLES.lock().remove(&self.ident);
    }
}

/// A mock catalog for Iceberg planner and executor tests.
#[derive(Debug)]
pub struct MockCatalog;

impl MockCatalog {
    const RANGE_TABLE: &'static str = "range_table";
    const SPARSE_TABLE: &'static str = "sparse_table";

    /// Register a table shared by mock catalog instances. Use distinct identifiers in parallel tests.
    pub fn register(table: Table) -> MockTableGuard {
        let ident = table.identifier().clone();
        match TABLES.lock().entry(ident.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(table);
            }
            Entry::Occupied(_) => panic!("mock table {ident} is already registered"),
        }
        MockTableGuard { ident }
    }

    fn updates_table(name: &str, version: iceberg::spec::FormatVersion) -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(10, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(20, "value", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(30, "_row_id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            iceberg::spec::SortOrder::unsorted_order(),
            "memory://updates".to_owned(),
            version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        // No writer properties, like tables created by older PK-index sinks.
        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("mock_namespace".to_owned()),
                name.to_owned(),
            ))
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::try_current().unwrap())
            .metadata(metadata)
            .build()
            .unwrap()
    }
}

impl MockCatalog {
    fn build_table(name: &str, schema: Schema, partition_spec: UnboundPartitionSpec) -> Table {
        let file_io = FileIO::new_with_memory();
        let table_creation = TableCreation {
            name: "ignore".to_owned(),
            location: Some("1".to_owned()),
            schema,
            partition_spec: Some(partition_spec),
            sort_order: None,
            properties: HashMap::new(),
            format_version: iceberg::spec::FormatVersion::V2,
        };
        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("mock_namespace".to_owned()),
                name.to_owned(),
            ))
            .file_io(file_io)
            .runtime(Runtime::try_current().unwrap())
            .metadata(
                TableMetadataBuilder::from_table_creation(table_creation)
                    .unwrap()
                    .build()
                    .unwrap()
                    .metadata,
            )
            .build()
            .unwrap()
    }

    fn sparse_table() -> Table {
        Self::build_table(
            Self::SPARSE_TABLE,
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "v1", Type::Primitive(PrimitiveType::Int), true).into(),
                    NestedField::new(2, "v2", Type::Primitive(PrimitiveType::Long), true).into(),
                    NestedField::new(3, "v3", Type::Primitive(PrimitiveType::String), true).into(),
                    NestedField::new(4, "v4", Type::Primitive(PrimitiveType::Time), true).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder()
                .with_spec_id(1)
                .add_partition_fields(vec![
                    UnboundPartitionField {
                        source_id: 1,
                        field_id: Some(5),
                        name: "f1".to_owned(),
                        transform: Transform::Identity,
                    },
                    UnboundPartitionField {
                        source_id: 2,
                        field_id: Some(6),
                        name: "f2".to_owned(),
                        transform: Transform::Bucket(1),
                    },
                    UnboundPartitionField {
                        source_id: 3,
                        field_id: Some(7),
                        name: "f3".to_owned(),
                        transform: Transform::Truncate(1),
                    },
                    UnboundPartitionField {
                        source_id: 4,
                        field_id: Some(8),
                        name: "f4".to_owned(),
                        transform: Transform::Void,
                    },
                ])
                .unwrap()
                .build(),
        )
    }

    fn range_table() -> Table {
        Self::build_table(
            Self::RANGE_TABLE,
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "v1", Type::Primitive(PrimitiveType::Date), true).into(),
                    NestedField::new(2, "v2", Type::Primitive(PrimitiveType::Timestamp), true)
                        .into(),
                    NestedField::new(3, "v3", Type::Primitive(PrimitiveType::Timestamptz), true)
                        .into(),
                    NestedField::new(4, "v4", Type::Primitive(PrimitiveType::Timestamptz), true)
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
                        name: "f1".to_owned(),
                        transform: Transform::Year,
                    },
                    UnboundPartitionField {
                        source_id: 2,
                        field_id: Some(6),
                        name: "f2".to_owned(),
                        transform: Transform::Month,
                    },
                    UnboundPartitionField {
                        source_id: 3,
                        field_id: Some(7),
                        name: "f3".to_owned(),
                        transform: Transform::Day,
                    },
                    UnboundPartitionField {
                        source_id: 4,
                        field_id: Some(8),
                        name: "f4".to_owned(),
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
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
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
    ) -> iceberg::Result<Table> {
        todo!()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if let Some(table) = TABLES.lock().get(table).cloned() {
            return Ok(table);
        }
        match table.name.as_ref() {
            "updates_v2" => Ok(Self::updates_table(
                table.name(),
                iceberg::spec::FormatVersion::V2,
            )),
            "updates_v3" => Ok(Self::updates_table(
                table.name(),
                iceberg::spec::FormatVersion::V3,
            )),
            Self::SPARSE_TABLE => Ok(Self::sparse_table()),
            Self::RANGE_TABLE => Ok(Self::range_table()),
            _ => unimplemented!("table {} not found", table.name()),
        }
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.drop_table(table).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        if TABLES.lock().contains_key(table) {
            return Ok(true);
        }
        match table.name.as_ref() {
            "updates_v2" | "updates_v3" => Ok(true),
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
    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        todo!()
    }

    #[expect(
        clippy::disallowed_types,
        reason = "iceberg catalog trait requires returning iceberg::Error"
    )]
    async fn register_table(
        &self,
        _table_ident: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "register_table is not supported in mock catalog",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registered_tables_are_isolated_and_scoped() -> iceberg::Result<()> {
        use iceberg::spec::FormatVersion;

        let first = MockCatalog::updates_table(
            &format!("registered_{}", uuid::Uuid::new_v4().simple()),
            FormatVersion::V2,
        );
        let second = MockCatalog::updates_table(
            &format!("registered_{}", uuid::Uuid::new_v4().simple()),
            FormatVersion::V2,
        );
        let first_guard = MockCatalog::register(first.clone());
        let second_guard = MockCatalog::register(second.clone());
        let catalog = MockCatalog;
        assert!(catalog.table_exists(first.identifier()).await?);
        assert!(catalog.table_exists(second.identifier()).await?);
        assert_eq!(
            catalog
                .load_table(first.identifier())
                .await?
                .metadata()
                .uuid(),
            first.metadata().uuid()
        );

        let updated = MockCatalog::updates_table(first.identifier().name(), FormatVersion::V3);
        first_guard.update(updated.clone());
        assert_eq!(
            catalog
                .load_table(first.identifier())
                .await?
                .metadata()
                .uuid(),
            updated.metadata().uuid()
        );
        assert_eq!(
            catalog
                .load_table(second.identifier())
                .await?
                .metadata()
                .uuid(),
            second.metadata().uuid()
        );
        drop(first_guard);
        assert!(!catalog.table_exists(first.identifier()).await?);
        assert!(catalog.table_exists(second.identifier()).await?);
        drop(second_guard);
        assert!(!catalog.table_exists(second.identifier()).await?);
        Ok(())
    }
}
