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

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, TableMetadataBuilder, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{
    Catalog as CatalogV2, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};

#[cfg(test)]
pub use self::test_tables::MockTableGuard;

#[cfg(test)]
mod test_tables {
    use std::collections::HashMap;
    use std::collections::hash_map::Entry;
    use std::sync::LazyLock;

    use iceberg::table::Table;
    use iceberg::{TableCommit, TableIdent};
    use parking_lot::Mutex;

    use super::MockCatalog;

    static TABLES: LazyLock<Mutex<HashMap<TableIdent, Table>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// A registered table that is removed from the mock catalog when dropped.
    #[must_use]
    pub struct MockTableGuard {
        ident: TableIdent,
    }

    impl Drop for MockTableGuard {
        fn drop(&mut self) {
            TABLES.lock().remove(&self.ident);
        }
    }

    impl MockCatalog {
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

        pub(super) fn registered_table(ident: &TableIdent) -> Option<Table> {
            TABLES.lock().get(ident).cloned()
        }

        /// Apply a commit to a registered table. Returns the commit back if the table is not
        /// registered.
        pub(super) fn update_test_table(
            commit: TableCommit,
        ) -> Result<iceberg::Result<Table>, Box<TableCommit>> {
            let mut tables = TABLES.lock();
            let Some(table) = tables.get_mut(commit.identifier()) else {
                return Err(Box::new(commit));
            };
            Ok(apply_commit(table, commit))
        }
    }

    fn apply_commit(table: &mut Table, mut commit: TableCommit) -> iceberg::Result<Table> {
        for requirement in commit.take_requirements() {
            requirement.check(Some(table.metadata()))?;
        }
        let mut metadata_builder = table
            .metadata()
            .clone()
            .into_builder(table.metadata_location().map(str::to_owned));
        for update in commit.take_updates() {
            metadata_builder = update.apply(metadata_builder)?;
        }
        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .metadata(metadata_builder.build()?.metadata);
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location);
        }
        let updated = builder.build()?;
        *table = updated.clone();
        Ok(updated)
    }
}

/// A mock catalog for iceberg used for plan test.
#[derive(Debug)]
pub struct MockCatalog;

impl MockCatalog {
    const RANGE_TABLE: &'static str = "range_table";
    const SPARSE_TABLE: &'static str = "sparse_table";
}

impl MockCatalog {
    fn build_table(name: &str, schema: Schema, partition_spec: UnboundPartitionSpec) -> Table {
        let file_io = FileIO::from_path("memory://").unwrap().build().unwrap();
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
        #[cfg(test)]
        if let Some(table) = Self::registered_table(table) {
            return Ok(table);
        }
        match table.name.as_ref() {
            Self::SPARSE_TABLE => Ok(Self::sparse_table()),
            Self::RANGE_TABLE => Ok(Self::range_table()),
            _ => unimplemented!("table {} not found", table.name()),
        }
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        #[cfg(test)]
        if Self::registered_table(table).is_some() {
            return Ok(true);
        }
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
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        #[cfg(test)]
        let commit = match Self::update_test_table(commit) {
            Ok(result) => return result,
            Err(commit) => *commit,
        };
        let _ = commit;
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
