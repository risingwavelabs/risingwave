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

//! This module provide storage catalog.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::io::{FileIO, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct StorageCatalogConfig {
    warehouse: String,
    access_key: String,
    secret_key: String,
    endpoint: Option<String>,
    region: Option<String>,
}

/// File system catalog.
#[derive(Debug)]
pub struct StorageCatalog {
    warehouse: String,
    file_io: FileIO,
}

impl StorageCatalog {
    pub fn new(config: StorageCatalogConfig) -> Result<Self> {
        let mut file_io_builder = FileIO::from_path(&config.warehouse)?
            .with_prop(S3_ACCESS_KEY_ID, config.access_key)
            .with_prop(S3_SECRET_ACCESS_KEY, config.secret_key);
        file_io_builder = if let Some(endpoint) = config.endpoint {
            file_io_builder.with_prop(S3_ENDPOINT, endpoint)
        } else {
            file_io_builder
        };
        file_io_builder = if let Some(region) = config.region {
            file_io_builder.with_prop(S3_REGION, region)
        } else {
            file_io_builder
        };

        Ok(StorageCatalog {
            warehouse: config.warehouse,
            file_io: file_io_builder.build()?,
        })
    }

    /// Check if version hint file exist.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn is_version_hint_exist(&self, table_path: &str) -> Result<bool> {
        self.file_io
            .is_exist(format!("{table_path}/metadata/version-hint.text").as_str())
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("check if version hint exist failed: {}", e),
                )
            })
    }

    /// Read version hint of table.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn read_version_hint(&self, table_path: &str) -> Result<i32> {
        let content = self
            .file_io
            .new_input(format!("{table_path}/metadata/version-hint.text").as_str())?
            .read()
            .await?;
        let version_hint = String::from_utf8(content.to_vec()).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Fail to covert version_hint from utf8 to string: {}", err),
            )
        })?;

        version_hint.parse().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("parse version hint failed: {}", e),
            )
        })
    }
}

#[async_trait]
impl Catalog for StorageCatalog {
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
        let table_path = {
            let mut names = table.namespace.clone().inner();
            names.push(table.name.to_string());
            format!("{}/{}", self.warehouse, names.join("/"))
        };
        let path = if self.is_version_hint_exist(&table_path).await? {
            let version_hint = self.read_version_hint(&table_path).await?;
            format!("{table_path}/metadata/v{}.metadata.json", version_hint)
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "no table version hint found",
            ));
        };

        let metadata_file = self.file_io.new_input(path)?;
        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;

        Ok(Table::builder()
            .metadata(table_metadata)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            // Only support readonly table for storage catalog now.
            .readonly(true)
            .build())
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        todo!()
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Update a table to the catalog.
    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        todo!()
    }
}
