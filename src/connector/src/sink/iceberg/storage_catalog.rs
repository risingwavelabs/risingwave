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
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use opendal::Operator;
use thiserror_ext::AsReport;
use tokio_stream::StreamExt;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
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
    config: StorageCatalogConfig,
}

impl StorageCatalog {
    pub fn new(config: StorageCatalogConfig) -> Result<Self> {
        let mut file_io_builder = FileIO::from_path(&config.warehouse)?
            .with_prop(S3_ACCESS_KEY_ID, &config.access_key)
            .with_prop(S3_SECRET_ACCESS_KEY, &config.secret_key);
        file_io_builder = if let Some(endpoint) = &config.endpoint {
            file_io_builder.with_prop(S3_ENDPOINT, endpoint)
        } else {
            file_io_builder
        };
        file_io_builder = if let Some(region) = &config.region {
            file_io_builder.with_prop(S3_REGION, region)
        } else {
            file_io_builder
        };

        Ok(StorageCatalog {
            warehouse: config.warehouse.clone(),
            file_io: file_io_builder.build()?,
            config,
        })
    }

    /// Check if version hint file exist.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn is_version_hint_exist(&self, table_path: &str) -> Result<bool> {
        self.file_io
            .is_exist(format!("{table_path}/metadata/version-hint.text").as_str())
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("check if version hint exist failed: {}", err.as_report()),
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
                format!(
                    "Fail to covert version_hint from utf8 to string: {}",
                    err.as_report()
                ),
            )
        })?;

        version_hint
            .parse()
            .map_err(|_| Error::new(ErrorKind::DataInvalid, "parse version hint failed"))
    }

    /// List all paths of table metadata files.
    ///
    /// The returned paths are sorted by name.
    ///
    /// TODO: we can improve this by only fetch the latest metadata.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn list_table_metadata_paths(&self, table_path: &str) -> Result<Vec<String>> {
        // create s3 operator
        let mut builder = opendal::services::S3::default();
        builder
            .root(&self.warehouse)
            .access_key_id(&self.config.access_key)
            .secret_access_key(&self.config.secret_key);
        if let Some(endpoint) = &self.config.endpoint {
            builder.endpoint(endpoint);
        }
        if let Some(region) = &self.config.region {
            builder.region(region);
        }
        let op: Operator = Operator::new(builder)
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.to_report_string()))?
            .finish();

        // list metadata files
        let mut lister = op
            .lister(format!("{table_path}/metadata/").as_str())
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("list metadata failed: {}", err.as_report()),
                )
            })?;
        let mut paths = vec![];
        while let Some(entry) = lister.next().await {
            let entry = entry.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("list metadata entry failed: {}", err.as_report()),
                )
            })?;

            // Only push into paths if the entry is a metadata file.
            if entry.path().ends_with(".metadata.json") {
                paths.push(entry.path().to_string());
            }
        }

        // Make the returned paths sorted by name.
        paths.sort();

        Ok(paths)
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
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
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let table_path = {
            let mut names = table_ident.namespace.clone().inner();
            names.push(table_ident.name.to_string());
            if self.warehouse.ends_with('/') {
                format!("{}{}", self.warehouse, names.join("/"))
            } else {
                format!("{}/{}", self.warehouse, names.join("/"))
            }
        };

        // Create the metadata directory
        let metadata_path = format!("{table_path}/metadata");

        // Create the initial table metadata
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;

        // Write the initial metadata file
        let metadata_file_path = format!("{metadata_path}/v1.metadata.json");
        let metadata_json = serde_json::to_string(&table_metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;

        // Write the version hint file
        let version_hint_path = format!("{table_path}/metadata/version-hint.text");
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output.write("1".into()).await?;

        Table::builder()
            .metadata(table_metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let table_path = {
            let mut names = table.namespace.clone().inner();
            names.push(table.name.to_string());
            if self.warehouse.ends_with('/') {
                format!("{}{}", self.warehouse, names.join("/"))
            } else {
                format!("{}/{}", self.warehouse, names.join("/"))
            }
        };
        let path = if self.is_version_hint_exist(&table_path).await? {
            let version_hint = self.read_version_hint(&table_path).await?;
            format!("{table_path}/metadata/v{}.metadata.json", version_hint)
        } else {
            let files = self.list_table_metadata_paths(&table_path).await?;

            files.into_iter().last().ok_or(Error::new(
                ErrorKind::DataInvalid,
                "no table metadata found",
            ))?
        };

        let metadata_file = self.file_io.new_input(path)?;
        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;

        Table::builder()
            .metadata(table_metadata)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            // Only support readonly table for storage catalog now.
            .readonly(true)
            .build()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        let table_path = {
            let mut names = table.namespace.clone().inner();
            names.push(table.name.to_string());
            if self.warehouse.ends_with('/') {
                format!("{}{}", self.warehouse, names.join("/"))
            } else {
                format!("{}/{}", self.warehouse, names.join("/"))
            }
        };
        let metadata_path = format!("{table_path}/metadata/version-hint.text");
        self.file_io.is_exist(&metadata_path).await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to check if table exists: {}", err.as_report()),
            )
        })
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
