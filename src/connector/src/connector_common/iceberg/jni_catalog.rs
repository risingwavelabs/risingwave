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

//! This module provide jni catalog.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::table::Table as TableV2;
use iceberg::{
    Catalog as CatalogV2, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use icelake::catalog::models::{CommitTableRequest, CommitTableResponse, LoadTableResult};
use icelake::catalog::{
    BaseCatalogConfig, Catalog, IcebergTableIoArgs, OperatorCreator, UpdateTable,
};
use icelake::{ErrorKind, Table, TableIdentifier};
use itertools::Itertools;
use jni::objects::{GlobalRef, JObject};
use jni::JavaVM;
use risingwave_common::bail;
use risingwave_jni_core::call_method;
use risingwave_jni_core::jvm_runtime::{execute_with_jni_env, jobj_to_str, JVM};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub _config: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateTableRequest {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    pub write_order: Option<SortOrder>,
    /// The properties of the table.
    pub properties: HashMap<String, String>,
}

impl From<&TableCreation> for CreateTableRequest {
    fn from(value: &TableCreation) -> Self {
        Self {
            name: value.name.clone(),
            location: value.location.clone(),
            schema: value.schema.clone(),
            partition_spec: value.partition_spec.clone(),
            write_order: value.sort_order.clone(),
            properties: value.properties.clone(),
        }
    }
}

#[derive(Debug)]
pub struct JniCatalog {
    java_catalog: GlobalRef,
    jvm: &'static JavaVM,
    config: BaseCatalogConfig,
}

#[async_trait]
impl Catalog for JniCatalog {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn load_table(self: Arc<Self>, table_name: &TableIdentifier) -> icelake::Result<Table> {
        execute_with_jni_env(self.jvm, |env| {
            let table_name_str = table_name.to_string();

            let table_name_jstr = env.new_string(&table_name_str).unwrap();

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String loadTable(String)},
                &table_name_jstr)
                .with_context(|| format!("Failed to load iceberg table: {table_name_str}"))?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: LoadTableResult = serde_json::from_str(&rust_json_str)?;

            let metadata_location = resp.metadata_location.clone().ok_or_else(|| {
                icelake::Error::new(
                    ErrorKind::IcebergFeatureUnsupported,
                    "Loading uncommitted table is not supported!",
                )
            })?;

            tracing::info!("Table metadata location of {table_name} is {metadata_location}");

            let table_metadata = resp.table_metadata()?;

            let iceberg_io_args = IcebergTableIoArgs::builder_from_path(&table_metadata.location)?
                .with_args(self.config.table_io_configs.iter())
                .build()?;
            let table_op = iceberg_io_args.create()?;

            Ok(Table::builder_from_catalog(
                table_op,
                self.clone(),
                table_metadata,
                table_name.clone(),
            )
            .build()?)
        })
        .map_err(|e| {
            icelake::Error::new(ErrorKind::Unexpected, "Failed to load iceberg table.")
                .set_source(e)
        })
    }

    async fn update_table(self: Arc<Self>, update_table: &UpdateTable) -> icelake::Result<Table> {
        execute_with_jni_env(self.jvm, |env| {
            let request_str = serde_json::to_string(&CommitTableRequest::try_from(update_table)?)?;

            let request_jni_str = env.new_string(&request_str).with_context(|| {
                format!("Failed to create jni string from request json: {request_str}.")
            })?;

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String updateTable(String)},
                &request_jni_str)
                .with_context(|| {
                    format!(
                        "Failed to update iceberg table: {}",
                        update_table.table_name()
                    )
                })?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let response: CommitTableResponse = serde_json::from_str(&rust_json_str)?;

            tracing::info!(
                "Table metadata location of {} is {}",
                update_table.table_name(),
                response.metadata_location
            );

            let table_metadata = response.metadata()?;

            let args = IcebergTableIoArgs::builder_from_path(&table_metadata.location)?
                .with_args(self.config.table_io_configs.iter())
                .build()?;
            let table_op = args.create()?;

            Ok(Table::builder_from_catalog(
                table_op,
                self.clone(),
                table_metadata,
                update_table.table_name().clone(),
            )
            .build()?)
        })
        .map_err(|e| {
            icelake::Error::new(ErrorKind::Unexpected, "Failed to update iceberg table.")
                .set_source(e)
        })
    }
}

#[async_trait]
impl CatalogV2 for JniCatalog {
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
    ) -> iceberg::Result<TableV2> {
        execute_with_jni_env(self.jvm, |env| {
            let namespace_jstr = if namespace.is_empty() {
                env.new_string("").unwrap()
            } else {
                if namespace.len() > 1 {
                    bail!("Namespace with more than one level is not supported!")
                }
                env.new_string(&namespace[0]).unwrap()
            };

            let creation_str = serde_json::to_string(&CreateTableRequest::from(&creation))?;

            let creation_jstr = env.new_string(&creation_str).unwrap();

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String createTable(String, String)},
                &namespace_jstr, &creation_jstr)
                .with_context(|| format!("Failed to create iceberg table: {}", creation.name))?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

            let metadata_location = resp.metadata_location.ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    "Loading uncommitted table is not supported!",
                )
            })?;

            let table_metadata = resp.metadata;

            let file_io = FileIO::from_path(&metadata_location)?
                .with_props(self.config.table_io_configs.iter())
                .build()?;

            Ok(TableV2::builder()
                .file_io(file_io)
                .identifier(TableIdent::new(namespace.clone(), creation.name))
                .metadata(table_metadata)
                .build())
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to crete iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<TableV2> {
        execute_with_jni_env(self.jvm, |env| {
            let table_name_str = format!(
                "{}.{}",
                table.namespace().clone().inner().into_iter().join("."),
                table.name()
            );

            let table_name_jstr = env.new_string(&table_name_str).unwrap();

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String loadTable(String)},
                &table_name_jstr)
                .with_context(|| format!("Failed to load iceberg table: {table_name_str}"))?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

            let metadata_location = resp.metadata_location.ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    "Loading uncommitted table is not supported!",
                )
            })?;

            tracing::info!("Table metadata location of {table_name_str} is {metadata_location}");

            let table_metadata = resp.metadata;

            let file_io = FileIO::from_path(&metadata_location)?
                .with_props(self.config.table_io_configs.iter())
                .build()?;

            Ok(TableV2::builder()
                .file_io(file_io)
                .identifier(table.clone())
                .metadata(table_metadata)
                .build())
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to load iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        execute_with_jni_env(self.jvm, |env| {
            let table_name_str = format!(
                "{}.{}",
                table.namespace().clone().inner().into_iter().join("."),
                table.name()
            );

            let table_name_jstr = env.new_string(&table_name_str).unwrap();

            let exists =
                call_method!(env, self.java_catalog.as_obj(), {boolean tableExists(String)},
                &table_name_jstr)
                .with_context(|| {
                    format!("Failed to check iceberg table exists: {table_name_str}")
                })?;

            Ok(exists)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to load iceberg table.",
            )
            .with_source(e)
        })
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

impl JniCatalog {
    fn build(
        base_config: BaseCatalogConfig,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Self> {
        let jvm = JVM.get_or_init()?;

        execute_with_jni_env(jvm, |env| {
            // Convert props to string array
            let props = env.new_object_array(
                (java_catalog_props.len() * 2) as i32,
                "java/lang/String",
                JObject::null(),
            )?;
            for (i, (key, value)) in java_catalog_props.iter().enumerate() {
                let key_j_str = env.new_string(key)?;
                let value_j_str = env.new_string(value)?;
                env.set_object_array_element(&props, i as i32 * 2, key_j_str)?;
                env.set_object_array_element(&props, i as i32 * 2 + 1, value_j_str)?;
            }

            let jni_catalog_wrapper = env
                .call_static_method(
                    "com/risingwave/connector/catalog/JniCatalogWrapper",
                    "create",
                    "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)Lcom/risingwave/connector/catalog/JniCatalogWrapper;",
                    &[
                        (&env.new_string(name.to_string()).unwrap()).into(),
                        (&env.new_string(catalog_impl.to_string()).unwrap()).into(),
                        (&props).into(),
                    ],
                )?;

            let jni_catalog = env.new_global_ref(jni_catalog_wrapper.l().unwrap())?;

            Ok(Self {
                java_catalog: jni_catalog,
                jvm,
                config: base_config,
            })
        })
            .map_err(Into::into)
    }

    pub fn build_catalog(
        base_config: BaseCatalogConfig,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Arc<dyn Catalog>> {
        let catalog = Self::build(base_config, name, catalog_impl, java_catalog_props)?;
        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
    }

    pub fn build_catalog_v2(
        base_config: BaseCatalogConfig,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Arc<dyn CatalogV2>> {
        let catalog = Self::build(base_config, name, catalog_impl, java_catalog_props)?;
        Ok(Arc::new(catalog) as Arc<dyn CatalogV2>)
    }
}
