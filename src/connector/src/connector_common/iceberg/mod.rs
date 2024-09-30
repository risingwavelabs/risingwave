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

mod jni_catalog;
mod mock_catalog;
mod storage_catalog;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use icelake::catalog::{
    load_iceberg_base_catalog_config, BaseCatalogConfig, CATALOG_NAME, CATALOG_TYPE,
};
use risingwave_common::bail;
use serde_derive::Deserialize;
use serde_with::serde_as;
use url::Url;
use with_options::WithOptions;

use crate::error::ConnectorResult;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergCommon {
    // Catalog type supported by iceberg, such as "storage", "rest".
    // If not set, we use "storage" as default.
    #[serde(rename = "catalog.type")]
    pub catalog_type: Option<String>,
    #[serde(rename = "s3.region")]
    pub region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "s3.access.key")]
    pub access_key: String,
    #[serde(rename = "s3.secret.key")]
    pub secret_key: String,
    /// Path of iceberg warehouse, only applicable in storage catalog.
    #[serde(rename = "warehouse.path")]
    pub warehouse_path: String,
    /// Catalog name, can be omitted for storage catalog, but
    /// must be set for other catalogs.
    #[serde(rename = "catalog.name")]
    pub catalog_name: Option<String>,
    /// URI of iceberg catalog, only applicable in rest catalog.
    #[serde(rename = "catalog.uri")]
    pub catalog_uri: Option<String>,
    #[serde(rename = "database.name")]
    pub database_name: Option<String>,
    /// Full name of table, must include schema name.
    #[serde(rename = "table.name")]
    pub table_name: String,
}

impl IcebergCommon {
    pub fn catalog_type(&self) -> &str {
        self.catalog_type.as_deref().unwrap_or("storage")
    }

    pub fn catalog_name(&self) -> String {
        self.catalog_name
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "risingwave".to_string())
    }

    /// For both V1 and V2.
    fn build_jni_catalog_configs(
        &self,
        path_style_access: &Option<bool>,
        java_catalog_props: &HashMap<String, String>,
    ) -> ConnectorResult<(BaseCatalogConfig, HashMap<String, String>)> {
        let mut iceberg_configs = HashMap::new();

        let base_catalog_config = {
            let catalog_type = self.catalog_type().to_string();

            iceberg_configs.insert(CATALOG_TYPE.to_string(), catalog_type.clone());
            iceberg_configs.insert(CATALOG_NAME.to_string(), self.catalog_name());

            if let Some(region) = &self.region {
                // icelake
                iceberg_configs.insert(
                    "iceberg.table.io.region".to_string(),
                    region.clone().to_string(),
                );
                // iceberg-rust
                iceberg_configs.insert(
                    ("iceberg.table.io.".to_string() + S3_REGION).to_string(),
                    region.clone().to_string(),
                );
            }

            if let Some(endpoint) = &self.endpoint {
                iceberg_configs.insert(
                    "iceberg.table.io.endpoint".to_string(),
                    endpoint.clone().to_string(),
                );

                // iceberg-rust
                iceberg_configs.insert(
                    ("iceberg.table.io.".to_string() + S3_ENDPOINT).to_string(),
                    endpoint.clone().to_string(),
                );
            }

            // icelake
            iceberg_configs.insert(
                "iceberg.table.io.access_key_id".to_string(),
                self.access_key.clone().to_string(),
            );
            iceberg_configs.insert(
                "iceberg.table.io.secret_access_key".to_string(),
                self.secret_key.clone().to_string(),
            );

            // iceberg-rust
            iceberg_configs.insert(
                ("iceberg.table.io.".to_string() + S3_ACCESS_KEY_ID).to_string(),
                self.access_key.clone().to_string(),
            );
            iceberg_configs.insert(
                ("iceberg.table.io.".to_string() + S3_SECRET_ACCESS_KEY).to_string(),
                self.secret_key.clone().to_string(),
            );

            let (bucket, _) = {
                let url = Url::parse(&self.warehouse_path)
                    .with_context(|| format!("Invalid warehouse path: {}", self.warehouse_path))?;
                let bucket = url
                    .host_str()
                    .with_context(|| {
                        format!(
                            "Invalid s3 path: {}, bucket is missing",
                            self.warehouse_path
                        )
                    })?
                    .to_string();
                let root = url.path().trim_start_matches('/').to_string();
                (bucket, root)
            };

            iceberg_configs.insert("iceberg.table.io.bucket".to_string(), bucket);
            // #TODO
            // Support load config file
            iceberg_configs.insert(
                "iceberg.table.io.disable_config_load".to_string(),
                "true".to_string(),
            );

            load_iceberg_base_catalog_config(&iceberg_configs)?
        };

        // Prepare jni configs, for details please see https://iceberg.apache.org/docs/latest/aws/
        let mut java_catalog_configs = HashMap::new();
        {
            if let Some(uri) = self.catalog_uri.as_deref() {
                java_catalog_configs.insert("uri".to_string(), uri.to_string());
            }

            java_catalog_configs.insert("warehouse".to_string(), self.warehouse_path.clone());
            java_catalog_configs.extend(java_catalog_props.clone());

            // Currently we only support s3, so let's set it to s3
            java_catalog_configs.insert(
                "io-impl".to_string(),
                "org.apache.iceberg.aws.s3.S3FileIO".to_string(),
            );

            if let Some(endpoint) = &self.endpoint {
                java_catalog_configs
                    .insert("s3.endpoint".to_string(), endpoint.clone().to_string());
            }

            java_catalog_configs.insert(
                "s3.access-key-id".to_string(),
                self.access_key.clone().to_string(),
            );
            java_catalog_configs.insert(
                "s3.secret-access-key".to_string(),
                self.secret_key.clone().to_string(),
            );

            if let Some(path_style_access) = path_style_access {
                java_catalog_configs.insert(
                    "s3.path-style-access".to_string(),
                    path_style_access.to_string(),
                );
            }
            if matches!(self.catalog_type.as_deref(), Some("glue")) {
                java_catalog_configs.insert(
                    "client.credentials-provider".to_string(),
                    "com.risingwave.connector.catalog.GlueCredentialProvider".to_string(),
                );
                // Use S3 ak/sk and region as glue ak/sk and region by default.
                // TODO: use different ak/sk and region for s3 and glue.
                java_catalog_configs.insert(
                    "client.credentials-provider.glue.access-key-id".to_string(),
                    self.access_key.clone().to_string(),
                );
                java_catalog_configs.insert(
                    "client.credentials-provider.glue.secret-access-key".to_string(),
                    self.secret_key.clone().to_string(),
                );
                if let Some(region) = &self.region {
                    java_catalog_configs
                        .insert("client.region".to_string(), region.clone().to_string());
                    java_catalog_configs.insert(
                        "glue.endpoint".to_string(),
                        format!("https://glue.{}.amazonaws.com", region),
                    );
                }
            }
        }

        Ok((base_catalog_config, java_catalog_configs))
    }
}

/// icelake
mod v1 {
    use icelake::catalog::{load_catalog, CatalogRef};
    use icelake::{Table, TableIdentifier};

    use super::*;

    impl IcebergCommon {
        pub fn full_table_name(&self) -> ConnectorResult<TableIdentifier> {
            let ret = if let Some(database_name) = &self.database_name {
                TableIdentifier::new(vec![database_name, &self.table_name])
            } else {
                TableIdentifier::new(vec![&self.table_name])
            };

            Ok(ret.context("Failed to create table identifier")?)
        }

        fn build_iceberg_configs(
            &self,
            path_style_access: &Option<bool>,
        ) -> ConnectorResult<HashMap<String, String>> {
            let mut iceberg_configs = HashMap::new();

            let catalog_type = self.catalog_type().to_string();

            iceberg_configs.insert(CATALOG_TYPE.to_string(), catalog_type.clone());
            iceberg_configs.insert(CATALOG_NAME.to_string(), self.catalog_name());

            match catalog_type.as_str() {
                "storage" => {
                    iceberg_configs.insert(
                        format!("iceberg.catalog.{}.warehouse", self.catalog_name()),
                        self.warehouse_path.clone(),
                    );
                }
                "rest" => {
                    let uri = self
                        .catalog_uri
                        .clone()
                        .with_context(|| "`catalog.uri` must be set in rest catalog".to_string())?;
                    iceberg_configs
                        .insert(format!("iceberg.catalog.{}.uri", self.catalog_name()), uri);
                }
                _ => {
                    bail!(
                        "Unsupported catalog type: {}, only support `storage` and `rest`",
                        catalog_type
                    );
                }
            }

            if let Some(region) = &self.region {
                iceberg_configs.insert(
                    "iceberg.table.io.region".to_string(),
                    region.clone().to_string(),
                );
            }

            if let Some(endpoint) = &self.endpoint {
                iceberg_configs.insert(
                    "iceberg.table.io.endpoint".to_string(),
                    endpoint.clone().to_string(),
                );
            }

            iceberg_configs.insert(
                "iceberg.table.io.access_key_id".to_string(),
                self.access_key.clone().to_string(),
            );
            iceberg_configs.insert(
                "iceberg.table.io.secret_access_key".to_string(),
                self.secret_key.clone().to_string(),
            );
            if let Some(path_style_access) = path_style_access {
                iceberg_configs.insert(
                    "iceberg.table.io.enable_virtual_host_style".to_string(),
                    (!path_style_access).to_string(),
                );
            }

            let (bucket, root) = {
                let url = Url::parse(&self.warehouse_path)
                    .with_context(|| format!("Invalid warehouse path: {}", self.warehouse_path))?;
                let bucket = url
                    .host_str()
                    .with_context(|| {
                        format!(
                            "Invalid s3 path: {}, bucket is missing",
                            self.warehouse_path
                        )
                    })?
                    .to_string();
                let root = url.path().trim_start_matches('/').to_string();
                (bucket, root)
            };

            iceberg_configs.insert("iceberg.table.io.bucket".to_string(), bucket);

            // Only storage catalog should set this.
            if catalog_type == "storage" {
                iceberg_configs.insert("iceberg.table.io.root".to_string(), root);
            }
            // #TODO
            // Support load config file
            iceberg_configs.insert(
                "iceberg.table.io.disable_config_load".to_string(),
                "true".to_string(),
            );

            Ok(iceberg_configs)
        }

        /// TODO: remove the arguments and put them into `IcebergCommon`. Currently the handling in source and sink are different, so pass them separately to be safer.
        pub async fn create_catalog(
            &self,
            path_style_access: &Option<bool>,
            java_catalog_props: &HashMap<String, String>,
        ) -> ConnectorResult<CatalogRef> {
            match self.catalog_type() {
                "storage" | "rest" => {
                    let iceberg_configs = self.build_iceberg_configs(path_style_access)?;
                    let catalog = load_catalog(&iceberg_configs).await?;
                    Ok(catalog)
                }
                catalog_type
                    if catalog_type == "hive"
                        || catalog_type == "jdbc"
                        || catalog_type == "glue" =>
                {
                    // Create java catalog
                    let (base_catalog_config, java_catalog_props) =
                        self.build_jni_catalog_configs(path_style_access, java_catalog_props)?;
                    let catalog_impl = match catalog_type {
                        "hive" => "org.apache.iceberg.hive.HiveCatalog",
                        "jdbc" => "org.apache.iceberg.jdbc.JdbcCatalog",
                        "glue" => "org.apache.iceberg.aws.glue.GlueCatalog",
                        _ => unreachable!(),
                    };

                    jni_catalog::JniCatalog::build_catalog(
                        base_catalog_config,
                        self.catalog_name(),
                        catalog_impl,
                        java_catalog_props,
                    )
                }
                "mock" => Ok(Arc::new(mock_catalog::MockCatalog {})),
                _ => {
                    bail!(
                    "Unsupported catalog type: {}, only support `storage`, `rest`, `hive`, `jdbc`, `glue`",
                    self.catalog_type()
                )
                }
            }
        }

        /// TODO: remove the arguments and put them into `IcebergCommon`. Currently the handling in source and sink are different, so pass them separately to be safer.
        pub async fn load_table(
            &self,
            path_style_access: &Option<bool>,
            java_catalog_props: &HashMap<String, String>,
        ) -> ConnectorResult<Table> {
            let catalog = self
                .create_catalog(path_style_access, java_catalog_props)
                .await
                .context("Unable to load iceberg catalog")?;

            let table_id = self
                .full_table_name()
                .context("Unable to parse table name")?;

            catalog.load_table(&table_id).await.map_err(Into::into)
        }
    }
}

/// iceberg-rust
mod v2 {
    use iceberg::spec::TableMetadata;
    use iceberg::table::Table as TableV2;
    use iceberg::{Catalog as CatalogV2, TableIdent};
    use iceberg_catalog_glue::{AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY};

    use super::*;

    impl IcebergCommon {
        pub fn full_table_name_v2(&self) -> ConnectorResult<TableIdent> {
            let ret = if let Some(database_name) = &self.database_name {
                TableIdent::from_strs(vec![database_name, &self.table_name])
            } else {
                TableIdent::from_strs(vec![&self.table_name])
            };

            Ok(ret.context("Failed to create table identifier")?)
        }

        /// TODO: remove the arguments and put them into `IcebergCommon`. Currently the handling in source and sink are different, so pass them separately to be safer.
        pub async fn create_catalog_v2(
            &self,
            path_style_access: &Option<bool>,
            java_catalog_props: &HashMap<String, String>,
        ) -> ConnectorResult<Arc<dyn CatalogV2>> {
            match self.catalog_type() {
                "storage" => {
                    let config = storage_catalog::StorageCatalogConfig::builder()
                        .warehouse(self.warehouse_path.clone())
                        .access_key(self.access_key.clone())
                        .secret_key(self.secret_key.clone())
                        .region(self.region.clone())
                        .endpoint(self.endpoint.clone())
                        .build();
                    let catalog = storage_catalog::StorageCatalog::new(config)?;
                    Ok(Arc::new(catalog))
                }
                "rest" => {
                    let mut iceberg_configs = HashMap::new();
                    if let Some(region) = &self.region {
                        iceberg_configs.insert(S3_REGION.to_string(), region.clone().to_string());
                    }
                    if let Some(endpoint) = &self.endpoint {
                        iceberg_configs
                            .insert(S3_ENDPOINT.to_string(), endpoint.clone().to_string());
                    }
                    iceberg_configs.insert(
                        S3_ACCESS_KEY_ID.to_string(),
                        self.access_key.clone().to_string(),
                    );
                    iceberg_configs.insert(
                        S3_SECRET_ACCESS_KEY.to_string(),
                        self.secret_key.clone().to_string(),
                    );
                    let config = iceberg_catalog_rest::RestCatalogConfig::builder()
                        .uri(self.catalog_uri.clone().with_context(|| {
                            "`catalog.uri` must be set in rest catalog".to_string()
                        })?)
                        .props(iceberg_configs)
                        .build();
                    let catalog = iceberg_catalog_rest::RestCatalog::new(config);
                    Ok(Arc::new(catalog))
                }
                "glue" => {
                    let mut iceberg_configs = HashMap::new();
                    // glue
                    if let Some(region) = &self.region {
                        iceberg_configs
                            .insert(AWS_REGION_NAME.to_string(), region.clone().to_string());
                    }
                    iceberg_configs.insert(
                        AWS_ACCESS_KEY_ID.to_string(),
                        self.access_key.clone().to_string(),
                    );
                    iceberg_configs.insert(
                        AWS_SECRET_ACCESS_KEY.to_string(),
                        self.secret_key.clone().to_string(),
                    );
                    // s3
                    if let Some(region) = &self.region {
                        iceberg_configs.insert(S3_REGION.to_string(), region.clone().to_string());
                    }
                    if let Some(endpoint) = &self.endpoint {
                        iceberg_configs
                            .insert(S3_ENDPOINT.to_string(), endpoint.clone().to_string());
                    }
                    iceberg_configs.insert(
                        S3_ACCESS_KEY_ID.to_string(),
                        self.access_key.clone().to_string(),
                    );
                    iceberg_configs.insert(
                        S3_SECRET_ACCESS_KEY.to_string(),
                        self.secret_key.clone().to_string(),
                    );
                    let config_builder = iceberg_catalog_glue::GlueCatalogConfig::builder()
                        .warehouse(self.warehouse_path.clone())
                        .props(iceberg_configs);
                    let config = if let Some(uri) = self.catalog_uri.as_deref() {
                        config_builder.uri(uri.to_string()).build()
                    } else {
                        config_builder.build()
                    };
                    let catalog = iceberg_catalog_glue::GlueCatalog::new(config).await?;
                    Ok(Arc::new(catalog))
                }
                catalog_type if catalog_type == "hive" || catalog_type == "jdbc" => {
                    // Create java catalog
                    let (base_catalog_config, java_catalog_props) =
                        self.build_jni_catalog_configs(path_style_access, java_catalog_props)?;
                    let catalog_impl = match catalog_type {
                        "hive" => "org.apache.iceberg.hive.HiveCatalog",
                        "jdbc" => "org.apache.iceberg.jdbc.JdbcCatalog",
                        _ => unreachable!(),
                    };

                    jni_catalog::JniCatalog::build_catalog_v2(
                        base_catalog_config,
                        self.catalog_name(),
                        catalog_impl,
                        java_catalog_props,
                    )
                }
                "mock" => Ok(Arc::new(mock_catalog::MockCatalog {})),
                _ => {
                    bail!(
                    "Unsupported catalog type: {}, only support `storage`, `rest`, `hive`, `jdbc`, `glue`",
                    self.catalog_type()
                )
                }
            }
        }

        /// TODO: remove the arguments and put them into `IcebergCommon`. Currently the handling in source and sink are different, so pass them separately to be safer.
        pub async fn load_table_v2(
            &self,
            path_style_access: &Option<bool>,
            java_catalog_props: &HashMap<String, String>,
        ) -> ConnectorResult<TableV2> {
            let catalog = self
                .create_catalog_v2(path_style_access, java_catalog_props)
                .await
                .context("Unable to load iceberg catalog")?;

            let table_id = self
                .full_table_name_v2()
                .context("Unable to parse table name")?;

            catalog.load_table(&table_id).await.map_err(Into::into)
        }

        pub async fn load_table_v2_with_metadata(
            &self,
            metadata: TableMetadata,
            path_style_access: &Option<bool>,
            java_catalog_props: &HashMap<String, String>,
        ) -> ConnectorResult<TableV2> {
            match self.catalog_type() {
                "storage" => {
                    let config = storage_catalog::StorageCatalogConfig::builder()
                        .warehouse(self.warehouse_path.clone())
                        .access_key(self.access_key.clone())
                        .secret_key(self.secret_key.clone())
                        .region(self.region.clone())
                        .endpoint(self.endpoint.clone())
                        .build();
                    let storage_catalog = storage_catalog::StorageCatalog::new(config)?;

                    let table_id = self
                        .full_table_name_v2()
                        .context("Unable to parse table name")?;

                    Ok(iceberg::table::Table::builder()
                        .metadata(metadata)
                        .identifier(table_id)
                        .file_io(storage_catalog.file_io().clone())
                        // Only support readonly table for storage catalog now.
                        .readonly(true)
                        .build()?)
                }
                _ => {
                    self.load_table_v2(path_style_access, java_catalog_props)
                        .await
                }
            }
        }
    }
}
