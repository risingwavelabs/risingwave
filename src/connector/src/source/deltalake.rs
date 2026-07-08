// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltalake::DeltaTable;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::{Azblob, Azdls};
use phf::{Set, phf_set};
use risingwave_common::types::Timestamptz;
use serde::Deserialize;
use serde_with::serde_as;
use url::Url;
use with_options::WithOptions;

use crate::deserialize_optional_bool_from_string;
use crate::enforce_secret::{EnforceSecret, EnforceSecretError};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::filesystem::opendal_source::opendal_enumerator::OpendalEnumerator;
use crate::source::filesystem::opendal_source::{
    DEFAULT_REFRESH_INTERVAL_SEC, FsSourceCommon, OpendalSource,
};
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::iceberg::read_parquet_file;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitMetaData, SplitReader, UnknownFields,
};

pub const DELTALAKE_CONNECTOR: &str = "deltalake";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalDeltaLake;

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct DeltaLakeProperties {
    #[serde(rename = "location")]
    pub location: String,

    #[serde(rename = "azblob.container_name", default)]
    pub azblob_container_name: Option<String>,
    #[serde(rename = "azblob.credentials.account_name", default)]
    pub azblob_account_name: Option<String>,
    #[serde(rename = "azblob.credentials.account_key", default)]
    pub azblob_account_key: Option<String>,
    #[serde(rename = "azblob.credentials.sas_token", default)]
    pub azblob_sas_token: Option<String>,
    #[serde(rename = "azblob.endpoint_url", default)]
    pub azblob_endpoint_url: Option<String>,

    #[serde(rename = "azure.client.id", default)]
    pub azure_client_id: Option<String>,
    #[serde(rename = "azure.client.secret", default)]
    pub azure_client_secret: Option<String>,
    #[serde(rename = "azure.tenant.id", default)]
    pub azure_tenant_id: Option<String>,
    #[serde(
        rename = "azure.use.azure.cli",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub azure_use_azure_cli: Option<bool>,

    #[serde(flatten)]
    pub fs_common: FsSourceCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for DeltaLakeProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "azblob.credentials.account_key",
        "azblob.credentials.sas_token",
        "azure.client.secret",
    };

    fn enforce_one(prop: &str) -> ConnectorResult<()> {
        if Self::ENFORCE_SECRET_PROPERTIES.contains(prop) {
            return Err(EnforceSecretError {
                key: prop.to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

impl UnknownFields for DeltaLakeProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for DeltaLakeProperties {
    type Split = OpendalFsSplit<OpendalDeltaLake>;
    type SplitEnumerator = DeltaLakeSplitEnumerator;
    type SplitReader = DeltaLakeSplitReader;

    const SOURCE_NAME: &'static str = DELTALAKE_CONNECTOR;
}

#[derive(Debug, Clone)]
pub struct DeltaLakeSplitReader {
    connector: OpendalEnumerator<OpendalDeltaLake>,
    splits: Vec<OpendalFsSplit<OpendalDeltaLake>>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
    case_insensitive: bool,
}

#[async_trait]
impl SplitReader for DeltaLakeSplitReader {
    type Properties = DeltaLakeProperties;
    type Split = OpendalFsSplit<OpendalDeltaLake>;

    async fn new(
        properties: DeltaLakeProperties,
        splits: Vec<OpendalFsSplit<OpendalDeltaLake>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let case_insensitive = properties
            .fs_common
            .parquet_case_insensitive
            .unwrap_or(false);
        let connector = OpendalDeltaLake::new_enumerator(properties)?;
        Ok(Self {
            connector,
            splits,
            parser_config,
            source_ctx,
            columns,
            case_insensitive,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_stream_inner()
    }
}

impl DeltaLakeSplitReader {
    #[try_stream(boxed, ok = risingwave_common::array::StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in self.splits {
            let source_ctx = self.source_ctx.clone();
            let object_name = split.name.clone();
            let actor_id = source_ctx.actor_id.to_string();
            let fragment_id = source_ctx.fragment_id.to_string();
            let source_id = source_ctx.source_id.to_string();
            let source_name = source_ctx.source_name.clone();
            let file_source_input_row_count = self
                .source_ctx
                .metrics
                .file_source_input_row_count
                .with_guarded_label_values(&[&source_id, &source_name, &actor_id, &fragment_id]);
            let split_id = split.id();
            let parquet_source_skip_row_count_metrics = self
                .source_ctx
                .metrics
                .parquet_source_skip_row_count
                .with_guarded_label_values(&[
                    actor_id.as_str(),
                    source_id.as_str(),
                    &split_id,
                    source_name.as_str(),
                ]);
            let chunk_stream = read_parquet_file(
                self.connector.op.clone(),
                object_name,
                self.columns.clone(),
                Some(self.parser_config.common.rw_columns.clone()),
                self.case_insensitive,
                self.source_ctx.source_ctrl_opts.chunk_size,
                split.offset,
                Some(file_source_input_row_count),
                Some(parquet_source_skip_row_count_metrics),
            )
            .await?;

            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
            }
        }
    }
}

impl OpendalSource for OpendalDeltaLake {
    type Properties = DeltaLakeProperties;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
        let location = DeltaLakeTableLocation::parse(&properties)?;
        let endpoint = properties
            .azblob_endpoint_url
            .clone()
            .or_else(|| location.endpoint_url.clone());
        let account_name = properties
            .azblob_account_name
            .clone()
            .or_else(|| location.account_name.clone());

        let op = match location.storage_service {
            AzureStorageService::Blob => {
                let mut builder = Azblob::default()
                    .container(&location.container_name)
                    .root(&location.table_root);
                if let Some(endpoint) = endpoint.clone() {
                    builder = builder.endpoint(&endpoint);
                }
                if let Some(account_name) = account_name.clone() {
                    builder = builder.account_name(&account_name);
                }
                if let Some(account_key) = properties.azblob_account_key.clone() {
                    builder = builder.account_key(&account_key);
                }
                if let Some(sas_token) = properties.azblob_sas_token.clone() {
                    builder = builder.sas_token(&sas_token);
                }
                Operator::new(builder)?
                    .layer(LoggingLayer::default())
                    .layer(RetryLayer::default())
                    .finish()
            }
            AzureStorageService::DataLake => {
                let mut builder = Azdls::default()
                    .filesystem(&location.container_name)
                    .root(&location.table_root);
                if let Some(endpoint) = endpoint {
                    builder = builder.endpoint(&endpoint);
                }
                if let Some(account_name) = account_name {
                    builder = builder.account_name(&account_name);
                }
                if let Some(account_key) = properties.azblob_account_key {
                    builder = builder.account_key(&account_key);
                }
                if let Some(sas_token) = properties.azblob_sas_token {
                    builder = builder.sas_token(&sas_token);
                }
                if let Some(client_id) = properties.azure_client_id {
                    builder = builder.client_id(&client_id);
                }
                if let Some(client_secret) = properties.azure_client_secret {
                    builder = builder.client_secret(&client_secret);
                }
                if let Some(tenant_id) = properties.azure_tenant_id {
                    builder = builder.tenant_id(&tenant_id);
                }
                Operator::new(builder)?
                    .layer(LoggingLayer::default())
                    .layer(RetryLayer::default())
                    .finish()
            }
        };

        Ok(OpendalEnumerator {
            op,
            prefix: None,
            matcher: None,
            marker: PhantomData,
            compression_format: properties.fs_common.compression_format,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DeltaLakeSplitEnumerator {
    properties: DeltaLakeProperties,
}

#[async_trait]
impl SplitEnumerator for DeltaLakeSplitEnumerator {
    type Properties = DeltaLakeProperties;
    type Split = OpendalFsSplit<OpendalDeltaLake>;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self { properties })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        // Like streaming file source, Delta Lake source lists files in a source executor.
        // Validate that the table can be opened here.
        self.properties.create_deltalake_client().await?;
        Ok(vec![OpendalFsSplit::empty_split()])
    }
}

impl DeltaLakeProperties {
    pub async fn create_deltalake_client(&self) -> ConnectorResult<DeltaTable> {
        let storage_options = self.storage_options()?;
        deltalake::azure::register_handlers(None);
        let url = Url::parse(&self.location)
            .with_context(|| format!("invalid Delta Lake table location `{}`", self.location))?;
        deltalake::open_table_with_storage_options(url, storage_options)
            .await
            .map_err(|err| ConnectorError::from(anyhow!(err)))
    }

    pub async fn load_table_arrow_schema(
        &self,
    ) -> ConnectorResult<deltalake::arrow::datatypes::Schema> {
        let mut table = self.create_deltalake_client().await?;
        table
            .update_state()
            .await
            .map_err(|err| ConnectorError::from(anyhow!(err)))?;
        Ok(table
            .snapshot()
            .map_err(|err| ConnectorError::from(anyhow!(err)))?
            .schema()
            .as_ref()
            .try_into_arrow()
            .map_err(|err| ConnectorError::from(anyhow!(err)))?)
    }

    fn storage_options(&self) -> ConnectorResult<HashMap<String, String>> {
        let location = DeltaLakeTableLocation::parse(self)?;
        let mut options = self.unknown_fields.clone();

        if let Some(container_name) = self
            .azblob_container_name
            .clone()
            .or_else(|| Some(location.container_name.clone()))
        {
            options.insert("azure_container_name".to_owned(), container_name);
        }
        if let Some(account_name) = self
            .azblob_account_name
            .clone()
            .or_else(|| location.account_name.clone())
        {
            options.insert("azure_storage_account_name".to_owned(), account_name);
        }
        if let Some(account_key) = self.azblob_account_key.clone() {
            options.insert("azure_storage_account_key".to_owned(), account_key);
        }
        if let Some(sas_token) = self.azblob_sas_token.clone() {
            options.insert("azure_storage_sas_token".to_owned(), sas_token);
        }
        if let Some(endpoint) = self
            .azblob_endpoint_url
            .clone()
            .or_else(|| location.endpoint_url.clone())
        {
            options.insert("azure_storage_endpoint".to_owned(), endpoint);
        }
        if let Some(client_id) = self.azure_client_id.clone() {
            options.insert("azure_storage_client_id".to_owned(), client_id);
        }
        if let Some(client_secret) = self.azure_client_secret.clone() {
            options.insert("azure_storage_client_secret".to_owned(), client_secret);
        }
        if let Some(tenant_id) = self.azure_tenant_id.clone() {
            options.insert("azure_storage_tenant_id".to_owned(), tenant_id);
        }
        if let Some(use_azure_cli) = self.azure_use_azure_cli {
            options.insert("azure_use_azure_cli".to_owned(), use_azure_cli.to_string());
        }

        Ok(options)
    }
}

#[derive(Debug)]
struct DeltaLakeTableLocation {
    container_name: String,
    account_name: Option<String>,
    endpoint_url: Option<String>,
    table_root: String,
    storage_service: AzureStorageService,
}

#[derive(Debug)]
enum AzureStorageService {
    Blob,
    DataLake,
}

impl DeltaLakeTableLocation {
    fn parse(properties: &DeltaLakeProperties) -> ConnectorResult<Self> {
        let url = Url::parse(&properties.location).with_context(|| {
            format!(
                "invalid Delta Lake table location `{}`",
                properties.location
            )
        })?;
        match url.scheme() {
            "az" | "azure" | "adl" | "abfs" | "abfss" => {
                let host = url
                    .host_str()
                    .ok_or_else(|| anyhow!("Delta Lake Azure location must include a host"))?;
                let username = url.username();
                let storage_service = if matches!(url.scheme(), "adl" | "abfs" | "abfss")
                    || host.ends_with(".dfs.core.windows.net")
                {
                    AzureStorageService::DataLake
                } else {
                    AzureStorageService::Blob
                };

                let (container_name, account_name, endpoint_url) = if !username.is_empty() {
                    let account_name = host.split('.').next().map(str::to_owned);
                    let endpoint_scheme = if url.scheme() == "abfs" {
                        "http"
                    } else {
                        "https"
                    };
                    (
                        username.to_owned(),
                        account_name,
                        Some(format!("{endpoint_scheme}://{host}")),
                    )
                } else {
                    (
                        host.to_owned(),
                        properties.azblob_account_name.clone(),
                        properties.azblob_endpoint_url.clone(),
                    )
                };

                Ok(Self {
                    container_name: properties
                        .azblob_container_name
                        .clone()
                        .unwrap_or(container_name),
                    account_name,
                    endpoint_url,
                    table_root: url.path().trim_start_matches('/').to_owned(),
                    storage_service,
                })
            }
            other => Err(ConnectorError::from(anyhow!(
                "Delta Lake source only supports Azure locations with az, azure, adl, abfs, or abfss scheme, got `{}`",
                other
            ))),
        }
    }
}

#[try_stream(boxed, ok = FsPageItem, error = crate::error::ConnectorError)]
pub async fn build_delta_lake_list_stream(properties: DeltaLakeProperties, list_interval_sec: u64) {
    let mut seen_files = HashSet::new();
    loop {
        let mut table = properties.create_deltalake_client().await?;
        table
            .update_state()
            .await
            .map_err(|err| ConnectorError::from(anyhow!(err)))?;
        let mut files = table.get_active_add_actions_by_partitions(&[]);
        while let Some(file) = files
            .try_next()
            .await
            .map_err(|err| ConnectorError::from(anyhow!(err)))?
        {
            let file_path = file.path().to_string();
            if !seen_files.insert(file_path.clone()) {
                continue;
            }
            let timestamp = Timestamptz::from(
                DateTime::<Utc>::from_timestamp_millis(file.modification_time())
                    .ok_or_else(|| anyhow!("invalid Delta Lake file modification time"))?,
            );
            yield FsPageItem {
                name: file_path,
                size: file.size(),
                timestamp,
            };
        }
        tokio::time::sleep(std::time::Duration::from_secs(list_interval_sec)).await;
    }
}

pub fn refresh_interval_sec(properties: &DeltaLakeProperties) -> u64 {
    properties
        .fs_common
        .refresh_interval_sec
        .unwrap_or(DEFAULT_REFRESH_INTERVAL_SEC)
}
