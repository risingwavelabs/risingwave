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

pub mod parquet_file_reader;

use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use futures_async_stream::for_await;
use iceberg::scan::FileScanTask;
use iceberg::spec::{ManifestList, TableMetadata};
use iceberg::table::Table;
use itertools::Itertools;
pub use parquet_file_reader::*;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::sink::iceberg::IcebergConfig;
use crate::source::{
    BoxChunkSourceStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
};

pub const ICEBERG_CONNECTOR: &str = "iceberg";

#[derive(Clone, Debug, Deserialize, PartialEq, with_options::WithOptions)]
pub struct IcebergProperties {
    #[serde(rename = "catalog.type")]
    pub catalog_type: Option<String>,
    #[serde(rename = "s3.region")]
    pub region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "s3.access.key")]
    pub s3_access: String,
    #[serde(rename = "s3.secret.key")]
    pub s3_secret: String,
    #[serde(rename = "warehouse.path")]
    pub warehouse_path: String,
    // Catalog name, can be omitted for storage catalog, but
    // must be set for other catalogs.
    #[serde(rename = "catalog.name")]
    pub catalog_name: Option<String>,
    #[serde(rename = "catalog.uri")]
    pub catalog_uri: Option<String>, // URI of iceberg catalog, only applicable in rest catalog.
    #[serde(rename = "database.name")]
    pub database_name: Option<String>,
    #[serde(rename = "table.name")]
    pub table_name: String,
    // For jdbc catalog
    #[serde(rename = "catalog.jdbc.user")]
    pub jdbc_user: Option<String>,
    #[serde(rename = "catalog.jdbc.password")]
    pub jdbc_password: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl IcebergProperties {
    pub fn to_iceberg_config(&self) -> IcebergConfig {
        let mut java_catalog_props = HashMap::new();
        if let Some(jdbc_user) = self.jdbc_user.clone() {
            java_catalog_props.insert("jdbc.user".to_string(), jdbc_user);
        }
        if let Some(jdbc_password) = self.jdbc_password.clone() {
            java_catalog_props.insert("jdbc.password".to_string(), jdbc_password);
        }
        IcebergConfig {
            catalog_name: self.catalog_name.clone(),
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
            catalog_type: self.catalog_type.clone(),
            uri: self.catalog_uri.clone(),
            path: self.warehouse_path.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.s3_access.clone(),
            secret_key: self.s3_secret.clone(),
            region: self.region.clone(),
            java_catalog_props,
            ..Default::default()
        }
    }
}

impl SourceProperties for IcebergProperties {
    type Split = IcebergSplit;
    type SplitEnumerator = IcebergSplitEnumerator;
    type SplitReader = IcebergFileReader;

    const SOURCE_NAME: &'static str = ICEBERG_CONNECTOR;
}

impl UnknownFields for IcebergProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IcebergFileScanTaskJsonStr(String);

impl IcebergFileScanTaskJsonStr {
    pub fn deserialize(&self) -> FileScanTask {
        serde_json::from_str(&self.0).unwrap()
    }

    pub fn serialize(task: &FileScanTask) -> Self {
        Self(serde_json::to_string(task).unwrap())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TableMetadataJsonStr(String);

impl TableMetadataJsonStr {
    pub fn deserialize(&self) -> TableMetadata {
        serde_json::from_str(&self.0).unwrap()
    }

    pub fn serialize(metadata: &TableMetadata) -> Self {
        Self(serde_json::to_string(metadata).unwrap())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IcebergSplit {
    pub split_id: i64,
    pub snapshot_id: i64,
    pub table_meta: TableMetadataJsonStr,
    pub files: Vec<IcebergFileScanTaskJsonStr>,
    pub record_counts: Vec<u64>,
    pub eq_delete_files: Vec<IcebergFileScanTaskJsonStr>,
}

impl SplitMetaData for IcebergSplit {
    fn id(&self) -> SplitId {
        self.split_id.to_string().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSplitEnumerator {
    config: IcebergConfig,
}

#[async_trait]
impl SplitEnumerator for IcebergSplitEnumerator {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let iceberg_config = properties.to_iceberg_config();
        Ok(Self {
            config: iceberg_config,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        // Iceberg source does not support streaming queries
        Ok(vec![])
    }
}

pub enum IcebergTimeTravelInfo {
    Version(i64),
    TimestampMs(i64),
}

impl IcebergSplitEnumerator {
    pub async fn list_splits_batch_count_star(
        &self,
        time_traval_info: Option<IcebergTimeTravelInfo>,
        batch_parallelism: usize,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        if batch_parallelism == 0 {
            bail!("Batch parallelism is 0. Cannot split the iceberg files.");
        }
        let table = self.config.load_table_v2().await?;
        let table_meta = TableMetadataJsonStr::serialize(table.metadata());
        let Some(snapshot_id) = self.get_snapshot_id(&table, time_traval_info)? else{
            // If there is no snapshot, we will return a mock `IcebergSplit` with empty files.
            return Ok(vec![IcebergSplit {split_id:0,snapshot_id:0,table_meta,files:vec![],eq_delete_files:vec![], record_counts: vec![] }]);
        };
        let mut record_counts = vec![];
        let manifest_list: ManifestList = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| anyhow!(e))?;

        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(table.file_io())
                .await
                .map_err(|e| anyhow!(e))?;
            let mut manifest_entries_stream =
                futures::stream::iter(manifest.entries().iter().filter(|e| e.is_alive()));

            while let Some(manifest_entry) = manifest_entries_stream.next().await {
                let file = manifest_entry.data_file();
                record_counts.push(file.record_count());
            }
        }
        let split_num = batch_parallelism;
        // evenly split the files into splits based on the parallelism.
        let split_size = record_counts.len() / split_num;
        let remaining = record_counts.len() % split_num;
        let mut splits = vec![];
        for i in 0..split_num {
            let start = i * split_size;
            let end = (i + 1) * split_size;
            let split = IcebergSplit {
                split_id: i as i64,
                snapshot_id,
                table_meta: table_meta.clone(),
                files: vec![],
                record_counts: record_counts[start..end].to_vec(),
                eq_delete_files: vec![],
            };
            splits.push(split);
        }
        for i in 0..remaining {
            splits[i]
                .record_counts
                .push(record_counts[split_num * split_size + i]);
        }

        Ok(splits
            .into_iter()
            .filter(|split| !split.record_counts.is_empty())
            .collect_vec())
    }

    pub async fn list_splits_batch_scan(
        &self,
        schema: Schema,
        time_traval_info: Option<IcebergTimeTravelInfo>,
        batch_parallelism: usize,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        if batch_parallelism == 0 {
            bail!("Batch parallelism is 0. Cannot split the iceberg files.");
        }
        let table = self.config.load_table_v2().await?;

        let table_meta = TableMetadataJsonStr::serialize(table.metadata());

        let Some(snapshot_id) = self.get_snapshot_id(&table, time_traval_info)? else{
            // If there is no snapshot, we will return a mock `IcebergSplit` with empty files.
            return Ok(vec![IcebergSplit {split_id:0,snapshot_id:0,table_meta,files:vec![],eq_delete_files:vec![], record_counts: vec![] }]);
        };
        
        let require_names = Self::get_require_field_names(&table, snapshot_id, schema).await?;

        let mut data_files = vec![];
        let mut eq_delete_files = vec![];

        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .select(require_names)
            .build()
            .map_err(|e| anyhow!(e))?;

        let file_scan_stream = scan.plan_files().await.map_err(|e| anyhow!(e))?;

        #[for_await]
        for task in file_scan_stream {
            let mut task: FileScanTask = task.map_err(|e| anyhow!(e))?;
            match task.data_file_content {
                iceberg::spec::DataContentType::Data => {
                    data_files.push(IcebergFileScanTaskJsonStr::serialize(&task));
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    task.project_field_ids = task.equality_ids.clone();
                    eq_delete_files.push(IcebergFileScanTaskJsonStr::serialize(&task));
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    bail!("Position delete file is not supported")
                }
            }
        }

        let split_num = batch_parallelism;
        // evenly split the files into splits based on the parallelism.
        let split_size = data_files.len() / split_num;
        let remaining = data_files.len() % split_num;
        let mut splits = vec![];
        for i in 0..split_num {
            let start = i * split_size;
            let end = (i + 1) * split_size;
            let split = IcebergSplit {
                split_id: i as i64,
                snapshot_id,
                table_meta: table_meta.clone(),
                record_counts: vec![],
                files: data_files[start..end].to_vec(),
                eq_delete_files: eq_delete_files.clone(),
            };
            splits.push(split);
        }
        for i in 0..remaining {
            splits[i]
                .files
                .push(data_files[split_num * split_size + i].clone());
        }
        Ok(splits
            .into_iter()
            .filter(|split| !split.files.is_empty())
            .collect_vec())
    }

    async fn get_require_field_names(
        table: &Table,
        snapshot_id: i64,
        rw_schema: Schema,
    ) -> ConnectorResult<Vec<String>> {
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .build()
            .map_err(|e| anyhow!(e))?;
        let file_scan_stream = scan.plan_files().await.map_err(|e| anyhow!(e))?;
        let schema = scan.snapshot().schema(table.metadata())?;
        let mut equality_ids = vec![];
        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task.map_err(|e| anyhow!(e))?;
            if task.data_file_content == iceberg::spec::DataContentType::EqualityDeletes {
                if equality_ids.is_empty() {
                    equality_ids = task.equality_ids;
                } else if equality_ids != task.equality_ids {
                    bail!("The schema of iceberg equality delete file must be consistent");
                }
            }
        }
        let delete_columns = equality_ids
            .into_iter()
            .map(|id| match schema.name_by_field_id(id) {
                Some(name) => Ok::<std::string::String, ConnectorError>(name.to_string()),
                None => bail!("Delete field id {} not found in schema", id),
            })
            .collect::<ConnectorResult<Vec<_>>>()?;
        let mut require_field_names: Vec<_> = rw_schema.names().to_vec();
        // Add the delete columns to the required field names
        for names in delete_columns {
            if !require_field_names.contains(&names) {
                require_field_names.push(names);
            }
        }
        Ok(require_field_names)
    }

    fn get_snapshot_id(&self, table: &Table, time_traval_info: Option<IcebergTimeTravelInfo>) -> ConnectorResult<Option<i64>> {
        let current_snapshot = table.metadata().current_snapshot();
        if current_snapshot.is_none() {
            // If there is no snapshot, we will return a mock `IcebergSplit` with empty files.
            return Ok(None);
        }
        match time_traval_info {
            Some(IcebergTimeTravelInfo::Version(version)) => {
                let Some(snapshot) = table.metadata().snapshot_by_id(version) else {
                    bail!("Cannot find the snapshot id in the iceberg table.");
                };
                Ok(Some(snapshot.snapshot_id()))
            }
            Some(IcebergTimeTravelInfo::TimestampMs(timestamp)) => {
                let snapshot = table
                    .metadata()
                    .snapshots()
                    .map(|snapshot| snapshot.timestamp().map(|ts| ts.timestamp_millis()))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|&snapshot_millis| snapshot_millis <= timestamp)
                    .max_by_key(|&snapshot_millis| snapshot_millis);
                match snapshot {
                    Some(snapshot) => Ok(Some(snapshot)),
                    None => {
                        // convert unix time to human readable time
                        let time = chrono::DateTime::from_timestamp_millis(timestamp);
                        if time.is_some() {
                            bail!("Cannot find a snapshot older than {}", time.unwrap());
                        } else {
                            bail!("Cannot find a snapshot");
                        }
                    }
                }
            }
            None => {
                assert!(current_snapshot.is_some());
                Ok(Some(current_snapshot.unwrap().snapshot_id()))
            }
        }
    }
}

#[derive(Debug)]
pub struct IcebergFileReader {}

#[async_trait]
impl SplitReader for IcebergFileReader {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        _props: IcebergProperties,
        _splits: Vec<IcebergSplit>,
        _parser_config: ParserConfig,
        _source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        unimplemented!()
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        unimplemented!()
    }
}

