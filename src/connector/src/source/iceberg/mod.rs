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

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
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
    pub catalog_type: String,
    #[serde(rename = "s3.region")]
    pub region_name: String,
    #[serde(rename = "s3.endpoint", default)]
    pub endpoint: String,
    #[serde(rename = "s3.access.key", default)]
    pub s3_access: String,
    #[serde(rename = "s3.secret.key", default)]
    pub s3_secret: String,
    #[serde(rename = "warehouse.path")]
    pub warehouse_path: String,
    #[serde(rename = "database.name")]
    pub database_name: String,
    #[serde(rename = "table.name")]
    pub table_name: String,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl IcebergProperties {
    pub fn to_iceberg_config(&self) -> IcebergConfig {
        IcebergConfig {
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
            catalog_type: Some(self.catalog_type.clone()),
            path: self.warehouse_path.clone(),
            endpoint: Some(self.endpoint.clone()),
            access_key: self.s3_access.clone(),
            secret_key: self.s3_secret.clone(),
            region: Some(self.region_name.clone()),
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
pub struct IcebergSplit {
    pub split_id: i64,
    pub snapshot_id: i64,
    pub files: Vec<String>,
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

    fn update_with_offset(&mut self, _start_offset: String) -> ConnectorResult<()> {
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

impl IcebergSplitEnumerator {
    pub async fn list_splits_batch(
        &self,
        batch_parallelism: usize,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        let table = self.config.load_table().await?;
        let snapshot_id = table.current_table_metadata().current_snapshot_id.unwrap();
        let files = table
            .current_data_files()
            .await?
            .into_iter()
            .map(|f| f.file_path)
            .collect_vec();
        if batch_parallelism == 0 {
            bail!("Batch parallelism is 0. Cannot split the iceberg files.")
        }
        let split_num = batch_parallelism;
        // evenly split the files into splits based on the parallelism.
        let split_size = files.len() / split_num;
        let remaining = files.len() % split_num;
        let mut splits = vec![];
        for i in 0..split_num {
            let start = i * split_size;
            let end = (i + 1) * split_size;
            let split = IcebergSplit {
                split_id: i as i64,
                snapshot_id,
                files: files[start..end].to_vec(),
            };
            splits.push(split);
        }
        for i in 0..remaining {
            splits[i]
                .files
                .push(files[split_num * split_size + i].clone());
        }
        Ok(splits
            .into_iter()
            .filter(|split| !split.files.is_empty())
            .collect_vec())
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
