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
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::parser::ParserConfig;
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
pub struct IcebergSplit {}

impl SplitMetaData for IcebergSplit {
    fn id(&self) -> SplitId {
        unimplemented!()
    }

    fn restore_from_json(_value: JsonbVal) -> anyhow::Result<Self> {
        unimplemented!()
    }

    fn encode_to_json(&self) -> JsonbVal {
        unimplemented!()
    }

    fn update_with_offset(&mut self, _start_offset: String) -> anyhow::Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for IcebergSplitEnumerator {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        _properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<Self> {
        Ok(Self {})
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<Self::Split>> {
        Ok(vec![])
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
    ) -> anyhow::Result<Self> {
        unimplemented!()
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        unimplemented!()
    }
}
