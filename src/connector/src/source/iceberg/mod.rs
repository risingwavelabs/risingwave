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
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use simd_json::prelude::ArrayTrait;

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
    pub access: String,
    #[serde(rename = "s3.secret.key", default)]
    pub secret: String,
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
            access_key: self.access.clone(),
            secret_key: self.secret.clone(),
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

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, _start_offset: String) -> anyhow::Result<()> {
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
        context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<Self> {
        let iceberg_config = properties.to_iceberg_config();
        // check if the source schema matches the iceberg table schema
        match &context.info.source {
            Some(source) => {
                let columns: Vec<ColumnCatalog> = source
                    .columns
                    .iter()
                    .cloned()
                    .map(ColumnCatalog::from)
                    .collect_vec();

                let schema = Schema {
                    fields: columns
                        .iter()
                        .map(|c| Field::from(&c.column_desc))
                        .collect(),
                };

                let table = iceberg_config.load_table().await?;

                let iceberg_schema: arrow_schema::Schema = table
                    .current_table_metadata()
                    .current_schema()?
                    .clone()
                    .try_into()?;

                for f1 in schema.fields() {
                    if !iceberg_schema.fields.iter().any(|f2| f2.name() == &f1.name) {
                        return Err(anyhow::anyhow!(format!(
                            "Column {} not found in iceberg table",
                            f1.name
                        )));
                    }
                }

                let new_iceberg_field = iceberg_schema
                    .fields
                    .iter()
                    .filter(|f1| schema.fields.iter().any(|f2| f1.name() == &f2.name))
                    .cloned()
                    .collect::<Vec<_>>();
                let new_iceberg_schema = arrow_schema::Schema::new(new_iceberg_field);

                crate::sink::iceberg::try_matches_arrow_schema(&schema, &new_iceberg_schema)?;
            }
            None => {}
        }
        Ok(Self {
            config: iceberg_config,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<Self::Split>> {
        // Iceberg source does not support streaming queries
        Ok(vec![])
    }
}

impl IcebergSplitEnumerator {
    pub async fn list_splits_batch(&self) -> anyhow::Result<Vec<IcebergSplit>> {
        let table = self.config.load_table().await?;
        let snapshot_id = table.current_table_metadata().current_snapshot_id.unwrap();
        let files = table
            .current_data_files()
            .await?
            .into_iter()
            .map(|f| f.file_path)
            .collect_vec();
        let split_num = 12;
        // evenly split the files into 12 splits
        let split_size = files.len() / split_num;
        let mut splits = vec![];
        for i in 0..split_num {
            let start = i * split_size;
            let end = if i == split_num - 1 {
                files.len()
            } else {
                (i + 1) * split_size
            };
            let split = IcebergSplit {
                split_id: i as i64,
                snapshot_id,
                files: files[start..end].to_vec(),
            };
            splits.push(split);
        }
        Ok(splits)
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
