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
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<Self> {
        match &context.info.source {
            Some(source) => {
                let iceberg_config = IcebergConfig {
                    database_name: properties.database_name,
                    table_name: properties.table_name,
                    catalog_type: Some(properties.catalog_type),
                    path: properties.warehouse_path,
                    endpoint: Some(properties.endpoint),
                    access_key: properties.s3_access,
                    secret_key: properties.s3_secret,
                    region: Some(properties.region_name),
                    ..Default::default()
                };

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
                Ok(Self {})
            }
            None => unreachable!(),
        }
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
