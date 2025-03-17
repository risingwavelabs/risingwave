// Copyright 2025 RisingWave Labs
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

#![deprecated = "will be replaced by new fs source (list + fetch)"]

use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use futures::stream::pending;
use risingwave_common::catalog::ColumnId;

use crate::WithOptionsSecResolved;
use crate::error::ConnectorResult;
use crate::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use crate::source::{
    BoxSourceChunkStream, ConnectorProperties, ConnectorState, SourceColumnDesc, SourceContext,
};

#[derive(Clone, Debug)]
pub struct LegacyFsSourceReader {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub properties: WithOptionsSecResolved,
    pub parser_config: SpecificParserConfig,
}

impl LegacyFsSourceReader {
    pub fn new(
        properties: WithOptionsSecResolved,
        columns: Vec<SourceColumnDesc>,
        parser_config: SpecificParserConfig,
    ) -> ConnectorResult<Self> {
        // Store the connector node address to properties for later use.
        let config = ConnectorProperties::extract(properties.clone(), false)?;

        Ok(Self {
            config,
            columns,
            properties,
            parser_config,
        })
    }

    fn get_target_columns(
        &self,
        column_ids: Vec<ColumnId>,
    ) -> ConnectorResult<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.columns
                    .iter()
                    .find(|c| c.column_id == *id)
                    .with_context(|| {
                        format!("Failed to find column id: {} in source: {:?}", id, self)
                    })
                    .cloned()
            })
            .try_collect()
            .map_err(Into::into)
    }

    pub async fn to_stream(
        &self,
        state: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
    ) -> ConnectorResult<BoxSourceChunkStream> {
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

        let parser_config = ParserConfig {
            specific: self.parser_config.clone(),
            common: CommonParserConfig {
                rw_columns: columns,
            },
        };
        let stream = match state {
            None => pending().boxed(),
            Some(splits) => {
                config
                    .create_split_reader(
                        splits,
                        parser_config,
                        source_ctx,
                        None,
                        Default::default(),
                    )
                    .await?
                    .0
            }
        };
        Ok(stream)
    }
}
