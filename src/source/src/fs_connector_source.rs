// Copyright 2023 RisingWave Labs
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
use std::sync::Arc;

use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::ConnectorError;
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_connector::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use risingwave_connector::source::{
    ConnectorProperties, ConnectorState, SourceColumnDesc, SourceContext, SplitReaderImpl,
};

#[derive(Clone, Debug)]
pub struct FsConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub properties: HashMap<String, String>,
    pub parser_config: SpecificParserConfig,
}

impl FsConnectorSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_node_addr: Option<String>,
        parser_config: SpecificParserConfig,
    ) -> Result<Self> {
        // Store the connector node address to properties for later use.
        let mut source_props: HashMap<String, String> =
            HashMap::from_iter(properties.clone().into_iter());
        connector_node_addr
            .map(|addr| source_props.insert("connector_node_addr".to_string(), addr));
        let config =
            ConnectorProperties::extract(source_props).map_err(|e| ConnectorError(e.into()))?;

        Ok(Self {
            config,
            columns,
            properties,
            parser_config,
        })
    }

    fn get_target_columns(&self, column_ids: Vec<ColumnId>) -> Result<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.columns
                    .iter()
                    .find(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        internal_error(format!(
                            "Failed to find column id: {} in source: {:?}",
                            id, self
                        ))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }

    pub async fn stream_reader(
        &self,
        state: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
    ) -> Result<SplitReaderImpl> {
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

        let parser_config = ParserConfig {
            specific: self.parser_config.clone(),
            common: CommonParserConfig {
                rw_columns: columns,
            },
        };
        SplitReaderImpl::create(config, state, parser_config, source_ctx, None)
            .await
            .map_err(RwError::from)
    }
}
