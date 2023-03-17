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

use futures::future::try_join_all;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::ConnectorError;
use risingwave_common::error::{internal_error, Result};
use risingwave_common::util::select_all;
use risingwave_connector::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use risingwave_connector::source::{
    BoxSourceWithStateStream, Column, ConnectorProperties, ConnectorState, SourceColumnDesc,
    SourceContext, SplitReaderImpl,
};

#[derive(Clone, Debug)]
pub struct ConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    // pub parser: Arc<SourceParserImpl>,
    pub parser_config: SpecificParserConfig,
    pub connector_message_buffer_size: usize,
}

impl ConnectorSource {
    pub fn new(
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_node_addr: Option<String>,
        connector_message_buffer_size: usize,
        parser_config: SpecificParserConfig,
    ) -> Result<Self> {
        let mut config =
            ConnectorProperties::extract(properties).map_err(|e| ConnectorError(e.into()))?;
        if let Some(addr) = connector_node_addr {
            // fixme: require source_id
            config.init_properties_for_cdc(0, addr, None)
        }

        Ok(Self {
            config,
            columns,
            parser_config,
            connector_message_buffer_size,
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
        splits: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
    ) -> Result<BoxSourceWithStateStream> {
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

        let to_reader_splits = match splits {
            Some(vec_split_impl) => vec_split_impl
                .into_iter()
                .map(|split| Some(vec![split]))
                .collect::<Vec<ConnectorState>>(),
            None => vec![None],
        };
        let readers = try_join_all(to_reader_splits.into_iter().map(|state| {
            tracing::debug!("spawning connector split reader for split {:?}", state);
            let props = config.clone();
            let columns = columns.clone();
            let data_gen_columns = Some(
                columns
                    .iter()
                    .map(|col| Column {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        is_visible: col.is_visible(),
                    })
                    .collect_vec(),
            );
            // TODO: is this reader split across multiple threads...? Realistically, we want
            // source_ctx to live in a single actor.
            let source_ctx = source_ctx.clone();
            async move {
                // InnerConnectorSourceReader::new(props, split, columns, metrics,
                // source_info).await
                let parser_config = ParserConfig {
                    specific: self.parser_config.clone(),
                    common: CommonParserConfig {
                        rw_columns: columns,
                    },
                };
                SplitReaderImpl::create(props, state, parser_config, source_ctx, data_gen_columns)
                    .await
            }
        }))
        .await?;

        Ok(select_all(readers.into_iter().map(|r| r.into_stream())).boxed())
    }
}
