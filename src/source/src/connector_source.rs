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

use anyhow::anyhow;
use futures::future::try_join_all;
use futures::stream::pending;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::ConnectorError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::select_all;
use risingwave_connector::dispatch_source_prop;
use risingwave_connector::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use risingwave_connector::source::filesystem::opendal_source::opendal_enumerator::OpendalEnumerator;
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalGcs, OpendalS3, OpendalSource,
};
use risingwave_connector::source::filesystem::FsPageItem;
use risingwave_connector::source::{
    create_split_reader, BoxSourceWithStateStream, BoxTryStream, Column, ConnectorProperties,
    ConnectorState, FsFilterCtrlCtx, SourceColumnDesc, SourceContext, SplitReader,
};
use tokio::time;
use tokio::time::Duration;

#[derive(Clone, Debug)]
pub struct ConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub parser_config: SpecificParserConfig,
    pub connector_message_buffer_size: usize,
}

#[derive(Clone, Debug)]
pub struct FsListCtrlContext {
    pub interval: Duration,
    pub last_tick: Option<time::Instant>,

    pub filter_ctx: FsFilterCtrlCtx,
}
pub type FsListCtrlContextRef = Arc<FsListCtrlContext>;

impl ConnectorSource {
    pub fn new(
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_message_buffer_size: usize,
        parser_config: SpecificParserConfig,
    ) -> Result<Self> {
        let config =
            ConnectorProperties::extract(properties).map_err(|e| ConnectorError(e.into()))?;

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
                        anyhow!("Failed to find column id: {} in source: {:?}", id, self).into()
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }

    pub fn get_source_list(&self) -> Result<BoxTryStream<FsPageItem>> {
        let config = self.config.clone();
        match config {
            ConnectorProperties::Gcs(prop) => {
                let lister: OpendalEnumerator<OpendalGcs> =
                    OpendalEnumerator::new_gcs_source(*prop)?;
                Ok(build_opendal_fs_list_stream(lister))
            }
            ConnectorProperties::OpendalS3(prop) => {
                let lister: OpendalEnumerator<OpendalS3> =
                    OpendalEnumerator::new_s3_source(prop.s3_properties, prop.assume_role)?;
                Ok(build_opendal_fs_list_stream(lister))
            }
            other => bail!("Unsupported source: {:?}", other),
        }
    }

    pub async fn stream_reader(
        &self,
        state: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
    ) -> Result<BoxSourceWithStateStream> {
        let Some(splits) = state else {
            return Ok(pending().boxed());
        };
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

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

        let parser_config = ParserConfig {
            specific: self.parser_config.clone(),
            common: CommonParserConfig {
                rw_columns: columns,
            },
        };

        let support_multiple_splits = config.support_multiple_splits();
        dispatch_source_prop!(config, prop, {
            let readers = if support_multiple_splits {
                tracing::debug!(
                    "spawning connector split reader for multiple splits {:?}",
                    splits
                );
                let reader =
                    create_split_reader(*prop, splits, parser_config, source_ctx, data_gen_columns)
                        .await?;

                vec![reader]
            } else {
                let to_reader_splits = splits.into_iter().map(|split| vec![split]);

                try_join_all(to_reader_splits.into_iter().map(|splits| {
                    tracing::debug!(?splits, ?prop, "spawning connector split reader");
                    let props = prop.clone();
                    let data_gen_columns = data_gen_columns.clone();
                    let parser_config = parser_config.clone();
                    // TODO: is this reader split across multiple threads...? Realistically, we want
                    // source_ctx to live in a single actor.
                    let source_ctx = source_ctx.clone();
                    create_split_reader(*props, splits, parser_config, source_ctx, data_gen_columns)
                }))
                .await?
            };

            Ok(select_all(readers.into_iter().map(|r| r.into_stream())).boxed())
        })
    }
}

#[try_stream(boxed, ok = FsPageItem, error = RwError)]
async fn build_opendal_fs_list_stream<Src: OpendalSource>(lister: OpendalEnumerator<Src>) {
    let matcher = lister.get_matcher();
    let mut object_metadata_iter = lister.list().await?;

    while let Some(list_res) = object_metadata_iter.next().await {
        match list_res {
            Ok(res) => {
                if matcher
                    .as_ref()
                    .map(|m| m.matches(&res.name))
                    .unwrap_or(true)
                {
                    yield res
                } else {
                    // Currrntly due to the lack of prefix list, we just skip the unmatched files.
                    continue;
                }
            }
            Err(err) => {
                tracing::error!("list object fail, err {}", err);
                return Err(err.into());
            }
        }
    }
}
