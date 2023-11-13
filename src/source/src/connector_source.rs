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
use futures::stream::pending;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::ConnectorError;
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_common::util::select_all;
use risingwave_connector::dispatch_source_prop;
use risingwave_connector::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use risingwave_connector::source::filesystem::{FsPage, FsPageItem, S3SplitEnumerator};
use risingwave_connector::source::{
    create_split_reader, BoxSourceWithStateStream, BoxTryStream, Column, ConnectorProperties,
    ConnectorState, FsFilterCtrlCtx, FsListInner, SourceColumnDesc, SourceContext,
    SourceEnumeratorContext, SplitEnumerator, SplitReader,
};
use tokio::time;
use tokio::time::{Duration, MissedTickBehavior};

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
                        internal_error(format!(
                            "Failed to find column id: {} in source: {:?}",
                            id, self
                        ))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }

    pub async fn get_source_list(&self) -> Result<BoxTryStream<FsPage>> {
        let config = self.config.clone();
        let lister = match config {
            ConnectorProperties::S3(prop) => {
                S3SplitEnumerator::new(*prop, Arc::new(SourceEnumeratorContext::default())).await?
            }
            other => return Err(internal_error(format!("Unsupported source: {:?}", other))),
        };

        Ok(build_fs_list_stream(
            FsListCtrlContext {
                interval: Duration::from_secs(60),
                last_tick: None,
                filter_ctx: FsFilterCtrlCtx,
            },
            lister,
        ))
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

#[try_stream(boxed, ok = FsPage, error = RwError)]
async fn build_fs_list_stream(
    mut ctrl_ctx: FsListCtrlContext,
    mut list_op: impl FsListInner + Send + 'static,
) {
    let mut interval = time::interval(ctrl_ctx.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // controlling whether request for next page
    fn page_ctrl_logic(_ctx: &FsListCtrlContext, has_finished: bool, _page_num: usize) -> bool {
        !has_finished
    }

    loop {
        let mut page_num = 0;
        ctrl_ctx.last_tick = Some(time::Instant::now());
        'inner: loop {
            let (fs_page, has_finished) = list_op.get_next_page::<FsPageItem>().await?;
            let matched_items = fs_page
                .into_iter()
                .filter(|item| list_op.filter_policy(&ctrl_ctx.filter_ctx, page_num, item))
                .collect_vec();
            yield matched_items;
            page_num += 1;
            if !page_ctrl_logic(&ctrl_ctx, has_finished, page_num) {
                break 'inner;
            }
        }
        interval.tick().await;
    }
}
