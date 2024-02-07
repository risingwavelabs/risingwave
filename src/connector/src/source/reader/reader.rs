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
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use futures::stream::pending;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnId;
use rw_futures_util::select_all;

use crate::dispatch_source_prop;
use crate::error::ConnectorResult;
use crate::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use crate::source::filesystem::opendal_source::opendal_enumerator::OpendalEnumerator;
use crate::source::filesystem::opendal_source::{
    OpendalGcs, OpendalPosixFs, OpendalS3, OpendalSource,
};
use crate::source::filesystem::FsPageItem;
use crate::source::{
    create_split_reader, BoxChunkSourceStream, BoxTryStream, Column, ConnectorProperties,
    ConnectorState, SourceColumnDesc, SourceContext, SplitReader,
};

#[derive(Clone, Debug)]
pub struct SourceReader {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub parser_config: SpecificParserConfig,
    pub connector_message_buffer_size: usize,
}

impl SourceReader {
    pub fn new(
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_message_buffer_size: usize,
        parser_config: SpecificParserConfig,
    ) -> ConnectorResult<Self> {
        let config = ConnectorProperties::extract(properties, false)?;

        Ok(Self {
            config,
            columns,
            parser_config,
            connector_message_buffer_size,
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

    pub fn get_source_list(&self) -> ConnectorResult<BoxTryStream<FsPageItem>> {
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
            ConnectorProperties::PosixFs(prop) => {
                let lister: OpendalEnumerator<OpendalPosixFs> =
                    OpendalEnumerator::new_posix_fs_source(*prop)?;
                Ok(build_opendal_fs_list_stream(lister))
            }
            other => bail!("Unsupported source: {:?}", other),
        }
    }

    pub async fn to_stream(
        &self,
        state: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
    ) -> ConnectorResult<BoxChunkSourceStream> {
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

#[try_stream(boxed, ok = FsPageItem, error = crate::error::ConnectorError)]
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
                return Err(err);
            }
        }
    }
}
