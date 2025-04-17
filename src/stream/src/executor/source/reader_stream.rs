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

use std::collections::HashMap;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_connector::parser::schema_change::SchemaChangeEnvelope;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxSourceChunkStream, ConnectorState, CreateSplitReaderResult, SourceContext, SourceCtrlOpts,
    SplitMetaData, StreamChunkWithState,
};
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};

use super::{apply_rate_limit, get_split_offset_col_idx};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;

pub(crate) struct StreamReaderBuilder {
    pub source_desc: SourceDesc,
    pub rate_limit: Option<u32>,
    pub source_id: TableId,
    pub source_name: String,
    pub reader_stream: Option<BoxSourceChunkStream>,

    // cdc related
    pub is_auto_schema_change_enable: bool,
    pub actor_ctx: ActorContextRef,
}

impl StreamReaderBuilder {
    fn prepare_source_stream_build(&self) -> (Vec<ColumnId>, SourceContext) {
        let column_ids = self
            .source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();

        let (schema_change_tx, mut schema_change_rx) =
            mpsc::channel::<(SchemaChangeEnvelope, oneshot::Sender<()>)>(16);
        let schema_change_tx = if self.is_auto_schema_change_enable {
            let meta_client = self.actor_ctx.meta_client.clone();
            // spawn a task to handle schema change event from source parser
            let _join_handle = tokio::task::spawn(async move {
                while let Some((schema_change, finish_tx)) = schema_change_rx.recv().await {
                    let table_ids = schema_change.table_ids();
                    tracing::info!(
                        target: "auto_schema_change",
                        "recv a schema change event for tables: {:?}", table_ids);
                    // TODO: retry on rpc error
                    if let Some(ref meta_client) = meta_client {
                        match meta_client
                            .auto_schema_change(schema_change.to_protobuf())
                            .await
                        {
                            Ok(_) => {
                                tracing::info!(
                                    target: "auto_schema_change",
                                    "schema change success for tables: {:?}", table_ids);
                                finish_tx.send(()).unwrap();
                            }
                            Err(e) => {
                                tracing::error!(
                                    target: "auto_schema_change",
                                    error = %e.as_report(), "schema change error");
                                finish_tx.send(()).unwrap();
                            }
                        }
                    }
                }
            });
            Some(schema_change_tx)
        } else {
            info!("auto schema change is disabled in config");
            None
        };

        let source_ctx = SourceContext::new(
            self.actor_ctx.id,
            self.source_id,
            self.actor_ctx.fragment_id,
            self.source_name.clone(),
            self.source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit),
                split_txn: self.rate_limit.is_some(), // when rate limiting, we may split txn
            },
            self.source_desc.source.config.clone(),
            schema_change_tx,
        );

        (column_ids, source_ctx)
    }

    pub(crate) async fn fetch_latest_splits(
        &mut self,
        state: ConnectorState,
        seek_to_latest: bool,
    ) -> StreamExecutorResult<CreateSplitReaderResult> {
        let (column_ids, source_ctx) = self.prepare_source_stream_build();
        let source_ctx_ref = Arc::new(source_ctx);
        let (stream, res) = self
            .source_desc
            .source
            .build_stream(
                state.clone(),
                column_ids.clone(),
                source_ctx_ref.clone(),
                seek_to_latest,
            )
            .await
            .map_err(StreamExecutorError::connector_error)?;
        self.reader_stream = Some(stream);
        Ok(res)
    }

    #[try_stream(ok = StreamChunkWithState, error = StreamExecutorError)]
    pub(crate) async fn into_retry_stream(mut self, state: ConnectorState, is_initial_build: bool) {
        let (column_ids, source_ctx) = self.prepare_source_stream_build();
        let source_ctx_ref = Arc::new(source_ctx);

        let mut latest_splits_info = {
            if let Some(splits) = state.as_ref() {
                splits
                    .iter()
                    .map(|split| (split.id(), split.clone()))
                    .collect::<HashMap<_, _>>()
            } else {
                HashMap::new()
            }
        };

        let (Some(split_idx), Some(offset_idx)) =
            get_split_offset_col_idx(&self.source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        'build_consume_loop: loop {
            let bootstrap_state = if latest_splits_info.is_empty() {
                None
            } else {
                Some(latest_splits_info.values().cloned().collect_vec())
            };
            tracing::debug!(
                "build stream source reader with state: {:?}",
                bootstrap_state
            );
            let build_stream_result = if let Some(exist_stream) = self.reader_stream.take() {
                Ok((exist_stream, CreateSplitReaderResult::default()))
            } else {
                self.source_desc
                    .source
                    .build_stream(
                        bootstrap_state,
                        column_ids.clone(),
                        source_ctx_ref.clone(),
                        // just `seek_to_latest` for initial build
                        is_initial_build,
                    )
                    .await
            };
            if let Err(e) = build_stream_result {
                if is_initial_build {
                    return Err(StreamExecutorError::connector_error(e));
                } else {
                    tracing::warn!(
                        error = %e.as_report(),
                        source_name = self.source_name,
                        source_id = self.source_id.table_id,
                        actor_id = self.actor_ctx.id,
                        "build stream source reader error, retry in 1s"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue 'build_consume_loop;
                }
            }

            let (stream, _) = build_stream_result.unwrap();
            let stream = apply_rate_limit(stream, self.rate_limit).boxed();
            #[for_await]
            'consume: for msg in stream {
                match msg {
                    Ok(msg) => {
                        for (_, row) in msg.rows() {
                            let split = row.datum_at(split_idx).unwrap().into_utf8();
                            let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                            latest_splits_info
                                .get_mut(&Arc::from(split.to_owned()))
                                .map(|split_impl| split_impl.update_in_place(offset.to_owned()));
                        }
                        yield (msg, latest_splits_info.clone());
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            source_name = self.source_name,
                            source_id = self.source_id.table_id,
                            actor_id = self.actor_ctx.id,
                            "stream source reader error"
                        );
                        break 'consume;
                    }
                }
            }
            tracing::info!("stream source reader error, retry in 1s");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
