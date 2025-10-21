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
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_connector::parser::schema_change::SchemaChangeEnvelope;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxSourceChunkStream, CdcAutoSchemaChangeFailCallback, ConnectorState, CreateSplitReaderResult,
    SourceContext, SourceCtrlOpts, SplitMetaData, StreamChunkWithState,
};
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};

use super::{apply_rate_limit, get_split_offset_col_idx};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;

type AutoSchemaChangeSetup = (
    Option<mpsc::Sender<(SchemaChangeEnvelope, oneshot::Sender<()>)>>,
    Option<CdcAutoSchemaChangeFailCallback>,
);

pub(crate) struct StreamReaderBuilder {
    pub source_desc: SourceDesc,
    pub rate_limit: Option<u32>,
    pub source_id: TableId,
    pub source_name: String,
    pub reader_stream: Option<BoxSourceChunkStream>,

    // cdc related
    pub is_auto_schema_change_enable: bool,
    pub actor_ctx: ActorContextRef,
    pub cdc_table_schema_change_policies:
        HashMap<String, risingwave_connector::source::cdc::SchemaChangeFailurePolicy>,
}

impl StreamReaderBuilder {
    fn setup_auto_schema_change(&self) -> AutoSchemaChangeSetup {
        if self.is_auto_schema_change_enable {
            let (schema_change_tx, mut schema_change_rx) =
                mpsc::channel::<(SchemaChangeEnvelope, oneshot::Sender<()>)>(16);
            let meta_client = self.actor_ctx.meta_client.clone();

            // Extract source-level schema change failure policy as fallback
            let source_level_policy = match &self.source_desc.source.config {
                risingwave_connector::source::ConnectorProperties::MysqlCdc(props) => {
                    props.schema_change_failure_policy.clone()
                }
                risingwave_connector::source::ConnectorProperties::PostgresCdc(props) => {
                    props.schema_change_failure_policy.clone()
                }
                _ => risingwave_connector::source::cdc::SchemaChangeFailurePolicy::default(),
            };

            // Clone table-level policy mapping for use in async closure
            let table_policies = self.cdc_table_schema_change_policies.clone();

            // spawn a task to handle schema change event from source parser
            let _join_handle = tokio::task::spawn(async move {
                while let Some((mut schema_change, finish_tx)) = schema_change_rx.recv().await {
                    let table_ids = schema_change.table_ids();
                    tracing::info!(
                        target: "auto_schema_change",
                        "recv a schema change event for tables: {:?}", table_ids);

                    // Filter out schema changes for tables that are not managed in RW
                    // Only keep table changes that exist in table_policies mapping
                    schema_change.table_changes.retain(|tc| {
                        let exists = table_policies.contains_key(tc.cdc_table_id.as_str());
                        if !exists {
                            tracing::info!(
                                target: "auto_schema_change",
                                cdc_table_id = %tc.cdc_table_id,
                                "Skipping schema change for table not managed in RisingWave (not in table_policies)"
                            );
                        }
                        exists
                    });

                    // If all table changes were filtered out, skip sending to meta
                    if schema_change.is_empty() {
                        tracing::info!(
                            target: "auto_schema_change",
                            "All schema changes filtered out, skipping meta call"
                        );
                        finish_tx.send(()).unwrap();
                        continue;
                    }

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
                                // Extract cdc_table_id from the first table change event
                                let cdc_table_id = schema_change
                                    .table_changes
                                    .first()
                                    .map(|tc| tc.cdc_table_id.as_str())
                                    .unwrap_or("");

                                tracing::info!(
                                    target: "auto_schema_change",
                                    cdc_table_id = cdc_table_id,
                                    available_policies = ?table_policies,
                                    source_level_policy = ?source_level_policy,
                                    "CN Reader: Looking up schema change failure policy for table"
                                );

                                // Use table-level policy if available, otherwise fallback to source-level
                                let policy = table_policies
                                    .get(cdc_table_id)
                                    .cloned()
                                    .unwrap_or_else(|| source_level_policy.clone());

                                match policy {
                                    risingwave_connector::source::cdc::SchemaChangeFailurePolicy::Block => {
                                        tracing::error!(
                                            target: "auto_schema_change",
                                            error = %e.as_report(), "schema change error, blocking source");
                                        drop(finish_tx);
                                    }
                                    risingwave_connector::source::cdc::SchemaChangeFailurePolicy::Skip => {
                                        tracing::warn!(
                                            target: "auto_schema_change",
                                            error = %e.as_report(), "schema change error, skipping due to `schema_change_failure_policy` is set to Skip.");
                                        finish_tx.send(()).unwrap();
                                    }
                                }
                            }
                        }
                    }
                }
            });

            // Create callback function for reporting CDC auto schema change fail events
            let on_cdc_auto_schema_change_failure = if let Some(ref meta_client) =
                self.actor_ctx.meta_client
            {
                let meta_client = meta_client.clone();
                let source_id = self.source_id;
                Some(CdcAutoSchemaChangeFailCallback::new(
                    move |table_id: u32,
                          table_name: String,
                          cdc_table_id: String,
                          upstream_ddl: String,
                          fail_info: String| {
                        let meta_client = meta_client.clone();
                        let source_id = source_id;
                        tokio::spawn(async move {
                            if let Err(e) = meta_client
                                .add_cdc_auto_schema_change_fail_event(
                                    table_id,
                                    table_name,
                                    cdc_table_id,
                                    upstream_ddl,
                                    fail_info,
                                )
                                .await
                            {
                                tracing::warn!(
                                    error = %e.as_report(),
                                    source_id = source_id.table_id,
                                    "Failed to add CDC auto schema change fail event to event log."
                                );
                            }
                        });
                    },
                ))
            } else {
                None
            };

            (Some(schema_change_tx), on_cdc_auto_schema_change_failure)
        } else {
            info!("auto schema change is disabled in config");
            (None, None)
        }
    }

    fn prepare_source_stream_build(&self) -> (Vec<ColumnId>, SourceContext) {
        let column_ids = self
            .source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();

        let (schema_change_tx, on_cdc_auto_schema_change_failure) = self.setup_auto_schema_change();

        // Extract schema change failure policy from connector config
        let schema_change_failure_policy = match &self.source_desc.source.config {
            risingwave_connector::source::ConnectorProperties::MysqlCdc(props) => {
                props.schema_change_failure_policy.clone()
            }
            risingwave_connector::source::ConnectorProperties::PostgresCdc(props) => {
                props.schema_change_failure_policy.clone()
            }
            _ => risingwave_connector::source::cdc::SchemaChangeFailurePolicy::default(),
        };

        let source_ctx = SourceContext::new_with_auto_schema_change_callback(
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
            on_cdc_auto_schema_change_failure,
            schema_change_failure_policy,
            self.cdc_table_schema_change_policies.clone(),
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
                    tracing::error!(
                        error = %e.as_report(),
                        source_name = self.source_name,
                        source_id = self.source_id.table_id,
                        actor_id = self.actor_ctx.id,
                        "build stream source reader error, retry in 1s"
                    );
                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        e.variant_name().to_owned(),
                        self.source_id.table_id.to_string(),
                        self.source_name.to_owned(),
                        self.actor_ctx.fragment_id.to_string(),
                    ]);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue 'build_consume_loop;
                }
            }

            let (stream, _) = build_stream_result.unwrap();
            let stream = apply_rate_limit(stream, self.rate_limit).boxed();
            let mut is_error = false;
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
                        tracing::error!(
                            error = %e.as_report(),
                            source_name = self.source_name,
                            source_id = self.source_id.table_id,
                            actor_id = self.actor_ctx.id,
                            "stream source reader error"
                        );
                        GLOBAL_ERROR_METRICS.user_source_error.report([
                            e.variant_name().to_owned(),
                            self.source_id.table_id.to_string(),
                            self.source_name.to_owned(),
                            self.actor_ctx.fragment_id.to_string(),
                        ]);
                        is_error = true;
                        break 'consume;
                    }
                }
            }
            if !is_error {
                tracing::info!("stream source reader consume finished");
                latest_splits_info.values_mut().for_each(|split_impl| {
                    if let Some(mut batch_split) = split_impl.clone().into_batch_split() {
                        batch_split.finish();
                        *split_impl = batch_split.into();
                    }
                });
                yield (
                    StreamChunk::empty(
                        self.source_desc
                            .columns
                            .iter()
                            .map(|c| c.data_type.clone())
                            .collect_vec()
                            .as_slice(),
                    ),
                    latest_splits_info.clone(),
                );
                break 'build_consume_loop;
            }
            tracing::info!("stream source reader error, retry in 1s");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
