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

use std::sync::Arc;

use anyhow::{Context, anyhow};
use either::Either;
use futures_async_stream::try_stream;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use parking_lot::Mutex;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::config::StreamingConfig;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::iceberg::metrics::GLOBAL_ICEBERG_SCAN_METRICS;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{PersistedFileScanTask, StreamSourceCore, barrier_to_message_stream};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct IcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Columns of fetch executor, used to plan files.
    /// For backward compatibility, this can be None, meaning all columns are needed.
    downstream_columns: Option<Vec<ColumnCatalog>>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,

    /// Streaming config
    streaming_config: Arc<StreamingConfig>,
}

impl<S: StateStore> IcebergListExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        downstream_columns: Option<Vec<ColumnCatalog>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            downstream_columns,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
            streaming_config,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;
        let first_epoch = first_barrier.epoch;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            unreachable!()
        };

        // Get consistent column names for schema stability across snapshots
        let downstream_columns = self.downstream_columns.map(|columns| {
            columns
                .iter()
                .filter_map(|col| {
                    if col.is_hidden() {
                        None
                    } else {
                        Some(col.name().to_owned())
                    }
                })
                .collect::<Vec<_>>()
        });

        tracing::debug!("downstream_columns: {:?}", downstream_columns);

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let state_table = self.stream_source_core.split_state_store.state_table_mut();
        state_table.init_epoch(first_epoch).await?;
        let state_row = state_table.get_from_one_value_table().await?;
        // last_snapshot is EXCLUSIVE (i.e., already scanned)
        let mut last_snapshot: Option<i64> = state_row.map(|s| *s.as_int64());
        let mut prev_persisted_snapshot = last_snapshot;

        if last_snapshot.is_none() {
            // do a regular scan, then switch to incremental scan
            // TODO: we may support starting from a specific snapshot/timestamp later
            let table = iceberg_properties.load_table().await?;
            // If current_snapshot is None (empty table), we go to incremental scan directly.
            if let Some(start_snapshot) = table.metadata().current_snapshot() {
                last_snapshot = Some(start_snapshot.snapshot_id());
                let snapshot_scan_builder = table.scan().snapshot_id(start_snapshot.snapshot_id());

                let snapshot_scan = if let Some(ref downstream_columns) = downstream_columns {
                    snapshot_scan_builder.select(downstream_columns)
                } else {
                    // for backward compatibility
                    snapshot_scan_builder.select_all()
                }
                .build()
                .context("failed to build iceberg scan")?;

                let mut chunk_builder = StreamChunkBuilder::new(
                    self.streaming_config.developer.chunk_size,
                    vec![DataType::Varchar, DataType::Jsonb],
                );
                #[for_await]
                for scan_task in snapshot_scan
                    .plan_files()
                    .await
                    .context("failed to plan iceberg files")?
                {
                    let scan_task = scan_task.context("failed to get scan task")?;
                    if let Some(chunk) = chunk_builder.append_row(
                        Op::Insert,
                        &[
                            Some(ScalarImpl::Utf8(scan_task.data_file_path().into())),
                            Some(ScalarImpl::Jsonb(
                                serde_json::to_value(scan_task).unwrap().into(),
                            )),
                        ],
                    ) {
                        yield Message::Chunk(chunk);
                    }
                }
                if let Some(chunk) = chunk_builder.take() {
                    yield Message::Chunk(chunk);
                }
            }
        }

        let source_id_str = self.stream_source_core.source_id.to_string();
        let source_name_str = self.stream_source_core.source_name.clone();
        let list_table_name = iceberg_properties.table.table_name().to_owned();
        let list_metrics = &GLOBAL_ICEBERG_SCAN_METRICS;

        let last_snapshot = Arc::new(Mutex::new(last_snapshot));
        let build_incremental_stream = || {
            incremental_scan_stream(
                (*iceberg_properties).clone(),
                last_snapshot.clone(),
                self.streaming_config.developer.iceberg_list_interval_sec,
                downstream_columns.clone(),
                source_id_str.clone(),
                source_name_str.clone(),
            )
            .map(|res| match res {
                Ok(scan_task) => {
                    let row = (
                        Op::Insert,
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(scan_task.data_file_path().into())),
                            Some(ScalarImpl::Jsonb(PersistedFileScanTask::encode(scan_task))),
                        ]),
                    );
                    Ok(StreamChunk::from_rows(
                        &[row],
                        &[DataType::Varchar, DataType::Jsonb],
                    ))
                }
                Err(e) => Err(e),
            })
        };

        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, build_incremental_stream());

        // TODO: support pause (incl. pause on startup)/resume/rate limit

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "incremental iceberg list stream errored, rebuilding"
                    );
                    list_metrics
                        .iceberg_source_scan_errors_total
                        .with_guarded_label_values(&[
                            source_id_str.as_str(),
                            source_name_str.as_str(),
                            list_table_name.as_str(),
                            "list_error",
                        ])
                        .inc();
                    stream.replace_data_stream(build_incremental_stream());
                }
                Ok(msg) => match msg {
                    // Barrier arrives.
                    Either::Left(msg) => match &msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    _ => (),
                                }
                            }
                            if let Some(last_snapshot) = *last_snapshot.lock() {
                                let state_row =
                                    OwnedRow::new(vec![ScalarImpl::Int64(last_snapshot).into()]);
                                if let Some(prev_persisted_snapshot_id) = prev_persisted_snapshot {
                                    let prev_state_row = OwnedRow::new(vec![
                                        ScalarImpl::Int64(prev_persisted_snapshot_id).into(),
                                    ]);
                                    state_table.update(prev_state_row, state_row);
                                } else {
                                    state_table.insert(state_row);
                                }
                                prev_persisted_snapshot = Some(last_snapshot);
                            }
                            state_table
                                .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                                .await?;
                            // Propagate the barrier.
                            yield msg;
                        }
                        // Only barrier can be received.
                        _ => unreachable!(),
                    },
                    // Data arrives.
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

/// `last_snapshot` is EXCLUSIVE (i.e., already scanned)
#[try_stream(boxed, ok = FileScanTask, error = StreamExecutorError)]
async fn incremental_scan_stream(
    iceberg_properties: IcebergProperties,
    last_snapshot_lock: Arc<Mutex<Option<i64>>>,
    list_interval_sec: u64,
    downstream_columns: Option<Vec<String>>,
    source_id: String,
    source_name: String,
) {
    let metrics = &GLOBAL_ICEBERG_SCAN_METRICS;
    let table_name = iceberg_properties.table.table_name().to_owned();
    let label_values = [
        source_id.as_str(),
        source_name.as_str(),
        table_name.as_str(),
    ];

    let mut last_snapshot: Option<i64> = *last_snapshot_lock.lock();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(list_interval_sec)).await;

        // XXX: should we use sth like table.refresh() instead of reload the table every time?
        // iceberg-java does this, but iceberg-rust doesn't have this API now.
        let table = iceberg_properties.load_table().await?;

        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            tracing::info!("Skip incremental scan because table is empty");
            continue;
        };

        if Some(current_snapshot.snapshot_id()) == last_snapshot {
            // Ingestion is caught up — lag is 0.
            metrics
                .iceberg_source_snapshot_lag_seconds
                .with_guarded_label_values(&label_values)
                .set(0);

            tracing::info!(
                "Current table snapshot is already enumerated: {}, no new snapshot available",
                current_snapshot.snapshot_id()
            );
            continue;
        }

        if let Some(last_snapshot_id) = last_snapshot
            && let Some(last_ingested_snapshot) = table
                .metadata()
                .snapshots()
                .find(|snapshot| snapshot.snapshot_id() == last_snapshot_id)
        {
            let lag_secs =
                ((current_snapshot.timestamp_ms() - last_ingested_snapshot.timestamp_ms()).max(0)
                    / 1000) as i64;
            metrics
                .iceberg_source_snapshot_lag_seconds
                .with_guarded_label_values(&label_values)
                .set(lag_secs);
        }

        // New snapshot discovered.
        metrics
            .iceberg_source_snapshots_discovered_total
            .with_guarded_label_values(&label_values)
            .inc();

        let mut incremental_scan = table.scan().to_snapshot_id(current_snapshot.snapshot_id());
        if let Some(last_snapshot) = last_snapshot {
            incremental_scan = incremental_scan.from_snapshot_id(last_snapshot);
        }
        let incremental_scan = if let Some(ref downstream_columns) = downstream_columns {
            incremental_scan.select(downstream_columns)
        } else {
            // for backward compatibility
            incremental_scan.select_all()
        }
        .build()
        .context("failed to build iceberg scan")?;

        let mut list_duration = std::time::Duration::default();
        let mut active_since = std::time::Instant::now();
        let mut data_file_count: u64 = 0;
        let mut eq_delete_count: u64 = 0;
        let mut pos_delete_count: u64 = 0;

        #[for_await]
        for scan_task in incremental_scan
            .plan_files()
            .await
            .context("failed to plan iceberg files")?
        {
            let scan_task = scan_task.context("failed to get scan task")?;

            // Count file types: the main task is a data file, deletes are attached.
            data_file_count += 1;
            for delete_task in &scan_task.deletes {
                match delete_task.data_file_content {
                    DataContentType::EqualityDeletes => eq_delete_count += 1,
                    DataContentType::PositionDeletes => pos_delete_count += 1,
                    _ => {}
                }
            }

            // Record delete files per data file.
            metrics
                .iceberg_source_delete_files_per_data_file
                .with_guarded_label_values(&label_values)
                .observe(scan_task.deletes.len() as f64);

            list_duration += active_since.elapsed();
            yield scan_task;
            active_since = std::time::Instant::now();
        }

        list_duration += active_since.elapsed();
        metrics
            .iceberg_source_list_duration_seconds
            .with_guarded_label_values(&label_values)
            .observe(list_duration.as_secs_f64());

        if data_file_count > 0 {
            metrics
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[
                    label_values[0],
                    label_values[1],
                    label_values[2],
                    "data",
                ])
                .inc_by(data_file_count);
        }
        if eq_delete_count > 0 {
            metrics
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[
                    label_values[0],
                    label_values[1],
                    label_values[2],
                    "eq_delete",
                ])
                .inc_by(eq_delete_count);
        }
        if pos_delete_count > 0 {
            metrics
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[
                    label_values[0],
                    label_values[1],
                    label_values[2],
                    "pos_delete",
                ])
                .inc_by(pos_delete_count);
        }

        // This scan has fully ingested the latest snapshot we observed, so lag returns to 0.
        metrics
            .iceberg_source_snapshot_lag_seconds
            .with_guarded_label_values(&label_values)
            .set(0);

        last_snapshot = Some(current_snapshot.snapshot_id());
        *last_snapshot_lock.lock() = last_snapshot;
    }
}

impl<S: StateStore> Execute for IcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
