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

use anyhow::{Context, anyhow};
use either::Either;
use iceberg::scan::FileScanTask;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::id::TableId;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::iceberg::metrics::GLOBAL_ICEBERG_SCAN_METRICS;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{PersistedFileScanTask, StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchIcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,
    /// Columns of fetch executor, used to plan files.
    /// For backward compatibility, this can be None, meaning all columns are needed.
    downstream_columns: Option<Vec<ColumnCatalog>>,
    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// Local barrier manager for reporting list finished
    barrier_manager: LocalBarrierManager,
    associated_table_id: TableId,
    /// Metrics for monitor.
    _metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> BatchIcebergListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        downstream_columns: Option<Vec<ColumnCatalog>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core,
            downstream_columns,
            barrier_receiver: Some(barrier_receiver),
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
            _metrics: metrics,
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
        // Initialize metrics context.
        let iceberg_metrics = &GLOBAL_ICEBERG_SCAN_METRICS;
        let iceberg_table_name = iceberg_properties.table.table_name().to_owned();
        let source_id_str = self.stream_source_core.source_id.to_string();
        let source_name_str = self.stream_source_core.source_name.clone();
        let metrics_labels = [
            source_id_str.as_str(),
            source_name_str.as_str(),
            iceberg_table_name.as_str(),
        ];

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );
        let mut is_refreshing = false;
        let is_list_finished = Arc::new(RwLock::new(false));

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "encountered an error in batch iceberg list");

                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        e.variant_name().to_owned(),
                        self.stream_source_core.source_id.to_string(),
                        self.actor_ctx.fragment_id.to_string(),
                        self.associated_table_id.to_string(),
                    ]);

                    if !iceberg_table_name.is_empty() {
                        iceberg_metrics
                            .iceberg_source_scan_errors_total
                            .with_guarded_label_values(&[
                                metrics_labels[0],
                                metrics_labels[1],
                                metrics_labels[2],
                                "list_error",
                            ])
                            .inc();
                    }

                    if is_refreshing {
                        tracing::info!(
                            source_id = %self.stream_source_core.source_id,
                            table_id = %self.associated_table_id,
                            "re-listing iceberg scan tasks due to error"
                        );
                        let iceberg_list_stream = Self::list_iceberg_scan_task(
                            *iceberg_properties.clone(),
                            downstream_columns.clone(),
                            is_list_finished.clone(),
                            source_id_str.clone(),
                            source_name_str.clone(),
                        )
                        .boxed();
                        stream.replace_data_stream(iceberg_list_stream);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(e);
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::RefreshStart {
                                        associated_source_id,
                                        ..
                                    } if associated_source_id
                                        == &self.stream_source_core.source_id =>
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            table_id = %self.associated_table_id,
                                            "RefreshStart triggered file re-listing"
                                        );
                                        is_refreshing = true;

                                        *is_list_finished.write() = false;

                                        // re-list iceberg scan tasks
                                        let iceberg_list_stream = Self::list_iceberg_scan_task(
                                            *iceberg_properties.clone(),
                                            downstream_columns.clone(),
                                            is_list_finished.clone(),
                                            source_id_str.clone(),
                                            source_name_str.clone(),
                                        )
                                        .boxed();
                                        stream.replace_data_stream(iceberg_list_stream);
                                    }
                                    _ => (),
                                }
                            }

                            if is_refreshing && *is_list_finished.read() && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    source_id = %self.stream_source_core.source_id,
                                    table_id = %self.associated_table_id,
                                    "reporting batch iceberg list finished"
                                );
                                self.barrier_manager.report_source_list_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    self.stream_source_core.source_id,
                                );
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);
                        }
                        _ => unreachable!(),
                    },
                    Either::Right(file_scan_task) => {
                        let chunk = StreamChunk::from_rows(
                            &[(
                                Op::Insert,
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Utf8(file_scan_task.data_file_path().into())),
                                    Some(ScalarImpl::Jsonb(PersistedFileScanTask::encode(
                                        file_scan_task,
                                    ))),
                                ]),
                            )],
                            &[DataType::Varchar, DataType::Jsonb],
                        );

                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }

    #[try_stream(ok = FileScanTask, error = StreamExecutorError)]
    async fn list_iceberg_scan_task(
        iceberg_properties: IcebergProperties,
        downstream_columns: Option<Vec<String>>,
        is_list_finished: Arc<RwLock<bool>>,
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

        let table = iceberg_properties.load_table().await?;
        if let Some(start_snapshot) = table.metadata().current_snapshot() {
            let latest_snapshot = start_snapshot.snapshot_id();
            let snapshot_scan_builder = table.scan().snapshot_id(latest_snapshot);
            let snapshot_scan = if let Some(ref downstream_columns) = downstream_columns {
                snapshot_scan_builder.select(downstream_columns)
            } else {
                snapshot_scan_builder.select_all()
            }
            .build()
            .context("failed to build iceberg scan")?;

            let list_start = std::time::Instant::now();
            let mut data_file_count: u64 = 0;
            let mut eq_delete_count: u64 = 0;
            let mut pos_delete_count: u64 = 0;

            #[for_await]
            for scan_task in snapshot_scan
                .plan_files()
                .await
                .context("failed to plan iceberg files")?
            {
                let scan_task = scan_task.context("failed to get scan task")?;
                data_file_count += 1;
                for delete_task in &scan_task.deletes {
                    match delete_task.data_file_content {
                        iceberg::spec::DataContentType::EqualityDeletes => eq_delete_count += 1,
                        iceberg::spec::DataContentType::PositionDeletes => pos_delete_count += 1,
                        _ => {}
                    }
                }
                tracing::debug!(
                    "list_iceberg_scan_task: data_file_path={}, scan_task={:?}",
                    scan_task.data_file_path,
                    scan_task
                );
                yield scan_task;
            }

            let list_duration = list_start.elapsed();
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
        }
        *is_list_finished.write() = true;
    }
}

impl<S: StateStore> Execute for BatchIcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchIcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchIcebergListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .field("downstream_columns", &self.downstream_columns)
            .finish()
    }
}
