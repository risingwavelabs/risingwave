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
use risingwave_common::array::Op;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{PersistedFileScanTask, StreamSourceCore, barrier_to_message_stream};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct BatchIcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
}

impl<S: StateStore> BatchIcebergListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
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

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        // Create the initial scan stream for the latest snapshot
        let scan_stream = Self::create_snapshot_scan_stream(iceberg_properties.as_ref().clone())
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
            });

        let mut stream = StreamReaderWithPause::<true, _>::new(barrier_stream, scan_stream);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "encountered an error");
                }
                Ok(msg) => {
                    match msg {
                        // Barrier arrives.
                        Either::Left(msg) => {
                            match &msg {
                                Message::Barrier(barrier) => {
                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            Mutation::RefreshStart {
                                                table_id: _,
                                                associated_source_id,
                                            } if *associated_source_id
                                                == self.stream_source_core.source_id =>
                                            {
                                                tracing::info!(
                                                    actor_id = self.actor_ctx.id,
                                                    source_id = ?self.stream_source_core.source_id,
                                                    "RefreshStart triggered, resetting to latest snapshot"
                                                );
                                                // Stop current reading and start new snapshot scan
                                                let new_scan_stream = Self::create_snapshot_scan_stream(
                                            iceberg_properties.as_ref().clone(),
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
                                        });
                                                stream.replace_data_stream(new_scan_stream);
                                            }
                                            _ => (),
                                        }
                                    }
                                    // Propagate the barrier.
                                    yield msg;
                                }
                                // Only barrier can be received.
                                _ => unreachable!(),
                            }
                        }
                        // Data arrives.
                        Either::Right(chunk) => {
                            yield Message::Chunk(chunk);
                        }
                    }
                }
            }
        }
    }

    /// Creates a stream that scans the latest snapshot of the Iceberg table.
    #[try_stream(boxed, ok = FileScanTask, error = StreamExecutorError)]
    async fn create_snapshot_scan_stream(iceberg_properties: IcebergProperties) {
        let table = iceberg_properties.load_table().await?;

        // Get the current snapshot, skip if empty
        if let Some(current_snapshot) = table.metadata().current_snapshot() {
            tracing::info!(
                snapshot_id = current_snapshot.snapshot_id(),
                "Starting scan of snapshot"
            );

            let snapshot_scan = table
                .scan()
                .snapshot_id(current_snapshot.snapshot_id())
                // TODO: col prune
                // NOTE: select by name might be not robust under schema evolution
                .select_all()
                .build()
                .context("failed to build iceberg scan")?;

            #[for_await]
            for scan_task in snapshot_scan
                .plan_files()
                .await
                .context("failed to plan iceberg files")?
            {
                let scan_task = scan_task.context("failed to get scan task")?;
                yield scan_task;
            }
        } else {
            tracing::info!("Skip scan because table is empty");
        }
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
            .finish()
    }
}
