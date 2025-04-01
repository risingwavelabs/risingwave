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

use std::future::pending;
use std::num::NonZeroU64;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::bitmap::Bitmap;
use risingwave_pb::connector_service::SinkMetadata;
use tracing::{info, warn};

use super::{
    LogSinker, SinkCoordinationRpcClientEnum, SinkError, SinkLogReader, SinkWriterMetrics,
    SinkWriterParam,
};
use crate::sink::writer::SinkWriter;
use crate::sink::{LogStoreReadItem, Result, SinkParam, TruncateOffset};

pub struct CoordinatedLogSinker<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> {
    writer: W,
    sink_coordinate_client: SinkCoordinationRpcClientEnum,
    param: SinkParam,
    vnode_bitmap: Bitmap,
    commit_checkpoint_interval: NonZeroU64,
    sink_writer_metrics: SinkWriterMetrics,
}

impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> CoordinatedLogSinker<W> {
    pub async fn new(
        writer_param: &SinkWriterParam,
        param: SinkParam,
        writer: W,
        commit_checkpoint_interval: NonZeroU64,
    ) -> Result<Self> {
        Ok(Self {
            writer,
            sink_coordinate_client: writer_param
                .meta_client
                .as_ref()
                .ok_or_else(|| anyhow!("should have meta client"))?
                .clone()
                .sink_coordinate_client()
                .await,
            param,
            vnode_bitmap: writer_param
                .vnode_bitmap
                .as_ref()
                .ok_or_else(|| {
                    anyhow!("sink needs coordination and should not have singleton input")
                })?
                .clone(),
            commit_checkpoint_interval,
            sink_writer_metrics: SinkWriterMetrics::new(writer_param),
        })
    }
}

#[async_trait]
impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> LogSinker for CoordinatedLogSinker<W> {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        let (mut coordinator_stream_handle, log_store_rewind_start_epoch) = self
            .sink_coordinate_client
            .new_stream_handle(&self.param, self.vnode_bitmap)
            .await?;
        let mut sink_writer = self.writer;
        log_reader.start_from(log_store_rewind_start_epoch).await?;
        #[derive(Debug)]
        enum LogConsumerState {
            /// Mark that the log consumer is not initialized yet
            Uninitialized,

            /// Mark that a new epoch has begun.
            EpochBegun { curr_epoch: u64 },

            /// Mark that the consumer has just received a barrier
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;

        let mut current_checkpoint: u64 = 0;
        let commit_checkpoint_interval = self.commit_checkpoint_interval;
        let sink_writer_metrics = self.sink_writer_metrics;

        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => {
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::EpochBegun { curr_epoch } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::BarrierReceived { prev_epoch, .. } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );

                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, .. } => {
                    if let Err(e) = sink_writer.write_batch(chunk).await {
                        sink_writer.abort().await?;
                        return Err(e);
                    }
                }
                LogStoreReadItem::Barrier {
                    is_checkpoint,
                    new_vnode_bitmap,
                    is_stop,
                } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        current_checkpoint += 1;
                        if current_checkpoint >= commit_checkpoint_interval.get()
                            || new_vnode_bitmap.is_some()
                            || is_stop
                        {
                            let start_time = Instant::now();
                            let metadata = sink_writer.barrier(true).await?;
                            let metadata = metadata.ok_or_else(|| {
                                SinkError::Coordinator(anyhow!(
                                    "should get metadata on checkpoint barrier"
                                ))
                            })?;
                            coordinator_stream_handle.commit(epoch, metadata).await?;
                            sink_writer_metrics
                                .sink_commit_duration
                                .observe(start_time.elapsed().as_millis() as f64);
                            log_reader.truncate(TruncateOffset::Barrier { epoch })?;

                            current_checkpoint = 0;
                            if let Some(new_vnode_bitmap) = new_vnode_bitmap {
                                coordinator_stream_handle
                                    .update_vnode_bitmap(&new_vnode_bitmap)
                                    .await?;
                            }
                            if is_stop {
                                coordinator_stream_handle.stop().await?;
                                info!(
                                    sink_id = self.param.sink_id.sink_id,
                                    "coordinated log sinker stops"
                                );
                                return pending().await;
                            }
                        } else {
                            let metadata = sink_writer.barrier(false).await?;
                            if let Some(metadata) = metadata {
                                warn!(?metadata, "get metadata on non-checkpoint barrier");
                            }
                        }
                    } else {
                        let metadata = sink_writer.barrier(false).await?;
                        if let Some(metadata) = metadata {
                            warn!(?metadata, "get metadata on non-checkpoint barrier");
                        }
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}
