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

use std::time::Instant;

use async_trait::async_trait;

use crate::sink::file_sink::opendal_sink::OpenDalSinkWriter;
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::writer::SinkWriter;
use crate::sink::{LogSinker, Result, SinkLogReader, SinkMetrics};
pub const DEFAULT_COMMIT_CHECKPOINT_INTERVAL: u64 = 10;

pub fn default_commit_checkpoint_interval() -> u64 {
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL
}

/// The `LogSinker` implementation used for commit-decoupled sinks (such as `Iceberg`, `DeltaLake` and `StarRocks`).
/// The concurrent/frequent commit capability of these sinks is poor, so by leveraging the decoupled log reader,
/// we delay the checkpoint barrier to make commits less frequent.
pub struct BatchingLogSinkerOf {
    writer: OpenDalSinkWriter,
    no_batching_strategy_defined: bool,
    sink_metrics: SinkMetrics,
}

impl BatchingLogSinkerOf {
    /// Create a log sinker with a commit checkpoint interval. The sinker should be used with a
    /// decouple log reader `KvLogStoreReader`.
    pub fn new(
        writer: OpenDalSinkWriter,
        no_batching_strategy_defined: bool,
        sink_metrics: SinkMetrics,
    ) -> Self {
        BatchingLogSinkerOf {
            writer,
            no_batching_strategy_defined,
            sink_metrics,
        }
    }
}

#[async_trait]
impl LogSinker for BatchingLogSinkerOf {
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<!> {
        let mut sink_writer = self.writer;
        let sink_metrics = self.sink_metrics;
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
        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            if let LogStoreReadItem::UpdateVnodeBitmap(_) = &item {
                match &state {
                    LogConsumerState::BarrierReceived { .. } => {}
                    _ => unreachable!(
                        "update vnode bitmap can be accepted only right after \
                    barrier, but current state is {:?}",
                        state
                    ),
                }
            }
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
                LogConsumerState::BarrierReceived { prev_epoch } => {
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
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    match sink_writer
                        .write_batch_and_try_finish(chunk, chunk_id)
                        .await
                    {
                        Err(e) => {
                            sink_writer.abort().await?;
                            return Err(e);
                        }
                        // The file has been successfully written and is now visible to downstream consumers.
                        // Truncate the file to remove the specified `chunk_id` and any preceding content.
                        Ok(Some(chunk_id)) => {
                            log_reader.truncate(TruncateOffset::Chunk {
                                epoch: (epoch),
                                chunk_id: (chunk_id),
                            })?;
                        }
                        // The file has not been  written into downstream file system.
                        Ok(None) => {}
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };

                    match self.no_batching_strategy_defined {
                        true => {
                            // // The current sink does not specifies a batching strategy, so it will force finish write files when the checkpoint barrier arrives.
                            if is_checkpoint {
                                {
                                    let start_time = Instant::now();
                                    sink_writer.barrier(true).await?;
                                    sink_metrics
                                        .sink_commit_duration_metrics
                                        .observe(start_time.elapsed().as_millis() as f64);
                                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                                }
                            } else {
                                sink_writer.barrier(false).await?;
                            }
                        }
                        false => {
                            // The current sink specifies a batching strategy, which means that sink decoupling is enabled; therefore, there is no need to forcibly write to the file when the checkpoint barrier arrives.
                            // When the barrier arrives, call the writer's try_finish interface to check if the file write can be completed.
                            // If it is completed, which means the file is visible in the downstream file system, thentruncate the file in the log store; otherwise, do nothing.
                            if let Some(committed_chunk_id) = sink_writer.try_finish().await? {
                                log_reader.truncate(TruncateOffset::Chunk {
                                    epoch: (epoch),
                                    chunk_id: (committed_chunk_id),
                                })?
                            };
                        }
                    };

                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(vnode_bitmap) => {
                    sink_writer.update_vnode_bitmap(vnode_bitmap).await?;
                }
            }
        }
    }
}
