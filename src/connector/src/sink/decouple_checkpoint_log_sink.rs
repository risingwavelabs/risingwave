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

use std::num::NonZeroU64;
use std::time::Instant;

use async_trait::async_trait;

use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::writer::SinkWriter;
use crate::sink::{LogSinker, Result, SinkLogReader, SinkWriterMetrics};

pub const DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE: u64 = 10;
pub const DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITHOUT_SINK_DECOUPLE: u64 = 1;
pub const COMMIT_CHECKPOINT_INTERVAL: &str = "commit_checkpoint_interval";

pub fn default_commit_checkpoint_interval() -> u64 {
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE
}

/// The `LogSinker` implementation used for commit-decoupled sinks (such as `Iceberg`, `DeltaLake` and `StarRocks`).
/// The concurrent/frequent commit capability of these sinks is poor, so by leveraging the decoupled log reader,
/// we delay the checkpoint barrier to make commits less frequent.
pub struct DecoupleCheckpointLogSinkerOf<W> {
    writer: W,
    sink_writer_metrics: SinkWriterMetrics,
    commit_checkpoint_interval: NonZeroU64,
}

impl<W> DecoupleCheckpointLogSinkerOf<W> {
    /// Create a log sinker with a commit checkpoint interval. The sinker should be used with a
    /// decouple log reader `KvLogStoreReader`.
    pub fn new(
        writer: W,
        sink_writer_metrics: SinkWriterMetrics,
        commit_checkpoint_interval: NonZeroU64,
    ) -> Self {
        DecoupleCheckpointLogSinkerOf {
            writer,
            sink_writer_metrics,
            commit_checkpoint_interval,
        }
    }
}

#[async_trait]
impl<W: SinkWriter<CommitMetadata = ()>> LogSinker for DecoupleCheckpointLogSinkerOf<W> {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        let mut sink_writer = self.writer;
        log_reader.start_from(None).await?;
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
                    ..
                } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        current_checkpoint += 1;
                        if current_checkpoint >= commit_checkpoint_interval.get()
                            || new_vnode_bitmap.is_some()
                        {
                            let start_time = Instant::now();
                            sink_writer.barrier(true).await?;
                            sink_writer_metrics
                                .sink_commit_duration
                                .observe(start_time.elapsed().as_millis() as f64);
                            log_reader.truncate(TruncateOffset::Barrier { epoch })?;

                            current_checkpoint = 0;
                        } else {
                            sink_writer.barrier(false).await?;
                        }
                    } else {
                        assert!(new_vnode_bitmap.is_none());
                        sink_writer.barrier(false).await?;
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}
