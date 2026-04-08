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

use async_trait::async_trait;

use crate::sink::file_sink::opendal_sink::OpenDalSinkWriter;
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{LogSinker, Result, SinkLogReader};

/// `BatchingLogSinker` is used for a commit-decoupled sink that supports cross-barrier batching.
/// Currently, it is only used for file sinks, so it contains an `OpenDalSinkWriter`.
pub struct BatchingLogSinker {
    writer: OpenDalSinkWriter,
}

impl BatchingLogSinker {
    /// Create a log sinker with a file sink writer.
    pub fn new(writer: OpenDalSinkWriter) -> Self {
        BatchingLogSinker { writer }
    }
}

#[async_trait]
impl LogSinker for BatchingLogSinker {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        let mut sink_writer = self.writer;
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
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => {
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
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    sink_writer.write_batch(chunk).await?;
                    if sink_writer.try_commit().await? {
                        // The file has been successfully written and is now visible to downstream consumers.
                        // Truncate up to this chunk, which also covers any preceding barriers.
                        log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                    }
                }
                LogStoreReadItem::Barrier { .. } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };

                    // Truncate the barrier if either:
                    // 1. try_commit succeeded (file was written and is now visible), or
                    // 2. there is no pending data (no active writer), so the barrier can be safely discarded.
                    // This avoids accumulating barriers in the log store during idle periods with no data.
                    if sink_writer.try_commit().await? || !sink_writer.has_pending_data() {
                        log_reader.truncate(TruncateOffset::Barrier { epoch: prev_epoch })?;
                    }

                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}
