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

            /// Mark that a new epoch has begun, and store the max_uncommitted_epoch for cross-barrier batching.
            /// For example, suppose the current order is (chunk1, barrier1, chunk2, barrier2, chunk3), and the batching is not completed until chunk3,
            /// that is, barrier2 and its previous chunks are not truncated, the `max_uncommitted_epoch` is barrier2.
            /// When we truncate chunk3, we should first truncate barrier2, and then truncate chunk3.
            EpochBegun {
                curr_epoch: u64,
                max_uncommitted_epoch: Option<u64>,
            },

            /// Mark that the consumer has just received a barrier
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;
        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => LogConsumerState::EpochBegun {
                    curr_epoch: epoch,
                    max_uncommitted_epoch: None,
                },
                LogConsumerState::EpochBegun {
                    curr_epoch,
                    max_uncommitted_epoch,
                } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun {
                        curr_epoch: epoch,
                        max_uncommitted_epoch,
                    }
                }
                LogConsumerState::BarrierReceived { prev_epoch } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );
                    LogConsumerState::EpochBegun {
                        curr_epoch: epoch,
                        max_uncommitted_epoch: Some(prev_epoch),
                    }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    sink_writer.write_batch(chunk).await?;
                    match sink_writer.try_commit().await {
                        Err(e) => {
                            return Err(e);
                        }
                        // The file has been successfully written and is now visible to downstream consumers.
                        // Truncate the file to remove the specified `chunk_id` and any preceding content.
                        Ok(true) => {
                            // If epoch increased, we first need to truncate the previous epoch.
                            if let Some(max_uncommitted_epoch) = match state {
                                LogConsumerState::EpochBegun {
                                    curr_epoch: _,
                                    max_uncommitted_epoch,
                                } => max_uncommitted_epoch,
                                _ => unreachable!("epoch must have begun before handling barrier"),
                            } {
                                assert!(epoch > max_uncommitted_epoch);
                                log_reader.truncate(TruncateOffset::Barrier {
                                    epoch: max_uncommitted_epoch,
                                })?;
                                state = LogConsumerState::EpochBegun {
                                    curr_epoch: epoch,
                                    max_uncommitted_epoch: None,
                                }
                            };

                            log_reader.truncate(TruncateOffset::Chunk {
                                epoch: (epoch),
                                chunk_id: (chunk_id),
                            })?;
                        }
                        // The file has not been written into downstream file system.
                        Ok(false) => {}
                    }
                }
                LogStoreReadItem::Barrier { .. } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun {
                            curr_epoch,
                            max_uncommitted_epoch: _,
                        } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };

                    // When the barrier arrives, call the writer's try_finish interface to check if the file write can be completed.
                    // If it is completed, which means the file is visible in the downstream file system, then truncate the file in the log store; otherwise, do nothing.
                    // Since the current data must be before the current epoch, we only need to truncate `prev_epoch`.
                    if sink_writer.try_commit().await? {
                        log_reader.truncate(TruncateOffset::Barrier { epoch: prev_epoch })?;
                    };

                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}
