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
/// When the file sink writer completes writing a file, it truncates the corresponding `chunk_id` in the log store.
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
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<!> {
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
            if let LogStoreReadItem::UpdateVnodeBitmap(_) = &item {
                match &state {
                    LogConsumerState::BarrierReceived { .. } => {
                        // we need to force to finish the batch here. Otherwise, there can be data loss because actor can be dropped and rebuilt during scaling.
                        if let Some(committed_chunk_id) = sink_writer.should_finish().await? {
                            log_reader.truncate(TruncateOffset::Chunk {
                                epoch: (epoch),
                                chunk_id: (committed_chunk_id),
                            })?
                        };
                    }
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
                    // sink_writer.begin_epoch(epoch).await?;
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
                    // sink_writer.begin_epoch(epoch).await?;
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
                        // The file has not been written into downstream file system.
                        Ok(None) => {}
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint: _ } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };

                    // The current sink specifies a batching strategy, which means that sink decoupling is enabled; therefore, there is no need to forcibly write to the file when the checkpoint barrier arrives.
                    // When the barrier arrives, call the writer's try_finish interface to check if the file write can be completed.
                    // If it is completed, which means the file is visible in the downstream file system, thentruncate the file in the log store; otherwise, do nothing.
                    if let Some(committed_chunk_id) = sink_writer.try_finish().await? {
                        log_reader.truncate(TruncateOffset::Chunk {
                            epoch: (epoch),
                            chunk_id: (committed_chunk_id),
                        })?
                    };

                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(_vnode_bitmap) => {
                    unreachable!("Update vnode bitmap should have been handle earlier.")
                }
            }
        }
    }
}
