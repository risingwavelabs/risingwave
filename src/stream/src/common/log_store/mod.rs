// Copyright 2023 RisingWave Labs
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

use std::fmt::Debug;
use std::future::Future;

use risingwave_common::array::StreamChunk;
use risingwave_common::util::epoch::INVALID_EPOCH;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::oneshot;

use crate::common::log_store::LogReaderEpochProgress::{AwaitingTruncate, Consuming};

#[derive(thiserror::Error, Debug)]
pub enum LogStoreError {
    #[error("EndOfLogStream")]
    EndOfLogStream,
}

pub type LogStoreResult<T> = Result<T, LogStoreError>;

#[derive(Debug)]
pub enum LogStoreReadItem {
    StreamChunk(StreamChunk),
    Barrier {
        next_epoch: u64,
        is_checkpoint: bool,
    },
}

pub trait LogWriter {
    type InitFuture<'a>: Future<Output = LogStoreResult<()>> + Send + 'a
    where
        Self: 'a;
    type WriteChunkFuture<'a>: Future<Output = LogStoreResult<()>> + Send + 'a
    where
        Self: 'a;
    type FlushCurrentEpoch<'a>: Future<Output = LogStoreResult<()>> + Send + 'a
    where
        Self: 'a;

    fn init(&mut self, epoch: u64) -> Self::InitFuture<'_>;
    fn write_chunk(&mut self, chunk: StreamChunk) -> Self::WriteChunkFuture<'_>;
    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> Self::FlushCurrentEpoch<'_>;
}

pub trait LogReader {
    type InitFuture<'a>: Future<Output = LogStoreResult<u64>> + Send + 'a
    where
        Self: 'a;
    type NextItemFuture<'a>: Future<Output = LogStoreResult<LogStoreReadItem>> + Send + 'a
    where
        Self: 'a;
    type TruncateFuture<'a>: Future<Output = LogStoreResult<()>> + Send + 'a
    where
        Self: 'a;

    fn init(&mut self) -> Self::InitFuture<'_>;
    fn next_item(&mut self) -> Self::NextItemFuture<'_>;
    fn truncate(&mut self) -> Self::TruncateFuture<'_>;
}

pub trait LogStoreFactory {
    type Reader: LogReader;
    type Writer: LogWriter;

    fn build(self) -> (Self::Reader, Self::Writer);
}

pub struct BoundedInMemLogStoreWriter {
    curr_epoch: Option<u64>,

    init_epoch_tx: Option<oneshot::Sender<u64>>,
    item_tx: Sender<LogStoreReadItem>,
    truncated_epoch_rx: UnboundedReceiver<u64>,
}

#[derive(Eq, PartialEq, Debug)]
enum LogReaderEpochProgress {
    Consuming(u64),
    AwaitingTruncate { sealed_epoch: u64, next_epoch: u64 },
}

const UNINITIALIZED: LogReaderEpochProgress = LogReaderEpochProgress::Consuming(INVALID_EPOCH);

pub struct BoundedInMemLogStoreReader {
    epoch_progress: LogReaderEpochProgress,

    init_epoch_rx: Option<oneshot::Receiver<u64>>,
    item_rx: Receiver<LogStoreReadItem>,
    truncated_epoch_tx: UnboundedSender<u64>,
}

pub struct BoundedInMemLogStoreFactory {
    bound: usize,
}

impl BoundedInMemLogStoreFactory {
    pub fn new(bound: usize) -> Self {
        Self { bound }
    }
}

impl LogStoreFactory for BoundedInMemLogStoreFactory {
    type Reader = BoundedInMemLogStoreReader;
    type Writer = BoundedInMemLogStoreWriter;

    fn build(self) -> (Self::Reader, Self::Writer) {
        let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
        let (item_tx, item_rx) = channel(self.bound);
        let (truncated_epoch_tx, truncated_epoch_rx) = unbounded_channel();
        let reader = BoundedInMemLogStoreReader {
            epoch_progress: UNINITIALIZED,
            init_epoch_rx: Some(init_epoch_rx),
            item_rx,
            truncated_epoch_tx,
        };
        let writer = BoundedInMemLogStoreWriter {
            curr_epoch: None,
            init_epoch_tx: Some(init_epoch_tx),
            item_tx,
            truncated_epoch_rx,
        };
        (reader, writer)
    }
}

impl LogReader for BoundedInMemLogStoreReader {
    type InitFuture<'a> = impl Future<Output = LogStoreResult<u64>> + 'a;
    type NextItemFuture<'a> = impl Future<Output = LogStoreResult<LogStoreReadItem>> + 'a;
    type TruncateFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self) -> Self::InitFuture<'_> {
        async {
            let init_epoch_rx = self
                .init_epoch_rx
                .take()
                .expect("should not init for twice");
            // TODO: should return the error
            let epoch = init_epoch_rx.await.expect("should be able to init");
            assert_eq!(self.epoch_progress, UNINITIALIZED);
            self.epoch_progress = LogReaderEpochProgress::Consuming(epoch);
            Ok(epoch)
        }
    }

    fn next_item(&mut self) -> Self::NextItemFuture<'_> {
        async {
            match self.item_rx.recv().await {
                Some(item) => {
                    if let LogStoreReadItem::Barrier {
                        next_epoch,
                        is_checkpoint,
                    } = &item
                    {
                        match self.epoch_progress {
                            LogReaderEpochProgress::Consuming(current_epoch) => {
                                if *is_checkpoint {
                                    self.epoch_progress = AwaitingTruncate {
                                        next_epoch: *next_epoch,
                                        sealed_epoch: current_epoch,
                                    };
                                } else {
                                    self.epoch_progress = Consuming(*next_epoch);
                                }
                            }
                            LogReaderEpochProgress::AwaitingTruncate { .. } => {
                                unreachable!("should not be awaiting for when barrier comes")
                            }
                        }
                    }
                    Ok(item)
                }
                None => Err(LogStoreError::EndOfLogStream),
            }
        }
    }

    fn truncate(&mut self) -> Self::TruncateFuture<'_> {
        async move {
            let sealed_epoch = match self.epoch_progress {
                Consuming(_) => unreachable!("should be awaiting truncate"),
                LogReaderEpochProgress::AwaitingTruncate {
                    sealed_epoch,
                    next_epoch,
                } => {
                    self.epoch_progress = Consuming(next_epoch);
                    sealed_epoch
                }
            };
            // TODO: should return error
            self.truncated_epoch_tx
                .send(sealed_epoch)
                .expect("should not error");
            Ok(())
        }
    }
}

impl LogWriter for BoundedInMemLogStoreWriter {
    type FlushCurrentEpoch<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type WriteChunkFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self, epoch: u64) -> Self::InitFuture<'_> {
        let init_epoch_tx = self.init_epoch_tx.take().expect("cannot be init for twice");
        // TODO: return the error
        init_epoch_tx.send(epoch).unwrap();
        self.curr_epoch = Some(epoch);
        async { Ok(()) }
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Self::WriteChunkFuture<'_> {
        async {
            // TODO: return the sender error
            self.item_tx
                .send(LogStoreReadItem::StreamChunk(chunk))
                .await
                .expect("should be able to send");
            Ok(())
        }
    }

    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> Self::FlushCurrentEpoch<'_> {
        async move {
            // TODO: return the sender error
            self.item_tx
                .send(LogStoreReadItem::Barrier {
                    next_epoch,
                    is_checkpoint,
                })
                .await
                .expect("should be able to send");

            let prev_epoch = self
                .curr_epoch
                .replace(next_epoch)
                .expect("should have epoch");

            if is_checkpoint {
                // TODO: return err at None
                let truncated_epoch = self.truncated_epoch_rx.recv().await.unwrap();
                assert_eq!(truncated_epoch, prev_epoch);
            }

            Ok(())
        }
    }
}
