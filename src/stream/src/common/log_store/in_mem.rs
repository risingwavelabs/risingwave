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

use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::util::epoch::INVALID_EPOCH;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::oneshot;

use crate::common::log_store::in_mem::LogReaderEpochProgress::{AwaitingTruncate, Consuming};
use crate::common::log_store::{
    LogReader, LogStoreError, LogStoreFactory, LogStoreReadItem, LogStoreResult, LogWriter,
};

enum InMemLogStoreItem {
    StreamChunk(StreamChunk),
    Barrier {
        next_epoch: u64,
        is_checkpoint: bool,
    },
}

/// An in-memory log store that can buffer a bounded amount of stream chunk in memory via bounded
/// mpsc channel.
///
/// Since it is in-memory, when `flush_current_epoch` with checkpoint epoch, it should wait for the
/// reader to finish consuming all the data in current checkpoint epoch.
pub struct BoundedInMemLogStoreWriter {
    /// Current epoch. Should be `Some` after `init`
    curr_epoch: Option<u64>,

    /// Holder of oneshot channel to send the initial epoch to the associated log reader.
    init_epoch_tx: Option<oneshot::Sender<u64>>,

    /// Sending log store item to log reader
    item_tx: Sender<InMemLogStoreItem>,

    /// Receiver for the epoch consumed by log reader.
    truncated_epoch_rx: UnboundedReceiver<u64>,
}

#[derive(Eq, PartialEq, Debug)]
enum LogReaderEpochProgress {
    /// In progress of consuming data in current epoch.
    Consuming(u64),
    /// Finished emitting the data in checkpoint epoch, and waiting for a call on `truncate`.
    AwaitingTruncate { sealed_epoch: u64, next_epoch: u64 },
}

const UNINITIALIZED: LogReaderEpochProgress = LogReaderEpochProgress::Consuming(INVALID_EPOCH);

pub struct BoundedInMemLogStoreReader {
    /// Current progress of log reader. Can be either consuming an epoch, or has finished consuming
    /// an epoch and waiting to be truncated.
    epoch_progress: LogReaderEpochProgress,

    /// Holder for oneshot channel to receive the initial epoch
    init_epoch_rx: Option<oneshot::Receiver<u64>>,

    /// Receiver to fetch log store item
    item_rx: Receiver<InMemLogStoreItem>,

    /// Sender of consumed epoch to the log writer
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

    type BuildFuture = impl Future<Output = (Self::Reader, Self::Writer)>;

    fn build(self) -> Self::BuildFuture {
        async move {
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
}

impl LogReader for BoundedInMemLogStoreReader {
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type NextItemFuture<'a> = impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + 'a;
    type TruncateFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self) -> Self::InitFuture<'_> {
        async {
            let init_epoch_rx = self
                .init_epoch_rx
                .take()
                .expect("should not init for twice");
            let epoch = init_epoch_rx
                .await
                .map_err(|e| anyhow!("unable to get init epoch: {:?}", e))?;
            assert_eq!(self.epoch_progress, UNINITIALIZED);
            self.epoch_progress = LogReaderEpochProgress::Consuming(epoch);
            Ok(())
        }
    }

    fn next_item(&mut self) -> Self::NextItemFuture<'_> {
        async {
            match self.item_rx.recv().await {
                Some(item) => match self.epoch_progress {
                    Consuming(current_epoch) => match item {
                        InMemLogStoreItem::StreamChunk(chunk) => {
                            Ok((current_epoch, LogStoreReadItem::StreamChunk(chunk)))
                        }
                        InMemLogStoreItem::Barrier {
                            is_checkpoint,
                            next_epoch,
                        } => {
                            if is_checkpoint {
                                self.epoch_progress = AwaitingTruncate {
                                    next_epoch,
                                    sealed_epoch: current_epoch,
                                };
                            } else {
                                self.epoch_progress = Consuming(next_epoch);
                            }
                            Ok((current_epoch, LogStoreReadItem::Barrier { is_checkpoint }))
                        }
                    },
                    AwaitingTruncate { .. } => {
                        unreachable!("should not be awaiting for when barrier comes")
                    }
                },
                None => Err(LogStoreError::EndOfLogStream),
            }
        }
    }

    fn truncate(&mut self) -> Self::TruncateFuture<'_> {
        async move {
            let sealed_epoch = match self.epoch_progress {
                Consuming(_) => unreachable!("should be awaiting truncate"),
                AwaitingTruncate {
                    sealed_epoch,
                    next_epoch,
                } => {
                    self.epoch_progress = Consuming(next_epoch);
                    sealed_epoch
                }
            };
            self.truncated_epoch_tx
                .send(sealed_epoch)
                .map_err(|_| anyhow!("unable to send sealed epoch"))?;
            Ok(())
        }
    }
}

impl LogWriter for BoundedInMemLogStoreWriter {
    type FlushCurrentEpoch<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type WriteChunkFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self, epoch: u64) -> Self::InitFuture<'_> {
        async move {
            let init_epoch_tx = self.init_epoch_tx.take().expect("cannot be init for twice");
            init_epoch_tx
                .send(epoch)
                .map_err(|_| anyhow!("unable to send init epoch"))?;
            self.curr_epoch = Some(epoch);
            Ok(())
        }
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Self::WriteChunkFuture<'_> {
        async {
            self.item_tx
                .send(InMemLogStoreItem::StreamChunk(chunk))
                .await
                .map_err(|_| anyhow!("unable to send stream chunk"))?;
            Ok(())
        }
    }

    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> Self::FlushCurrentEpoch<'_> {
        async move {
            self.item_tx
                .send(InMemLogStoreItem::Barrier {
                    next_epoch,
                    is_checkpoint,
                })
                .await
                .map_err(|_| anyhow!("unable to send barrier"))?;

            let prev_epoch = self
                .curr_epoch
                .replace(next_epoch)
                .expect("should have epoch");

            if is_checkpoint {
                let truncated_epoch = self
                    .truncated_epoch_rx
                    .recv()
                    .await
                    .ok_or(anyhow!("cannot get truncated epoch"))?;
                assert_eq!(truncated_epoch, prev_epoch);
            }

            Ok(())
        }
    }

    fn update_vnode_bitmap(&mut self, _new_vnodes: Arc<Bitmap>) {
        // Since this is in memory, we don't need to handle the vnode bitmap
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

    use crate::common::log_store::in_mem::BoundedInMemLogStoreFactory;
    use crate::common::log_store::{LogReader, LogStoreFactory, LogStoreReadItem, LogWriter};

    #[tokio::test]
    async fn test_in_memory_log_store() {
        let factory = BoundedInMemLogStoreFactory::new(4);
        let (mut reader, mut writer) = factory.build().await;

        let init_epoch = 233;
        let epoch1 = init_epoch + 1;
        let epoch2 = init_epoch + 2;

        let ops = vec![Op::Insert, Op::Delete, Op::UpdateInsert, Op::UpdateDelete];
        let mut builder = DataChunkBuilder::new(vec![DataType::Int64, DataType::Varchar], 10000);
        for i in 0..ops.len() {
            assert!(builder
                .append_one_row(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(i as i64)),
                    Some(ScalarImpl::Utf8(format!("name_{}", i).into_boxed_str()))
                ]))
                .is_none());
        }
        let data_chunk = builder.consume_all().unwrap();
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
        let stream_chunk_clone = stream_chunk.clone();

        let join_handle = tokio::spawn(async move {
            writer.init(init_epoch).await.unwrap();
            writer
                .write_chunk(stream_chunk_clone.clone())
                .await
                .unwrap();
            writer.flush_current_epoch(epoch1, false).await.unwrap();
            writer.write_chunk(stream_chunk_clone).await.unwrap();
            writer.flush_current_epoch(epoch2, true).await.unwrap();
        });

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(chunk)) => {
                assert_eq!(epoch, init_epoch);
                assert_eq!(&chunk, &stream_chunk);
            }
            _ => unreachable!(),
        }

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert!(!is_checkpoint);
                assert_eq!(epoch, init_epoch);
            }
            _ => unreachable!(),
        }

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(chunk)) => {
                assert_eq!(&chunk, &stream_chunk);
                assert_eq!(epoch, epoch1);
            }
            _ => unreachable!(),
        }

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert!(is_checkpoint);
                assert_eq!(epoch, epoch1);
            }
            _ => unreachable!(),
        }

        reader.truncate().await.unwrap();
        join_handle.await.unwrap();
    }
}
