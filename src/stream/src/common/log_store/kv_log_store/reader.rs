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
use std::ops::Bound::{Excluded, Included};
use std::pin::Pin;

use bytes::Bytes;
use futures::future::try_join_all;
use futures::stream::select_all;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::{PrefetchOptions, ReadOptions};
use risingwave_storage::StateStore;
use tokio_stream::StreamExt;

use crate::common::log_store::kv_log_store::buffer::{LogStoreBufferItem, LogStoreBufferReceiver};
use crate::common::log_store::kv_log_store::serde::{
    new_log_store_item_stream, LogStoreItemStream, LogStoreRowSerde,
};
use crate::common::log_store::{LogReader, LogStoreError, LogStoreReadItem, LogStoreResult};

pub struct KvLogStoreReader<S: StateStore> {
    table_id: TableId,

    state_store: S,

    serde: LogStoreRowSerde,

    rx: LogStoreBufferReceiver,

    reader_state: ReaderState<S>,
}

enum ReaderState<S: StateStore> {
    Uninitialized,
    ConsumingStateStore {
        first_write_epoch: u64,
        state_store_stream: Pin<Box<LogStoreItemStream<S::IterStream>>>,
    },
    ConsumingStream {
        epoch: u64,
    },
}

impl<S: StateStore> KvLogStoreReader<S> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: S,
        serde: LogStoreRowSerde,
        rx: LogStoreBufferReceiver,
    ) -> Self {
        Self {
            table_id,
            state_store,
            reader_state: ReaderState::Uninitialized,
            serde,
            rx,
        }
    }
}

impl<S: StateStore> LogReader for KvLogStoreReader<S> {
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type NextItemFuture<'a> = impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + 'a;
    type TruncateFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self) -> Self::InitFuture<'_> {
        async move {
            let first_write_epoch = self.rx.init().await;
            let streams = try_join_all(self.serde.vnodes().iter_vnodes().map(|vnode| {
                let range_start = Bytes::from(Vec::from(vnode.to_be_bytes()));
                let range_end = self.serde.serialize_epoch(vnode, first_write_epoch);
                let table_id = self.table_id;
                let state_store = self.state_store.clone();
                async move {
                    state_store
                        .iter(
                            (Included(range_start), Excluded(range_end)),
                            first_write_epoch,
                            ReadOptions {
                                prefix_hint: None,
                                ignore_range_tombstone: false,
                                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                                cache_policy: CachePolicy::NotFill,
                                retention_seconds: None,
                                table_id,
                                read_version_from_backup: false,
                            },
                        )
                        .await
                }
            }))
            .await?;
            // TODO: set chunk size by config
            let state_store_stream =
                Box::pin(new_log_store_item_stream(streams, self.serde.clone(), 1024));
            self.reader_state = ReaderState::ConsumingStateStore {
                first_write_epoch,
                state_store_stream,
            };
            Ok(())
        }
    }

    fn next_item(&mut self) -> Self::NextItemFuture<'_> {
        async move {
            let epoch = match &mut self.reader_state {
                ReaderState::Uninitialized => unreachable!("should be initialized"),
                ReaderState::ConsumingStateStore {
                    first_write_epoch,
                    state_store_stream,
                } => {
                    match state_store_stream.try_next().await? {
                        Some((epoch, item)) => return Ok((epoch, item)),
                        None => {
                            let first_write_epoch = *first_write_epoch;
                            // all consumed
                            self.reader_state = ReaderState::ConsumingStream {
                                epoch: first_write_epoch,
                            };
                            first_write_epoch
                        }
                    }
                }
                ReaderState::ConsumingStream { epoch } => *epoch,
            };
            loop {
                match self.rx.next_item().await {
                    LogStoreBufferItem::StreamChunk(chunk) => {
                        return Ok((epoch, LogStoreReadItem::StreamChunk(chunk)));
                    }
                    LogStoreBufferItem::Flushed {
                        vnode_bitmap,
                        start_seq_id,
                        end_seq_id,
                    } => {
                        let streams = try_join_all(vnode_bitmap.iter_vnodes().map(|vnode| {
                            let range_start =
                                self.serde
                                    .serialize_log_store_pk(vnode, epoch, start_seq_id);
                            let range_end =
                                self.serde.serialize_log_store_pk(vnode, epoch, end_seq_id);
                            let state_store = self.state_store.clone();
                            let table_id = self.table_id;
                            // Use u64::MAX here because the epoch to consume may be below the safe
                            // epoch
                            async move {
                                Ok::<_, LogStoreError>(Box::pin(
                                    state_store
                                        .iter(
                                            (Included(range_start), Included(range_end)),
                                            u64::MAX,
                                            ReadOptions {
                                                prefix_hint: None,
                                                ignore_range_tombstone: false,
                                                prefetch_options:
                                                    PrefetchOptions::new_for_exhaust_iter(),
                                                cache_policy: CachePolicy::NotFill,
                                                retention_seconds: None,
                                                table_id,
                                                read_version_from_backup: false,
                                            },
                                        )
                                        .await?,
                                ))
                            }
                        }))
                        .await?;
                        let combined_stream = select_all(streams);
                        let stream_chunk = self
                            .serde
                            .deserialize_stream_chunk(
                                combined_stream,
                                start_seq_id,
                                end_seq_id,
                                epoch,
                            )
                            .await?;
                        return Ok((epoch, LogStoreReadItem::StreamChunk(stream_chunk)));
                    }
                    LogStoreBufferItem::Barrier {
                        is_checkpoint,
                        next_epoch,
                    } => {
                        self.reader_state = ReaderState::ConsumingStream { epoch: next_epoch };
                        return Ok((epoch, LogStoreReadItem::Barrier { is_checkpoint }));
                    }
                    LogStoreBufferItem::UpdateVnodes(bitmap) => {
                        self.serde.update_vnode_bitmap(bitmap);
                        continue;
                    }
                }
            }
        }
    }

    fn truncate(&mut self) -> Self::TruncateFuture<'_> {
        async move { todo!() }
    }
}
