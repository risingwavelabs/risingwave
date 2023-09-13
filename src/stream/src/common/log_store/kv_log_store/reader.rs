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

use std::ops::Bound::{Excluded, Included};
use std::pin::Pin;

use anyhow::anyhow;
use bytes::Bytes;
use futures::future::try_join_all;
use futures::stream::select_all;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::{PrefetchOptions, ReadOptions};
use risingwave_storage::StateStore;
use tokio_stream::StreamExt;

use crate::common::log_store::kv_log_store::buffer::{LogStoreBufferItem, LogStoreBufferReceiver};
use crate::common::log_store::kv_log_store::serde::{
    new_log_store_item_stream, KvLogStoreItem, LogStoreItemStream, LogStoreRowSerde,
};
use crate::common::log_store::{
    LogReader, LogStoreError, LogStoreReadItem, LogStoreResult, TruncateOffset,
};

pub struct KvLogStoreReader<S: StateStore> {
    table_id: TableId,

    state_store: S,

    serde: LogStoreRowSerde,

    rx: LogStoreBufferReceiver,

    first_write_epoch: Option<u64>,

    state_store_stream: Option<Pin<Box<LogStoreItemStream<S::IterStream>>>>,

    latest_offset: TruncateOffset,

    truncate_offset: TruncateOffset,
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
            serde,
            rx,
            first_write_epoch: None,
            state_store_stream: None,
            latest_offset: TruncateOffset::Barrier { epoch: 0 },
            truncate_offset: TruncateOffset::Barrier { epoch: 0 },
        }
    }
}

impl<S: StateStore> LogReader for KvLogStoreReader<S> {
    async fn init(&mut self) -> LogStoreResult<()> {
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
                        u64::MAX,
                        ReadOptions {
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::Low),
                            table_id,
                            ..Default::default()
                        },
                    )
                    .await
            }
        }))
        .await?;

        assert!(
            self.first_write_epoch.replace(first_write_epoch).is_none(),
            "should not init twice"
        );
        // TODO: set chunk size by config
        self.state_store_stream = Some(Box::pin(new_log_store_item_stream(
            streams,
            self.serde.clone(),
            1024,
        )));
        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        if let Some(state_store_stream) = &mut self.state_store_stream {
            match state_store_stream.try_next().await? {
                Some((epoch, item)) => {
                    self.latest_offset.check_next_item_epoch(epoch)?;
                    let item = match item {
                        KvLogStoreItem::StreamChunk(chunk) => {
                            let chunk_id = self.latest_offset.next_chunk_id();
                            self.latest_offset = TruncateOffset::Chunk { epoch, chunk_id };
                            LogStoreReadItem::StreamChunk { chunk, chunk_id }
                        }
                        KvLogStoreItem::Barrier { is_checkpoint } => {
                            self.latest_offset = TruncateOffset::Barrier { epoch };
                            LogStoreReadItem::Barrier { is_checkpoint }
                        }
                    };
                    return Ok((epoch, item));
                }
                None => {
                    self.state_store_stream = None;
                }
            }
        }

        // Now the historical state store has been consumed.
        let (item_epoch, item) = self.rx.next_item().await;
        self.latest_offset.check_next_item_epoch(item_epoch)?;
        Ok(match item {
            LogStoreBufferItem::StreamChunk {
                chunk, chunk_id, ..
            } => {
                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                assert!(offset > self.latest_offset);
                self.latest_offset = offset;
                (
                    item_epoch,
                    LogStoreReadItem::StreamChunk { chunk, chunk_id },
                )
            }
            LogStoreBufferItem::Flushed {
                vnode_bitmap,
                start_seq_id,
                end_seq_id,
                chunk_id,
            } => {
                let streams = try_join_all(vnode_bitmap.iter_vnodes().map(|vnode| {
                    let range_start =
                        self.serde
                            .serialize_log_store_pk(vnode, item_epoch, Some(start_seq_id));
                    let range_end =
                        self.serde
                            .serialize_log_store_pk(vnode, item_epoch, Some(end_seq_id));
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
                                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                                        cache_policy: CachePolicy::Fill(CachePriority::Low),
                                        table_id,
                                        ..Default::default()
                                    },
                                )
                                .await?,
                        ))
                    }
                }))
                .await?;
                let combined_stream = select_all(streams);
                let chunk = self
                    .serde
                    .deserialize_stream_chunk(combined_stream, start_seq_id, end_seq_id, item_epoch)
                    .await?;
                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                assert!(offset > self.latest_offset);
                self.latest_offset = offset;
                (
                    item_epoch,
                    LogStoreReadItem::StreamChunk { chunk, chunk_id },
                )
            }
            LogStoreBufferItem::Barrier {
                is_checkpoint,
                next_epoch,
            } => {
                assert!(
                    item_epoch < next_epoch,
                    "next epoch {} should be greater than current epoch {}",
                    next_epoch,
                    item_epoch
                );
                self.latest_offset = TruncateOffset::Barrier { epoch: item_epoch };
                (item_epoch, LogStoreReadItem::Barrier { is_checkpoint })
            }
            LogStoreBufferItem::UpdateVnodes(bitmap) => {
                self.serde.update_vnode_bitmap(bitmap.clone());
                (item_epoch, LogStoreReadItem::UpdateVnodeBitmap(bitmap))
            }
        })
    }

    async fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        if offset <= self.truncate_offset {
            return Err(anyhow!(
                "truncate offset {:?} earlier than prev truncate offset {:?}",
                offset,
                self.truncate_offset
            )
            .into());
        }
        if offset.epoch() >= self.first_write_epoch.expect("should have init") {
            self.rx.truncate(offset);
        } else {
            // For historical data, no need to truncate at seq id level. Only truncate at barrier.
            if let TruncateOffset::Barrier { .. } = &offset {
                self.rx.truncate(offset);
            }
        }
        self.truncate_offset = offset;
        Ok(())
    }
}
