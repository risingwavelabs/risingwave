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

use std::future::Future;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use foyer::CacheHint;
use futures::future::{try_join_all, BoxFuture};
use futures::{FutureExt, TryFutureExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common::util::epoch::EpochExt;
use risingwave_connector::sink::log_store::{
    ChunkId, LogReader, LogStoreReadItem, LogStoreResult, TruncateOffset,
};
use risingwave_hummock_sdk::key::{prefixed_range_with_vnode, FullKey, TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::{
    PrefetchOptions, ReadOptions, StateStoreIterItemRef, StateStoreRead,
};
use risingwave_storage::{StateStore, StateStoreIter};
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_stream::StreamExt;

use crate::common::log_store_impl::kv_log_store::buffer::{
    LogStoreBufferItem, LogStoreBufferReceiver,
};
use crate::common::log_store_impl::kv_log_store::serde::{
    merge_log_store_item_stream, KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::KvLogStoreMetrics;

pub(crate) const REWIND_BASE_DELAY: Duration = Duration::from_secs(1);
pub(crate) const REWIND_BACKOFF_FACTOR: u64 = 2;
pub(crate) const REWIND_MAX_DELAY: Duration = Duration::from_secs(180);

mod rewind_backoff_policy {
    use std::time::Duration;

    use crate::common::log_store_impl::kv_log_store::{
        REWIND_BACKOFF_FACTOR, REWIND_BASE_DELAY, REWIND_MAX_DELAY,
    };

    pub(super) type RewindBackoffPolicy = impl Iterator<Item = Duration>;
    pub(super) fn initial_rewind_backoff_policy() -> RewindBackoffPolicy {
        tokio_retry::strategy::ExponentialBackoff::from_millis(REWIND_BASE_DELAY.as_millis() as _)
            .factor(REWIND_BACKOFF_FACTOR)
            .max_delay(REWIND_MAX_DELAY)
            .map(tokio_retry::strategy::jitter)
    }
}

use rewind_backoff_policy::*;

struct RewindDelay {
    last_rewind_truncate_offset: Option<TruncateOffset>,
    backoff_policy: RewindBackoffPolicy,
    rewind_count: LabelGuardedIntCounter<4>,
    rewind_delay: LabelGuardedHistogram<4>,
}

impl RewindDelay {
    fn new(metrics: &KvLogStoreMetrics) -> Self {
        Self {
            last_rewind_truncate_offset: None,
            backoff_policy: initial_rewind_backoff_policy(),
            rewind_count: metrics.rewind_count.clone(),
            rewind_delay: metrics.rewind_delay.clone(),
        }
    }

    async fn rewind_delay(&mut self, truncate_offset: Option<TruncateOffset>) {
        match (&self.last_rewind_truncate_offset, &truncate_offset) {
            (Some(prev_rewind_truncate_offset), Some(truncate_offset)) => {
                if truncate_offset > prev_rewind_truncate_offset {
                    self.last_rewind_truncate_offset = Some(*truncate_offset);
                    // Have new truncate progress before this round of rewind.
                    // Reset rewind backoff
                    self.backoff_policy = initial_rewind_backoff_policy();
                }
            }
            (None, _) => {
                self.last_rewind_truncate_offset = truncate_offset;
            }
            _ => {}
        };
        self.rewind_count.inc();
        if let Some(delay) = self.backoff_policy.next() {
            self.rewind_delay.observe(delay.as_secs_f64());
            sleep(delay).await;
        }
    }
}

pub struct KvLogStoreReader<S: StateStore> {
    table_id: TableId,

    state_store: S,

    serde: LogStoreRowSerde,

    rx: LogStoreBufferReceiver,

    /// The first epoch that newly written by the log writer
    first_write_epoch: Option<u64>,

    /// `Some` means consuming historical log data
    state_store_stream: Option<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>,

    /// Store the future that attempts to read a flushed stream chunk.
    /// This is for cancellation safety. Since it is possible that the future of `next_item`
    /// gets dropped after it takes an flushed item out from the buffer, but before it successfully
    /// read the stream chunk out from the storage. Therefore we store the future so that it can continue
    /// reading the stream chunk after the next `next_item` is called.
    read_flushed_chunk_future:
        Option<BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, u64)>>>,

    latest_offset: Option<TruncateOffset>,

    truncate_offset: Option<TruncateOffset>,

    metrics: KvLogStoreMetrics,

    is_paused: watch::Receiver<bool>,

    identity: String,

    rewind_delay: RewindDelay,
}

impl<S: StateStore> KvLogStoreReader<S> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: S,
        serde: LogStoreRowSerde,
        rx: LogStoreBufferReceiver,
        metrics: KvLogStoreMetrics,
        is_paused: watch::Receiver<bool>,
        identity: String,
    ) -> Self {
        let rewind_delay = RewindDelay::new(&metrics);
        Self {
            table_id,
            state_store,
            serde,
            rx,
            read_flushed_chunk_future: None,
            first_write_epoch: None,
            state_store_stream: None,
            latest_offset: None,
            truncate_offset: None,
            metrics,
            is_paused,
            identity,
            rewind_delay,
        }
    }

    async fn may_continue_read_flushed_chunk(
        &mut self,
    ) -> LogStoreResult<Option<(ChunkId, StreamChunk, u64)>> {
        if let Some(future) = self.read_flushed_chunk_future.as_mut() {
            let result = future.instrument_await("Read Flushed Chunk").await;
            let _fut = self
                .read_flushed_chunk_future
                .take()
                .expect("future not None");
            Ok(Some(result?))
        } else {
            Ok(None)
        }
    }
}

struct AutoRebuildStateStoreReadIter<S: StateStoreRead, F> {
    state_store: S,
    iter: S::Iter,
    // call to get whether to rebuild the iter. Once return true, the closure should reset itself.
    should_rebuild: F,
    end_bound: Bound<TableKey<Bytes>>,
    epoch: HummockEpoch,
    options: ReadOptions,
}

impl<S: StateStoreRead, F: FnMut() -> bool> AutoRebuildStateStoreReadIter<S, F> {
    async fn new(
        state_store: S,
        should_rebuild: F,
        range: TableKeyRange,
        epoch: HummockEpoch,
        options: ReadOptions,
    ) -> StorageResult<Self> {
        let (start_bound, end_bound) = range;
        let iter = state_store
            .iter((start_bound, end_bound.clone()), epoch, options.clone())
            .await?;
        Ok(Self {
            state_store,
            iter,
            should_rebuild,
            end_bound,
            epoch,
            options,
        })
    }
}

mod timeout_auto_rebuild {
    use std::time::{Duration, Instant};

    use risingwave_hummock_sdk::key::TableKeyRange;
    use risingwave_hummock_sdk::HummockEpoch;
    use risingwave_storage::error::StorageResult;
    use risingwave_storage::store::{ReadOptions, StateStoreRead};

    use crate::common::log_store_impl::kv_log_store::reader::AutoRebuildStateStoreReadIter;

    pub(super) type TimeoutAutoRebuildIter<S: StateStoreRead> =
        AutoRebuildStateStoreReadIter<S, impl FnMut() -> bool + Send>;

    pub(super) async fn iter_with_timeout_rebuild<S: StateStoreRead>(
        state_store: S,
        range: TableKeyRange,
        epoch: HummockEpoch,
        options: ReadOptions,
        timeout: Duration,
    ) -> StorageResult<TimeoutAutoRebuildIter<S>> {
        const CHECK_TIMEOUT_PERIOD: usize = 100;
        // use a struct here to avoid accidental copy instead of move on primitive usize
        struct Count(usize);
        let mut check_count = Count(0);
        let mut total_count = Count(0);
        let mut curr_iter_item_count = Count(0);
        let mut start_time = Instant::now();
        let initial_start_time = start_time;
        AutoRebuildStateStoreReadIter::new(
            state_store,
            move || {
                check_count.0 += 1;
                curr_iter_item_count.0 += 1;
                total_count.0 += 1;
                if check_count.0 == CHECK_TIMEOUT_PERIOD {
                    check_count.0 = 0;
                    if start_time.elapsed() > timeout {
                        let prev_iter_item_count = curr_iter_item_count.0;
                        curr_iter_item_count.0 = 0;
                        start_time = Instant::now();
                        info!(
                            table_id = options.table_id.table_id,
                            iter_exist_time_secs = initial_start_time.elapsed().as_secs(),
                            prev_iter_item_count,
                            total_iter_item_count = total_count.0,
                            "kv log store iter is rebuilt"
                        );
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            },
            range,
            epoch,
            options,
        )
        .await
    }
}

use timeout_auto_rebuild::*;

impl<S: StateStoreRead, F: FnMut() -> bool + Send> StateStoreIter
    for AutoRebuildStateStoreReadIter<S, F>
{
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreIterItemRef<'_>>> {
        let should_rebuild = (self.should_rebuild)();
        if should_rebuild {
            let Some((key, _value)) = self.iter.try_next().await? else {
                return Ok(None);
            };
            let key: FullKey<&[u8]> = key;
            let range_start = Bytes::copy_from_slice(key.user_key.table_key.as_ref());
            let new_iter = self
                .state_store
                .iter(
                    (
                        Included(TableKey(range_start.clone())),
                        self.end_bound.clone(),
                    ),
                    self.epoch,
                    self.options.clone(),
                )
                .await?;
            self.iter = new_iter;
            let item: Option<StateStoreIterItemRef<'_>> = self.iter.try_next().await?;
            if let Some((key, value)) = item {
                assert_eq!(
                    key.user_key.table_key.0,
                    range_start.as_ref(),
                    "the first key should be the previous key"
                );
                Ok(Some((key, value)))
            } else {
                unreachable!(
                    "the first key should be the previous key {:?}, but get None",
                    range_start
                )
            }
        } else {
            self.iter.try_next().await
        }
    }
}

impl<S: StateStore> KvLogStoreReader<S> {
    fn read_persisted_log_store(
        &self,
        last_persisted_epoch: Option<u64>,
    ) -> impl Future<
        Output = LogStoreResult<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>,
    > + Send {
        let range_start = if let Some(last_persisted_epoch) = last_persisted_epoch {
            // start from the next epoch of last_persisted_epoch
            Included(
                self.serde
                    .serialize_pk_epoch_prefix(last_persisted_epoch.next_epoch()),
            )
        } else {
            Unbounded
        };
        let range_end = self.serde.serialize_pk_epoch_prefix(
            self.first_write_epoch
                .expect("should have set first write epoch"),
        );

        let serde = self.serde.clone();
        let table_id = self.table_id;
        let read_metrics = self.metrics.persistent_log_read_metrics.clone();
        let streams_future = try_join_all(serde.vnodes().iter_vnodes().map(|vnode| {
            let key_range = prefixed_range_with_vnode(
                (range_start.clone(), Excluded(range_end.clone())),
                vnode,
            );
            let state_store = self.state_store.clone();
            async move {
                // rebuild the iter every 10 minutes to avoid pinning hummock version for too long
                iter_with_timeout_rebuild(
                    state_store,
                    key_range,
                    HummockEpoch::MAX,
                    ReadOptions {
                        // This stream lives too long, the connection of prefetch object may break. So use a short connection prefetch.
                        prefetch_options: PrefetchOptions::prefetch_for_small_range_scan(),
                        cache_policy: CachePolicy::Fill(CacheHint::Low),
                        table_id,
                        ..Default::default()
                    },
                    Duration::from_secs(10 * 60),
                )
                .await
            }
        }));

        streams_future.map_err(Into::into).map_ok(|streams| {
            // TODO: set chunk size by config
            Box::pin(merge_log_store_item_stream(
                streams,
                serde,
                1024,
                read_metrics,
            ))
        })
    }
}

impl<S: StateStore> LogReader for KvLogStoreReader<S> {
    async fn init(&mut self) -> LogStoreResult<()> {
        let first_write_epoch = self.rx.init().await;

        assert!(
            self.first_write_epoch.replace(first_write_epoch).is_none(),
            "should not init twice"
        );

        self.state_store_stream = Some(self.read_persisted_log_store(None).await?);

        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        while *self.is_paused.borrow_and_update() {
            info!("next_item of {} get blocked by is_pause", self.identity);
            self.is_paused
                .changed()
                .instrument_await("Wait for Pause Resume")
                .await
                .map_err(|_| anyhow!("unable to subscribe resume"))?;
        }
        if let Some(state_store_stream) = &mut self.state_store_stream {
            match state_store_stream
                .try_next()
                .instrument_await("Try Next for Historical Stream")
                .await?
            {
                Some((epoch, item)) => {
                    if let Some(latest_offset) = &self.latest_offset {
                        latest_offset.check_next_item_epoch(epoch)?;
                    }
                    let item = match item {
                        KvLogStoreItem::StreamChunk(chunk) => {
                            let chunk_id = if let Some(latest_offset) = self.latest_offset {
                                latest_offset.next_chunk_id()
                            } else {
                                0
                            };
                            self.latest_offset = Some(TruncateOffset::Chunk { epoch, chunk_id });
                            LogStoreReadItem::StreamChunk { chunk, chunk_id }
                        }
                        KvLogStoreItem::Barrier { is_checkpoint } => {
                            self.latest_offset = Some(TruncateOffset::Barrier { epoch });
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

        // It is possible that the future gets dropped after it pops a flushed
        // item but before it reads a stream chunk. Therefore, we may continue
        // driving the future to continue reading the stream chunk.
        if let Some((chunk_id, chunk, item_epoch)) = self.may_continue_read_flushed_chunk().await? {
            let offset = TruncateOffset::Chunk {
                epoch: item_epoch,
                chunk_id,
            };
            if let Some(latest_offset) = &self.latest_offset {
                assert!(offset > *latest_offset);
            }
            self.latest_offset = Some(offset);
            return Ok((
                item_epoch,
                LogStoreReadItem::StreamChunk { chunk, chunk_id },
            ));
        }

        // Now the historical state store has been consumed.
        let (item_epoch, item) = self
            .rx
            .next_item()
            .instrument_await("Wait Next Item from Buffer")
            .await;
        if let Some(latest_offset) = &self.latest_offset {
            latest_offset.check_next_item_epoch(item_epoch)?;
        }
        Ok(match item {
            LogStoreBufferItem::StreamChunk {
                chunk, chunk_id, ..
            } => {
                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                if let Some(latest_offset) = &self.latest_offset {
                    assert!(offset > *latest_offset);
                }
                self.latest_offset = Some(offset);
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
                let read_flushed_chunk_future = {
                    let serde = self.serde.clone();
                    let state_store = self.state_store.clone();
                    let table_id = self.table_id;
                    let read_metrics = self.metrics.flushed_buffer_read_metrics.clone();
                    async move {
                        let iters = try_join_all(vnode_bitmap.iter_vnodes().map(|vnode| {
                            let range_start =
                                serde.serialize_log_store_pk(vnode, item_epoch, Some(start_seq_id));
                            let range_end =
                                serde.serialize_log_store_pk(vnode, item_epoch, Some(end_seq_id));
                            let state_store = &state_store;

                            // Use MAX EPOCH here because the epoch to consume may be below the safe
                            // epoch
                            async move {
                                Ok::<_, anyhow::Error>(
                                    state_store
                                        .iter(
                                            (Included(range_start), Included(range_end)),
                                            HummockEpoch::MAX,
                                            ReadOptions {
                                                prefetch_options:
                                                    PrefetchOptions::prefetch_for_large_range_scan(),
                                                cache_policy: CachePolicy::Fill(CacheHint::Low),
                                                table_id,
                                                ..Default::default()
                                            },
                                        )
                                        .await?,
                                )
                            }
                        }))
                            .instrument_await("Wait Create Iter Stream")
                        .await?;

                        let chunk = serde
                            .deserialize_stream_chunk(
                                iters,
                                start_seq_id,
                                end_seq_id,
                                item_epoch,
                                &read_metrics,
                            )
                            .instrument_await("Deserialize Stream Chunk")
                            .await?;

                        Ok((chunk_id, chunk, item_epoch))
                    }
                    .boxed()
                };

                // Store the future in case that in the subsequent pending await point,
                // the future is cancelled, and we lose an flushed item.
                assert!(self
                    .read_flushed_chunk_future
                    .replace(read_flushed_chunk_future)
                    .is_none());

                // for cancellation test
                #[cfg(test)]
                {
                    sleep(Duration::from_secs(1)).await;
                }

                let (_, chunk, _) = self
                    .may_continue_read_flushed_chunk()
                    .await?
                    .expect("future just insert. unlikely to be none");

                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                if let Some(latest_offset) = &self.latest_offset {
                    assert!(offset > *latest_offset);
                }
                self.latest_offset = Some(offset);
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
                self.latest_offset = Some(TruncateOffset::Barrier { epoch: item_epoch });
                (item_epoch, LogStoreReadItem::Barrier { is_checkpoint })
            }
            LogStoreBufferItem::UpdateVnodes(bitmap) => {
                self.serde.update_vnode_bitmap(bitmap.clone());
                (item_epoch, LogStoreReadItem::UpdateVnodeBitmap(bitmap))
            }
        })
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        if offset > self.latest_offset.expect("should exist before truncation") {
            return Err(anyhow!(
                "truncate at a later offset {:?} than the current latest offset {:?}",
                offset,
                self.latest_offset
            ));
        }
        if offset.epoch() >= self.first_write_epoch.expect("should have init") {
            if let Some(truncate_offset) = &self.truncate_offset {
                if offset <= *truncate_offset {
                    return Err(anyhow!(
                        "truncate offset {:?} earlier than prev truncate offset {:?}",
                        offset,
                        truncate_offset
                    ));
                }
            }
            self.rx.truncate_buffer(offset);
            self.truncate_offset = Some(offset);
        } else {
            // For historical data, no need to truncate at seq id level. Only truncate at barrier.
            if let TruncateOffset::Barrier { epoch } = &offset {
                if let Some(truncate_offset) = &self.truncate_offset {
                    if offset <= *truncate_offset {
                        return Err(anyhow!(
                            "truncate offset {:?} earlier than prev truncate offset {:?}",
                            offset,
                            truncate_offset
                        ));
                    }
                }
                self.rx.truncate_historical(*epoch);
                self.truncate_offset = Some(offset);
            }
        }
        Ok(())
    }

    async fn rewind(&mut self) -> LogStoreResult<(bool, Option<Bitmap>)> {
        self.rewind_delay.rewind_delay(self.truncate_offset).await;
        self.latest_offset = None;
        self.read_flushed_chunk_future = None;
        if self.truncate_offset.is_none()
            || self.truncate_offset.expect("not none").epoch()
                < self.first_write_epoch.expect("should have init")
        {
            // still consuming persisted state store data
            let persisted_epoch =
                self.truncate_offset
                    .map(|truncate_offset| match truncate_offset {
                        TruncateOffset::Chunk { epoch, .. } => epoch.prev_epoch(),
                        TruncateOffset::Barrier { epoch } => epoch,
                    });
            self.state_store_stream = Some(self.read_persisted_log_store(persisted_epoch).await?);
        } else {
            assert!(self.state_store_stream.is_none());
        }
        self.rx.rewind();

        Ok((true, Some((**self.serde.vnodes()).clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::Unbounded;

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_storage::hummock::iterator::test_utils::{
        iterator_test_table_key_of, iterator_test_value_of,
    };
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::{ReadOptions, StateStoreRead, StateStoreWrite, WriteOptions};
    use risingwave_storage::StateStoreIter;

    use crate::common::log_store_impl::kv_log_store::reader::AutoRebuildStateStoreReadIter;
    use crate::common::log_store_impl::kv_log_store::test_utils::TEST_TABLE_ID;

    #[tokio::test]
    async fn test_auto_rebuild_iter() {
        let state_store = MemoryStateStore::new();
        let key_count = 100;
        let pairs = (0..key_count)
            .map(|i| {
                let key = iterator_test_table_key_of(i);
                let value = iterator_test_value_of(i);
                (TableKey(Bytes::from(key)), StorageValue::new_put(value))
            })
            .collect_vec();
        let epoch = test_epoch(1);
        state_store
            .ingest_batch(
                pairs.clone(),
                vec![],
                WriteOptions {
                    epoch,
                    table_id: TEST_TABLE_ID,
                },
            )
            .unwrap();

        async fn validate(
            mut kv_iter: impl Iterator<Item = (TableKey<Bytes>, StorageValue)>,
            mut iter: impl StateStoreIter,
        ) {
            while let Some((key, value)) = iter.try_next().await.unwrap() {
                let (k, v) = kv_iter.next().unwrap();
                assert_eq!(key.user_key.table_key, k.to_ref());
                assert_eq!(v.user_value.as_deref(), Some(value));
            }
            assert!(kv_iter.next().is_none());
        }

        let read_options = ReadOptions {
            table_id: TEST_TABLE_ID,
            ..Default::default()
        };

        let kv_iter = pairs.clone().into_iter();
        let iter = state_store
            .iter((Unbounded, Unbounded), epoch, read_options.clone())
            .await
            .unwrap();
        validate(kv_iter, iter).await;

        let kv_iter = pairs.clone().into_iter();
        let mut count = 0;
        let count_mut_ref = &mut count;
        let rebuild_period = 8;
        let mut rebuild_count = 0;
        let rebuild_count_mut_ref = &mut rebuild_count;
        let iter = AutoRebuildStateStoreReadIter::new(
            state_store,
            move || {
                *count_mut_ref += 1;
                if *count_mut_ref % rebuild_period == 0 {
                    *rebuild_count_mut_ref += 1;
                    true
                } else {
                    false
                }
            },
            (Unbounded, Unbounded),
            epoch,
            read_options,
        )
        .await
        .unwrap();
        validate(kv_iter, iter).await;
        assert_eq!(count, key_count + 1); // with an extra call on the last None
        assert_eq!(rebuild_count, key_count / rebuild_period);
    }
}
