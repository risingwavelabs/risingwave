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

//! This contains the synced kv log store implementation.
//! It's meant to buffer a large number of records emitted from upstream,
//! to avoid overwhelming the downstream executor.
//!
//! The synced kv log store polls two futures:
//!
//! 1. Upstream: upstream message source
//!
//!   It will write stream messages to the log store buffer. e.g. `Message::Barrier`, `Message::Chunk`, ...
//!   When writing a stream chunk, if the log store buffer is full, it will:
//!     a. Flush the buffer to the log store.
//!     b. Convert the stream chunk into a reference (`LogStoreBufferItem::Flushed`)
//!       which can read the corresponding chunks in the log store.
//!       We will compact adjacent references,
//!       so it can read multiple chunks if there's a build up.
//!
//!   On receiving barriers, it will:
//!     a. Apply truncation to historical data in the logstore.
//!     b. Flush and checkpoint the logstore data.
//!
//! 2. State store + buffer + recently flushed chunks: the storage components of the logstore.
//!
//!   It will read all historical data from the logstore first. This can be done just by
//!   constructing a state store stream, which will read all data until the latest epoch.
//!   This is a static snapshot of data.
//!   For any subsequently flushed chunks, we will read them via
//!   `flushed_chunk_future`. See the next paragraph below.
//!
//!   We will next read `flushed_chunk_future` (if there's one pre-existing one), see below for how
//!   it's constructed, what it is.
//!
//!   Finally we will pop the earliest item in the buffer.
//!   - If it's a chunk yield it.
//!   - If it's a watermark yield it.
//!   - If it's a flushed chunk reference (`LogStoreBufferItem::Flushed`),
//!     we will read the corresponding chunks in the log store.
//!     This is done by constructing a `flushed_chunk_future` which will read the log store
//!     using the `seq_id`.
//!   - Barrier,
//!     because they are directly propagated from the upstream when polling it.
//!
//! TODO(kwannoel):
//! - [] Add dedicated metrics for sync log store, namespace according to the upstream.
//! - [] Add tests
//! - [] Handle watermark r/w
//! - [] Handle paused stream

use std::collections::VecDeque;

use futures::StreamExt;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_connector::sink::log_store::ChunkId;
use risingwave_storage::StateStore;
use tokio::time::{Duration, Instant};

use crate::common::log_store_impl::kv_log_store::SeqId;
use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::executor::prelude::*;
use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
use crate::executor::sync_log_store_impl::SyncedKvLogStoreExecutorInner;

pub mod metrics {
    use risingwave_common::id::FragmentId;
    use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};

    use crate::common::log_store_impl::kv_log_store::KvLogStoreReadMetrics;
    use crate::executor::monitor::StreamingMetrics;
    use crate::task::ActorId;

    #[derive(Clone)]
    pub struct SyncedKvLogStoreMetrics {
        // state of the log store
        pub unclean_state: LabelGuardedIntCounter,
        pub clean_state: LabelGuardedIntCounter,
        pub wait_next_poll_ns: LabelGuardedIntCounter,

        // Write metrics
        pub storage_write_count: LabelGuardedIntCounter,
        pub storage_write_size: LabelGuardedIntCounter,
        pub pause_duration_ns: LabelGuardedIntCounter,

        // Buffer metrics
        pub buffer_unconsumed_item_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_row_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_epoch_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_min_epoch: LabelGuardedIntGauge,
        pub buffer_read_count: LabelGuardedIntCounter,
        pub buffer_read_size: LabelGuardedIntCounter,

        // Read metrics
        pub total_read_count: LabelGuardedIntCounter,
        pub total_read_size: LabelGuardedIntCounter,
        pub persistent_log_read_metrics: KvLogStoreReadMetrics,
        pub flushed_buffer_read_metrics: KvLogStoreReadMetrics,
    }

    impl SyncedKvLogStoreMetrics {
        /// `id`: refers to a unique way to identify the logstore. This can be the sink id,
        ///       or for joins, it can be the `fragment_id`.
        /// `name`: refers to the MV / Sink that the log store is associated with.
        /// `target`: refers to the target of the log store,
        ///           for instance `MySql` Sink, PG sink, etc...
        ///           or unaligned join.
        pub(crate) fn new(
            metrics: &StreamingMetrics,
            actor_id: ActorId,
            fragment_id: FragmentId,
            name: &str,
            target: &'static str,
        ) -> Self {
            let actor_id_str = actor_id.to_string();
            let fragment_id_str = fragment_id.to_string();
            let labels = &[&actor_id_str, target, &fragment_id_str, name];

            let unclean_state = metrics.sync_kv_log_store_state.with_guarded_label_values(&[
                "dirty",
                &actor_id_str,
                target,
                &fragment_id_str,
                name,
            ]);
            let clean_state = metrics.sync_kv_log_store_state.with_guarded_label_values(&[
                "clean",
                &actor_id_str,
                target,
                &fragment_id_str,
                name,
            ]);
            let wait_next_poll_ns = metrics
                .sync_kv_log_store_wait_next_poll_ns
                .with_guarded_label_values(labels);

            let storage_write_size = metrics
                .sync_kv_log_store_storage_write_size
                .with_guarded_label_values(labels);
            let storage_write_count = metrics
                .sync_kv_log_store_storage_write_count
                .with_guarded_label_values(labels);
            let storage_pause_duration_ns = metrics
                .sync_kv_log_store_write_pause_duration_ns
                .with_guarded_label_values(labels);

            let buffer_unconsumed_item_count = metrics
                .sync_kv_log_store_buffer_unconsumed_item_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_row_count = metrics
                .sync_kv_log_store_buffer_unconsumed_row_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_epoch_count = metrics
                .sync_kv_log_store_buffer_unconsumed_epoch_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_min_epoch = metrics
                .sync_kv_log_store_buffer_unconsumed_min_epoch
                .with_guarded_label_values(labels);
            let buffer_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    "buffer",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let buffer_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    "buffer",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let total_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    "total",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let total_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    "total",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            const READ_PERSISTENT_LOG: &str = "persistent_log";
            const READ_FLUSHED_BUFFER: &str = "flushed_buffer";

            let persistent_log_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    READ_PERSISTENT_LOG,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let persistent_log_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    READ_PERSISTENT_LOG,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let flushed_buffer_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    READ_FLUSHED_BUFFER,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let flushed_buffer_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    READ_FLUSHED_BUFFER,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            Self {
                unclean_state,
                clean_state,
                wait_next_poll_ns,
                storage_write_size,
                storage_write_count,
                pause_duration_ns: storage_pause_duration_ns,
                buffer_unconsumed_item_count,
                buffer_unconsumed_row_count,
                buffer_unconsumed_epoch_count,
                buffer_unconsumed_min_epoch,
                buffer_read_count,
                buffer_read_size,
                total_read_count,
                total_read_size,
                persistent_log_read_metrics: KvLogStoreReadMetrics {
                    storage_read_size: persistent_log_read_size,
                    storage_read_count: persistent_log_read_count,
                },
                flushed_buffer_read_metrics: KvLogStoreReadMetrics {
                    storage_read_count: flushed_buffer_read_count,
                    storage_read_size: flushed_buffer_read_size,
                },
            }
        }

        #[cfg(test)]
        pub(crate) fn for_test() -> Self {
            SyncedKvLogStoreMetrics {
                unclean_state: LabelGuardedIntCounter::test_int_counter::<5>(),
                clean_state: LabelGuardedIntCounter::test_int_counter::<5>(),
                wait_next_poll_ns: LabelGuardedIntCounter::test_int_counter::<4>(),
                storage_write_count: LabelGuardedIntCounter::test_int_counter::<4>(),
                storage_write_size: LabelGuardedIntCounter::test_int_counter::<4>(),
                pause_duration_ns: LabelGuardedIntCounter::test_int_counter::<4>(),
                buffer_unconsumed_item_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_row_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_epoch_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_min_epoch: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_read_count: LabelGuardedIntCounter::test_int_counter::<5>(),
                buffer_read_size: LabelGuardedIntCounter::test_int_counter::<5>(),
                total_read_count: LabelGuardedIntCounter::test_int_counter::<5>(),
                total_read_size: LabelGuardedIntCounter::test_int_counter::<5>(),
                persistent_log_read_metrics: KvLogStoreReadMetrics::for_test(),
                flushed_buffer_read_metrics: KvLogStoreReadMetrics::for_test(),
            }
        }
    }
}

pub struct SyncedKvLogStoreExecutor<S: StateStore> {
    inner: SyncedKvLogStoreExecutorInner<S>,
}
// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        actor_context: ActorContextRef,
        table_id: TableId,
        metrics: SyncedKvLogStoreMetrics,
        serde: LogStoreRowSerde,
        state_store: S,
        buffer_size: usize,
        chunk_size: usize,
        upstream: Executor,
        pause_duration_ms: Duration,
        aligned: bool,
    ) -> Self {
        Self {
            inner: SyncedKvLogStoreExecutorInner::new(
                actor_context,
                table_id,
                metrics,
                serde,
                state_store,
                buffer_size,
                chunk_size,
                upstream,
                pause_duration_ms,
                aligned,
            ),
        }
    }
}

// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    #[try_stream(ok= Message, error = StreamExecutorError)]
    pub async fn execute_monitored(self) {
        let wait_next_poll_ns = self.inner.metrics().wait_next_poll_ns.clone();
        #[for_await]
        for message in self.inner.execute_inner() {
            let current_time = Instant::now();
            yield message?;
            wait_next_poll_ns.inc_by(current_time.elapsed().as_nanos() as _);
        }
    }
}

pub(crate) struct SyncedLogStoreBuffer {
    pub(crate) buffer: VecDeque<(u64, LogStoreBufferItem)>,
    pub(crate) current_size: usize,
    pub(crate) max_size: usize,
    pub(crate) max_chunk_size: usize,
    pub(crate) next_chunk_id: ChunkId,
    pub(crate) metrics: SyncedKvLogStoreMetrics,
    pub(crate) flushed_count: usize,
}

impl SyncedLogStoreBuffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    pub(crate) fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk: StreamChunk,
        epoch: u64,
    ) -> Option<StreamChunk> {
        let current_size = self.current_size;
        let chunk_size = chunk.cardinality();

        tracing::trace!(
            current_size,
            chunk_size,
            max_size = self.max_size,
            "checking chunk size"
        );
        let should_flush_chunk = current_size + chunk_size > self.max_size;
        if should_flush_chunk {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "flushing chunk",);
            Some(chunk)
        } else {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "buffering chunk",);
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    pub(crate) fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        let new_chunk_size = (end_seq_id - start_seq_id + 1) as usize;

        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.buffer.back_mut()
            && let flushed_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && let projected_flushed_chunk_size = flushed_chunk_size + new_chunk_size
            && projected_flushed_chunk_size <= self.max_chunk_size
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.buffer.push_back((
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            ));
            self.flushed_count += 1;
            tracing::trace!(
                "adding flushed item to buffer: start_seq_id: {start_seq_id}, end_seq_id: {end_seq_id}, chunk_id: {chunk_id}"
            );
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_unconsumed_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        self.current_size += chunk.cardinality();
        self.buffer.push_back((
            epoch,
            LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            },
        ));
        self.update_unconsumed_buffer_metrics();
    }

    pub(crate) fn pop_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let item = self.buffer.pop_front();
        match &item {
            Some((_, LogStoreBufferItem::Flushed { .. })) => {
                self.flushed_count -= 1;
            }
            Some((_, LogStoreBufferItem::StreamChunk { chunk, .. })) => {
                self.current_size -= chunk.cardinality();
            }
            _ => {}
        }
        self.update_unconsumed_buffer_metrics();
        item
    }

    pub(crate) fn update_unconsumed_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        for (_, item) in &self.buffer {
            match item {
                LogStoreBufferItem::StreamChunk { chunk, .. } => {
                    row_count += chunk.cardinality();
                }
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    ..
                } => {
                    row_count += (end_seq_id - start_seq_id) as usize;
                }
                LogStoreBufferItem::Barrier { .. } => {
                    epoch_count += 1;
                }
            }
        }
        self.metrics.buffer_unconsumed_epoch_count.set(epoch_count);
        self.metrics.buffer_unconsumed_row_count.set(row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.buffer.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.buffer
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
    }
}

impl<S> Execute for SyncedKvLogStoreExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_monitored().boxed()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::*;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::assert_stream_chunk_eq;
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table, test_payload_schema,
    };
    use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
    use crate::executor::test_utils::MockSource;

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    // test read/write buffer
    #[tokio::test]
    async fn test_read_write_buffer() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U-  9  10
            U+ 10  11",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        tx.push_barrier(test_epoch(2), false);

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }
    }

    // test barrier persisted read
    //
    // sequence of events (earliest -> latest):
    // barrier(1) -> chunk(1) -> chunk(2) -> poll(3) items -> barrier(2) -> poll(1) item
    // * poll just means we read from the executor stream.
    #[tokio::test]
    async fn test_barrier_persisted_read() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }
    }

    // When we hit buffer max_chunk, we only store placeholder `FlushedItem`.
    // So we just let capacity = 0, and we will always flush incoming chunks to state store.
    #[tokio::test]
    async fn test_max_chunk_persisted_read() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            0,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        for i in 1..=2 {
            match stream.next().await {
                Some(Ok(Message::Barrier(barrier))) => {
                    assert_eq!(barrier.epoch.curr, test_epoch(i));
                }
                other => panic!("Expected a barrier message, got {:?}", other),
            }
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(actual))) => {
                let expected = StreamChunk::from_pretty(
                    "   I   T
                    +   5  10
                    +   6  10
                    +   8  10
                    +   9  10
                    +  10  11
                    -   5  10
                    -   6  10
                    -   8  10
                    U- 10  11
                    U+ 10  10",
                );
                assert_stream_chunk_eq!(actual, expected);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }
    }
}
