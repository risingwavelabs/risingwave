// Copyright 2022 RisingWave Labs
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

use std::collections::BTreeMap;
use std::future::Future;
use std::mem;

use either::Either;
use futures::future::{Either as FutureEither, select};
use futures::stream::FuturesOrdered;
use futures::{StreamExt, TryStreamExt};
use risingwave_common::catalog::{ColumnDesc, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_expr::codegen::BoxStream;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::common::ThrottleType;
use risingwave_storage::StateStore;
use risingwave_storage::store::TryWaitEpochOptions;
use tokio::sync::oneshot;

use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

/// [`DmlExecutor`] accepts both stream data and batch data for data manipulation on a specific
/// table. The two streams will be merged into one and then sent to downstream.
pub struct DmlExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    upstream: Executor,

    /// Stores the information of batch data channels.
    dml_manager: DmlManagerRef,

    // Id of the table on which DML performs.
    table_id: TableId,

    // Version of the table on which DML performs.
    table_version_id: TableVersionId,

    // Column descriptions of the table.
    column_descs: Vec<ColumnDesc>,

    chunk_size: usize,

    rate_limiter: Arc<MonitoredRateLimiter>,

    state_store: S,
}

/// If a transaction's data is less than `MAX_CHUNK_FOR_ATOMICITY` * `CHUNK_SIZE`, we can provide
/// atomicity. Otherwise, it is possible that part of transaction's data is sent to the downstream
/// without barrier boundaries. There are some cases that could cause non-atomicity for large
/// transaction. 1. The system crashes.
/// 2. Actor scale-in or migration.
/// 3. Dml's batch query error occurs at the middle of its execution. (e.g. Remove UDF function
/// server become unavailable).
const MAX_CHUNK_FOR_ATOMICITY: usize = 32;

#[derive(Debug, Default)]
struct TxnBuffer {
    vec: Vec<StreamChunk>,
    // When vec size exceeds `MAX_CHUNK_FOR_ATOMICITY`, set true to `overflow`.
    overflow: bool,
}

impl<S: StateStore> DmlExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        upstream: Executor,
        dml_manager: DmlManagerRef,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_descs: Vec<ColumnDesc>,
        chunk_size: usize,
        rate_limit: RateLimit,
        state_store: S,
    ) -> Self {
        let rate_limiter = Arc::new(RateLimiter::new(rate_limit).monitored(table_id));
        Self {
            actor_ctx,
            upstream,
            dml_manager,
            table_id,
            table_version_id,
            column_descs,
            chunk_size,
            rate_limiter,
            state_store,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let mut upstream = self.upstream.execute();

        let actor_id = self.actor_ctx.id;

        // The first barrier message should be propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;

        // Construct the reader of batch data (DML from users). We must create a variable to hold
        // this `Arc<TableDmlHandle>` here, or it will be dropped due to the `Weak` reference in
        // `DmlManager`.
        //
        // Note(bugen): Only register after the first barrier message is received, which means the
        // current executor is activated. This avoids the new reader overwriting the old one during
        // the preparation of schema change.
        let handle = self.dml_manager.register_reader(
            self.table_id,
            self.table_version_id,
            &self.column_descs,
        )?;
        let reader = apply_dml_rate_limit(
            handle.stream_reader().into_stream(),
            self.rate_limiter.clone(),
        )
        .boxed()
        .map_err(StreamExecutorError::from);

        // Merge the two streams using `StreamReaderWithPause` because when we receive a pause
        // barrier, we should stop receiving the data from DML. We poll data from the two streams in
        // a round robin way.
        let mut stream = StreamReaderWithPause::<false, TxnMsg>::new(upstream, reader);

        // If the first barrier requires us to pause on startup, pause the stream.
        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        // Active transactions: txn_id -> TxnBuffer with transaction chunks.
        let mut active_txn_map: BTreeMap<TxnId, TxnBuffer> = Default::default();
        // A batch group of small chunks.
        let mut batch_group: Vec<StreamChunk> = vec![];

        let mut builder = StreamChunkBuilder::new(
            self.chunk_size,
            self.column_descs
                .iter()
                .map(|c| c.data_type.clone())
                .collect(),
        );

        // Notifiers from end_wait_persistence() calls in the current open epoch.
        // Drained into a try_wait_epoch future on each barrier.
        let mut pending_persistence_notifiers: Vec<oneshot::Sender<()>> = Vec::new();

        // In-flight persistence waits, one future per closed epoch that had pending notifiers.
        // Polled concurrently with the input stream via next_input_driving_persistence.
        let mut persistence_futures = FuturesOrdered::new();

        while let Some(input_msg) =
            next_input_driving_persistence(&mut stream, &mut persistence_futures).await?
        {
            match input_msg {
                Either::Left(msg) => {
                    // Stream messages.
                    if let Message::Barrier(barrier) = &msg {
                        // Flush any pending persistence notifiers for the epoch that is closing.
                        if !pending_persistence_notifiers.is_empty() {
                            let notifiers = mem::take(&mut pending_persistence_notifiers);
                            let closing_epoch = barrier.epoch.prev;
                            let store = self.state_store.clone();
                            let table_id = self.table_id;
                            persistence_futures.push_back(async move {
                                store
                                    .try_wait_epoch(
                                        HummockReadEpoch::Committed(closing_epoch),
                                        TryWaitEpochOptions { table_id },
                                    )
                                    .await
                                    .map_err(StreamExecutorError::from)?;
                                for tx in notifiers {
                                    let _ = tx.send(());
                                }
                                Ok(())
                            });
                        }

                        // We should handle barrier messages here to pause or resume the data from
                        // DML.
                        if let Some(mutation) = barrier.mutation.as_deref() {
                            match mutation {
                                Mutation::Pause => stream.pause_stream(),
                                Mutation::Resume => stream.resume_stream(),
                                Mutation::Throttle(fragment_to_apply) => {
                                    if let Some(entry) =
                                        fragment_to_apply.get(&self.actor_ctx.fragment_id)
                                        && entry.throttle_type() == ThrottleType::Dml
                                    {
                                        let new_rate_limit = entry.rate_limit.into();
                                        let old_rate_limit =
                                            self.rate_limiter.update(new_rate_limit);

                                        if old_rate_limit != new_rate_limit {
                                            tracing::info!(
                                                old_rate_limit = ?old_rate_limit,
                                                new_rate_limit = ?new_rate_limit,
                                                %actor_id,
                                                "dml rate limit changed",
                                            );
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }

                        // Flush the remaining batch group
                        if !batch_group.is_empty() {
                            let vec = mem::take(&mut batch_group);
                            for chunk in vec {
                                for (op, row) in chunk.rows() {
                                    if let Some(chunk) = builder.append_row(op, row) {
                                        yield Message::Chunk(chunk);
                                    }
                                }
                            }
                            if let Some(chunk) = builder.take() {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                    yield msg;
                }
                Either::Right(txn_msg) => {
                    // Batch data.
                    match txn_msg {
                        TxnMsg::Begin(txn_id) => {
                            active_txn_map
                                .try_insert(txn_id, TxnBuffer::default())
                                .unwrap_or_else(|_| {
                                    panic!("Transaction id collision txn_id = {}.", txn_id)
                                });
                        }
                        TxnMsg::End(txn_id, persistence_notifier) => {
                            if let Some(tx) = persistence_notifier {
                                pending_persistence_notifiers.push(tx);
                            }
                            let mut txn_buffer = active_txn_map.remove(&txn_id)
                                .unwrap_or_else(|| panic!("Receive an unexpected transaction end message. Active transaction map doesn't contain this transaction txn_id = {}.", txn_id));

                            let txn_buffer_cardinality = txn_buffer
                                .vec
                                .iter()
                                .map(|c| c.cardinality())
                                .sum::<usize>();
                            let batch_group_cardinality =
                                batch_group.iter().map(|c| c.cardinality()).sum::<usize>();

                            if txn_buffer_cardinality >= self.chunk_size {
                                // txn buffer is too large, so yield batch group first to preserve the transaction order in the same session.
                                if !batch_group.is_empty() {
                                    let vec = mem::take(&mut batch_group);
                                    for chunk in vec {
                                        for (op, row) in chunk.rows() {
                                            if let Some(chunk) = builder.append_row(op, row) {
                                                yield Message::Chunk(chunk);
                                            }
                                        }
                                    }
                                    if let Some(chunk) = builder.take() {
                                        yield Message::Chunk(chunk);
                                    }
                                }

                                // txn buffer isn't small, so yield.
                                for chunk in txn_buffer.vec {
                                    yield Message::Chunk(chunk);
                                }
                            } else if txn_buffer_cardinality + batch_group_cardinality
                                <= self.chunk_size
                            {
                                // txn buffer is small and batch group has space.
                                batch_group.extend(txn_buffer.vec);
                            } else {
                                // txn buffer is small and batch group has no space, so yield the batch group first to preserve the transaction order in the same session.
                                if !batch_group.is_empty() {
                                    let vec = mem::take(&mut batch_group);
                                    for chunk in vec {
                                        for (op, row) in chunk.rows() {
                                            if let Some(chunk) = builder.append_row(op, row) {
                                                yield Message::Chunk(chunk);
                                            }
                                        }
                                    }
                                    if let Some(chunk) = builder.take() {
                                        yield Message::Chunk(chunk);
                                    }
                                }

                                // put txn buffer into the batch group
                                mem::swap(&mut txn_buffer.vec, &mut batch_group);
                            }
                        }
                        TxnMsg::Rollback(txn_id) => {
                            let txn_buffer = active_txn_map.remove(&txn_id)
                                .unwrap_or_else(|| panic!("Receive an unexpected transaction rollback message. Active transaction map doesn't contain this transaction txn_id = {}.", txn_id));
                            if txn_buffer.overflow {
                                tracing::warn!(
                                    "txn_id={} large transaction tries to rollback, but part of its data has already been sent to the downstream.",
                                    txn_id
                                );
                            }
                        }
                        TxnMsg::Data(txn_id, chunk) => {
                            match active_txn_map.get_mut(&txn_id) {
                                Some(txn_buffer) => {
                                    // This transaction is too large, we can't provide atomicity,
                                    // so yield chunk ASAP.
                                    if txn_buffer.overflow {
                                        yield Message::Chunk(chunk);
                                        continue;
                                    }
                                    txn_buffer.vec.push(chunk);
                                    if txn_buffer.vec.len() > MAX_CHUNK_FOR_ATOMICITY {
                                        // Too many chunks for atomicity. Drain and yield them.
                                        tracing::warn!(
                                            "txn_id={} Too many chunks for atomicity. Sent them to the downstream anyway.",
                                            txn_id
                                        );
                                        for chunk in txn_buffer.vec.drain(..) {
                                            yield Message::Chunk(chunk);
                                        }
                                        txn_buffer.overflow = true;
                                    }
                                }
                                None => panic!(
                                    "Receive an unexpected transaction data message. Active transaction map doesn't contain this transaction txn_id = {}.",
                                    txn_id
                                ),
                            };
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Execute for DmlExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

/// Poll `stream` for the next input message while concurrently driving `persistence_futures`.
/// Any error from a completed persistence future is propagated immediately, causing the executor
/// (and thus the actor) to fail and trigger recovery.
async fn next_input_driving_persistence(
    stream: &mut StreamReaderWithPause<false, TxnMsg>,
    persistence_futures: &mut FuturesOrdered<impl Future<Output = StreamExecutorResult<()>>>,
) -> StreamExecutorResult<Option<Either<Message, TxnMsg>>> {
    loop {
        if persistence_futures.is_empty() {
            return stream.next().await.transpose();
        }

        match select(persistence_futures.next(), stream.next()).await {
            FutureEither::Left((Some(Ok(())), _)) => continue,
            FutureEither::Left((Some(Err(err)), _)) => return Err(err),
            FutureEither::Left((None, _)) => {
                unreachable!("persistence_futures is known to be non-empty")
            }
            FutureEither::Right((stream_item, _)) => return stream_item.transpose(),
        }
    }
}

type BoxTxnMessageStream = BoxStream<'static, risingwave_dml::error::Result<TxnMsg>>;
#[try_stream(ok = TxnMsg, error = risingwave_dml::error::DmlError)]
async fn apply_dml_rate_limit(
    stream: BoxTxnMessageStream,
    rate_limiter: Arc<MonitoredRateLimiter>,
) {
    #[for_await]
    for txn_msg in stream {
        match txn_msg? {
            TxnMsg::Begin(txn_id) => {
                yield TxnMsg::Begin(txn_id);
            }
            TxnMsg::End(txn_id, persistence_notifier) => {
                yield TxnMsg::End(txn_id, persistence_notifier);
            }
            TxnMsg::Rollback(txn_id) => {
                yield TxnMsg::Rollback(txn_id);
            }
            TxnMsg::Data(txn_id, chunk) => {
                let chunk_size = chunk.capacity();
                if chunk_size == 0 {
                    // empty chunk
                    yield TxnMsg::Data(txn_id, chunk);
                    continue;
                }
                let rate_limit = loop {
                    match rate_limiter.rate_limit() {
                        RateLimit::Pause => rate_limiter.wait(0).await,
                        limit => break limit,
                    }
                };

                match rate_limit {
                    RateLimit::Pause => unreachable!(),
                    RateLimit::Disabled => {
                        yield TxnMsg::Data(txn_id, chunk);
                        continue;
                    }
                    RateLimit::Fixed(limit) => {
                        let max_permits = limit.get();
                        let required_permits = chunk.rate_limit_permits();
                        if required_permits <= max_permits {
                            rate_limiter.wait(required_permits).await;
                            yield TxnMsg::Data(txn_id, chunk);
                        } else {
                            // Split the chunk into smaller chunks.
                            for small_chunk in chunk.split(max_permits as _) {
                                rate_limiter.wait_chunk(&small_chunk).await;
                                yield TxnMsg::Data(txn_id, small_chunk);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::FutureExt;
    use risingwave_common::catalog::{ColumnId, Field, INITIAL_TABLE_VERSION_ID};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_dml::dml_manager::DmlManager;
    use risingwave_hummock_sdk::key::TableKeyRange;
    use risingwave_storage::error::StorageResult;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::panic_store::{PanicStateStore, PanicStateStoreIter};
    use risingwave_storage::store::*;

    use super::*;
    use crate::executor::test_utils::MockSource;

    const TEST_TRANSACTION_ID: TxnId = 0;
    const TEST_SESSION_ID: u32 = 0;

    type WaitEpochCallSender = oneshot::Sender<(HummockReadEpoch, TryWaitEpochOptions)>;

    #[derive(Clone)]
    struct MockWaitEpochStateStore {
        wait_epoch_called_tx: Arc<Mutex<Option<WaitEpochCallSender>>>,
        wait_epoch_release_rx: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>,
    }

    impl StateStoreReadLog for MockWaitEpochStateStore {
        type ChangeLogIter = PanicStateStoreIter<StateStoreReadLogItem>;

        async fn next_epoch(&self, _epoch: u64, _options: NextEpochOptions) -> StorageResult<u64> {
            panic!("should not read changelog from MockWaitEpochStateStore")
        }

        async fn iter_log(
            &self,
            _epoch_range: (u64, u64),
            _key_range: TableKeyRange,
            _options: ReadLogOptions,
        ) -> StorageResult<Self::ChangeLogIter> {
            panic!("should not read changelog from MockWaitEpochStateStore")
        }
    }

    impl StateStore for MockWaitEpochStateStore {
        type Local = PanicStateStore;
        type ReadSnapshot = PanicStateStore;
        type VectorWriter = PanicStateStore;

        async fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
            options: TryWaitEpochOptions,
        ) -> StorageResult<()> {
            if let Some(tx) = self.wait_epoch_called_tx.lock().unwrap().take() {
                assert!(tx.send((epoch, options)).is_ok());
            }
            let rx = self.wait_epoch_release_rx.lock().await.take().unwrap();
            rx.await.unwrap();
            Ok(())
        }

        async fn new_local(&self, _option: NewLocalOptions) -> Self::Local {
            panic!("should not create local state from MockWaitEpochStateStore")
        }

        async fn new_read_snapshot(
            &self,
            _epoch: HummockReadEpoch,
            _options: NewReadSnapshotOptions,
        ) -> StorageResult<Self::ReadSnapshot> {
            panic!("should not read snapshot from MockWaitEpochStateStore")
        }

        async fn new_vector_writer(&self, _options: NewVectorWriterOptions) -> Self::VectorWriter {
            panic!("should not create vector writer from MockWaitEpochStateStore")
        }
    }

    #[tokio::test]
    async fn test_dml_executor() {
        let table_id = TableId::default();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let stream_key = vec![0];
        let dml_manager = Arc::new(DmlManager::for_test());

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, stream_key);

        let dml_executor = DmlExecutor::new(
            ActorContext::for_test(0),
            source,
            dml_manager.clone(),
            table_id,
            INITIAL_TABLE_VERSION_ID,
            column_descs,
            1024,
            RateLimit::Disabled,
            MemoryStateStore::new(),
        );
        let mut dml_executor = dml_executor.boxed().execute();

        let stream_chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6",
        );
        let stream_chunk2 = StreamChunk::from_pretty(
            " I I
            + 88 43",
        );
        let stream_chunk3 = StreamChunk::from_pretty(
            " I I
            + 199 40
            + 978 72
            + 134 41
            + 398 98",
        );
        let batch_chunk = StreamChunk::from_pretty(
            "  I I
            U+ 1 11
            U+ 2 22",
        );

        // The first barrier
        tx.push_barrier(test_epoch(1), false);
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert!(matches!(msg, Message::Barrier(_)));

        // Messages from upstream streaming executor
        tx.push_chunk(stream_chunk1);
        tx.push_chunk(stream_chunk2);
        tx.push_chunk(stream_chunk3);

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, INITIAL_TABLE_VERSION_ID)
            .unwrap();
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();

        // Message from batch
        write_handle.begin().unwrap();
        write_handle.write_chunk(batch_chunk).await.unwrap();
        // Since the end will wait the notifier which is sent by the reader,
        // we need to spawn a task here to avoid dead lock.
        tokio::spawn(async move {
            write_handle.end().await.unwrap();
            // a barrier to trigger batch group flush
            tx.push_barrier(test_epoch(2), false);
        });

        // Consume the 1st message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2
                + 3 6",
            )
        );

        // Consume the message from batch (because dml executor selects from the streams in a round
        // robin way)

        // TxnMsg::Begin is consumed implicitly

        // Consume the 2nd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 88 43",
            )
        );

        // TxnMsg::Data is buffed

        // Consume the 3rd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 199 40
                + 978 72
                + 134 41
                + 398 98",
            )
        );

        // After TxnMsg::End, we can consume dml data
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                U+ 1 11
                U+ 2 22",
            )
        );

        let msg = dml_executor.next().await.unwrap().unwrap();
        assert!(matches!(msg, Message::Barrier(_)));
    }

    #[tokio::test]
    async fn test_dml_executor_waits_for_barrier_prev_epoch_persistence() {
        let table_id = TableId::new(233);
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let column_descs = vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)];
        let stream_key = vec![0];
        let dml_manager = Arc::new(DmlManager::for_test());
        let (wait_epoch_called_tx, wait_epoch_called_rx) = oneshot::channel();
        let (wait_epoch_release_tx, wait_epoch_release_rx) = oneshot::channel();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, stream_key);
        let dml_executor = DmlExecutor::new(
            ActorContext::for_test(0),
            source,
            dml_manager.clone(),
            table_id,
            INITIAL_TABLE_VERSION_ID,
            column_descs,
            1024,
            RateLimit::Disabled,
            MockWaitEpochStateStore {
                wait_epoch_called_tx: Arc::new(Mutex::new(Some(wait_epoch_called_tx))),
                wait_epoch_release_rx: Arc::new(tokio::sync::Mutex::new(Some(
                    wait_epoch_release_rx,
                ))),
            },
        );
        let mut dml_executor = dml_executor.boxed().execute();

        tx.push_barrier_with_prev_epoch_for_test(test_epoch(10), test_epoch(9), false);
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert!(matches!(msg, Message::Barrier(_)));

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, INITIAL_TABLE_VERSION_ID)
            .unwrap();
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();
        write_handle.begin().unwrap();
        write_handle
            .write_chunk(StreamChunk::from_pretty(
                " I
                + 7",
            ))
            .await
            .unwrap();

        let mut persistence_future = Box::pin(write_handle.end_wait_persistence().unwrap());

        // Drive the executor until all queued DML messages are consumed. The transaction is smaller
        // than `chunk_size`, so it is buffered and no output is produced before the next barrier.
        assert!(dml_executor.next().now_or_never().is_none());

        let drain_handle = tokio::spawn(async move {
            while let Some(msg) = dml_executor.next().await {
                let _ = msg.unwrap();
            }
        });

        tx.push_barrier_with_prev_epoch_for_test(test_epoch(11), test_epoch(10), false);

        let (wait_epoch, options) = wait_epoch_called_rx.await.unwrap();
        assert!(matches!(
            wait_epoch,
            HummockReadEpoch::Committed(epoch) if epoch == test_epoch(10)
        ));
        assert_eq!(options.table_id, table_id);
        assert!(persistence_future.as_mut().now_or_never().is_none());

        wait_epoch_release_tx.send(()).unwrap();
        persistence_future.await.unwrap();

        drain_handle.abort();
    }
}
