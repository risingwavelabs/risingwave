use std::collections::BTreeMap;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::Datum;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    PkIndices, Watermark,
};

/// [`SortBufferKey`] contains a record's timestamp and pk.
type SortBufferKey = (Datum, Row);
/// [`SortBufferValue`] contains a record's operation and value, and also a flag indicating whether
/// the record has been persisted to storage.
type SortBufferValue = (Op, Row, bool);

/// [`SortExecutor`] consumes unordered input data and outputs ordered data to downstream.
pub struct SortExecutor<S: StateStore> {
    context: ActorContextRef,

    input: BoxedExecutor,

    pk_indices: PkIndices,

    identity: String,

    schema: Schema,

    state_table: StateTable<S>,

    /// The index of the column on which the sort executor sorts data.
    sort_column_index: usize,

    /// Stores data in memory ordered by the column indexed by `sort_column_index`. Once a
    /// watermark of `sort_column_index` arrives, data below watermark (i.e. value of that column
    /// being less than or equal to the watermark) should be sent to downstream and cleared from
    /// the buffer.
    buffer: BTreeMap<SortBufferKey, SortBufferValue>,

    /// The last received watermark. `None` on initialization. Used for range delete.
    _prev_watermark: Option<Datum>,
}

impl<S: StateStore> SortExecutor<S> {
    pub fn new(
        context: ActorContextRef,
        input: BoxedExecutor,
        pk_indices: PkIndices,
        executor_id: u64,
        state_table: StateTable<S>,
        sort_column_index: usize,
    ) -> Self {
        let schema = input.schema().clone();
        Self {
            context,
            input,
            pk_indices,
            identity: format!("SortExecutor {:X}", executor_id),
            schema,
            state_table,
            sort_column_index,
            buffer: BTreeMap::new(),
            _prev_watermark: None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(watermark) => {
                    let Watermark { col_idx, val } = watermark.clone();
                    // Sort executor only sends a stream chunk to downstream when
                    // `self.sort_column_index` matches the watermark's column index. Otherwise, it
                    // just forwards the watermark message to downstream without sending a stream
                    // chunk message.
                    if col_idx == self.sort_column_index {
                        let mut stream_chunk_data = Vec::with_capacity(self.buffer.len());
                        let watermark_value = val.clone();

                        // Find out the records to send to downstream.
                        while let Some(entry) = self.buffer.first_entry() {
                            // Only when a record's timestamp is prior to or equivalent to the
                            // watermark should it be sent to downstream.
                            if entry.key().0 <= watermark_value {
                                // Remove the record from memory.
                                let (op, row, persisted) = entry.remove();
                                // Remove the record from state store. It is possible that a record
                                // is not present in state store because this watermark arrives
                                // before a barrier since last watermark.
                                // TODO: Use range delete instead.
                                if persisted {
                                    self.state_table.delete(row.clone());
                                }
                                // Add the record to stream chunk data. Note that we retrieve the
                                // record from a BTreeMap, so data in this vector should be ordered
                                // by timestamp and pk.
                                stream_chunk_data.push((op, row));
                            } else {
                                // We have collected all data below watermark.
                                break;
                            }
                        }

                        // Construct and send a stream chunk message. Rows in this message are
                        // always ordered by timestamp.
                        if !stream_chunk_data.is_empty() {
                            let stream_chunk = StreamChunk::from_rows(
                                &stream_chunk_data,
                                &self.schema.data_types(),
                            );
                            yield Message::Chunk(stream_chunk);
                        }

                        // Update previous watermark, which is used for range delete.
                        self._prev_watermark = Some(val);
                    }

                    // Forward the watermark message.
                    yield Message::Watermark(watermark);
                }

                Message::Chunk(chunk) => {
                    for op_and_row in chunk.rows() {
                        match op_and_row.0 {
                            Op::Insert => {
                                // For insert operation, we buffer the record in memory.
                                let row = op_and_row.1.to_owned_row();
                                let timestamp = row.0.get(self.sort_column_index).unwrap().clone();
                                let pk = row.by_indices(&self.pk_indices);
                                self.buffer.insert((timestamp, pk), (op_and_row.0, row, false));
                            },
                            // Other operations are not supported currently.
                            _ => unimplemented!("Operations other than insert currently are not supported by sort executor")
                        }
                    }
                }

                Message::Barrier(barrier) => {
                    // Persist all records in buffer that have not been persisted before to state
                    // store.
                    for (_, (_, row, persisted)) in self.buffer.iter_mut() {
                        if !*persisted {
                            self.state_table.insert(row.clone());
                            // Update `persisted` so if the next barrier arrives before the
                            // next watermark, this record will not be persisted redundantly.
                            *persisted = true;
                        }
                    }

                    // Commit the epoch.
                    self.state_table.commit(barrier.epoch).await?;

                    // Update the vnode bitmap for the state table if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.context.id) {
                        let _ = self.state_table.update_vnode_bitmap(vnode_bitmap);
                    }

                    // FIXME: Scaling?

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl<S: StateStore> Executor for SortExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;

    use super::*;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{ActorContext, BoxedMessageStream, Executor};

    #[tokio::test]
    async fn test_sort_executor() {
        let sort_column_index = 1;
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 98 4
            + 37 5
            + 60 8",
        );
        let watermark1 = Some(ScalarImpl::Int64(3));
        let watermark2 = Some(ScalarImpl::Int64(6));

        let (mut tx, mut sort_executor) = create_executor(sort_column_index);

        // Init barrier
        tx.push_barrier(1, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        // Init watermark
        tx.push_watermark(0, Some(ScalarImpl::Int64(0)));
        tx.push_watermark(sort_column_index, Some(ScalarImpl::Int64(0)));

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();
        sort_executor.next().await.unwrap().unwrap();

        // Push data chunk1
        tx.push_chunk(chunk1);

        // Push watermark1 on an irrelevant column
        tx.push_watermark(0, watermark1.clone());

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark1 on sorted column
        tx.push_watermark(sort_column_index, watermark1);

        // Consume the data chunk
        let chunk_msg = sort_executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2"
            )
        );

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push data chunk2
        tx.push_chunk(chunk2);

        // Push barrier
        tx.push_barrier(2, false);

        // Consume the barrier
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark2 on an irrelevant column
        tx.push_watermark(0, watermark2.clone());

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();

        // Push watermark2 on sorted column
        tx.push_watermark(sort_column_index, watermark2);

        // Consume the data chunk
        let chunk_msg = sort_executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 98 4
                + 37 5
                + 3 6"
            )
        );

        // Consume the watermark
        sort_executor.next().await.unwrap().unwrap();
    }

    fn create_executor(sort_column_index: usize) -> (MessageSender, BoxedMessageStream) {
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let order_types = vec![OrderType::Ascending];
        let pk_indices = vec![0];
        let state_table = StateTable::new_without_distribution(
            memory_state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices.clone(),
        );

        let (tx, source) = MockSource::channel(schema, pk_indices.clone());
        let sort_executor = SortExecutor::new(
            ActorContext::create(123),
            Box::new(source),
            pk_indices,
            1,
            state_table,
            sort_column_index,
        );

        (tx, Box::new(sort_executor).execute())
    }
}
