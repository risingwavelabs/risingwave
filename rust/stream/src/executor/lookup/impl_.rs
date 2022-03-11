use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{Row, RowRef, StreamChunk};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::{Keyspace, Segment};

use super::sides::{stream_lookup_arrange_prev_epoch, stream_lookup_arrange_this_epoch};
use super::*;
use crate::common::StreamChunkBuilder;

impl<S: StateStore> LookupExecutor<S> {
    pub fn new(
        arrangement: Box<dyn Executor>,
        stream: Box<dyn Executor>,
        arrangement_keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        use_current_epoch: bool,
        stream_join_key_indices: Vec<usize>,
    ) -> Self {
        let output_column_length = stream.schema().len() + arrangement.schema().len();

        // output schema: | arrange | stream |
        let schema_fields = arrangement
            .schema()
            .fields
            .iter()
            .chain(stream.schema().fields.iter())
            .cloned()
            .collect_vec();

        assert_eq!(schema_fields.len(), output_column_length);

        let schema = Schema::new(schema_fields);

        let output_data_types = schema.data_types();
        let arrangement_datatypes = arrangement.schema().data_types();
        let stream_datatypes = stream.schema().data_types();

        let arrangement_pk_indices = arrangement.pk_indices().to_vec();
        let stream_pk_indices = stream.pk_indices().to_vec();

        Self {
            output_data_types,
            schema,
            pk_indices,
            last_barrier: None,
            input: if use_current_epoch {
                Box::pin(stream_lookup_arrange_this_epoch(stream, arrangement))
            } else {
                Box::pin(stream_lookup_arrange_prev_epoch(stream, arrangement))
            },
            stream: StreamJoinSide {
                key_indices: stream_join_key_indices,
                pk_indices: stream_pk_indices,
                col_types: stream_datatypes,
            },
            arrangement: ArrangeJoinSide {
                arrange_key_types: vec![],
                pk_indices: arrangement_pk_indices,
                col_types: arrangement_datatypes,
                col_descs: vec![],
                order_types: vec![],
                keyspace: arrangement_keyspace,
                use_current_epoch,
                serializer: OrderedRowSerializer::new(vec![]),
                deserializer: CellBasedRowDeserializer::new(vec![]),
            },
        }
    }

    /// Try produce one stream message from [`LookupExecutor`]. If there's no message to produce, it
    /// will return `None`, and the `next` function of [`LookupExecutor`] will continously polling
    /// messages until there's one.
    ///
    /// If we can use `async_stream` to write this part, things could be easier.
    pub async fn next_inner(&mut self) -> Result<Option<Message>> {
        match self.input.next().await.expect("unexpected end of stream")? {
            ArrangeMessage::Barrier(barrier) => {
                self.process_barrier(barrier.clone()).await?;
                Ok(Some(Message::Barrier(barrier)))
            }
            ArrangeMessage::Arrange(_) => {
                // TODO: replicate batch
                //
                // As we assume currently all lookups are on the same worker node of arrangements,
                // the data would always be available in the local shared buffer. Therefore, there's
                // no need to replicate batch.
                Ok(None)
            }
            ArrangeMessage::Stream(chunk) => Ok(Some(Message::Chunk(self.lookup(chunk).await?))),
        }
    }

    /// Store the barrier.
    async fn process_barrier(&mut self, barrier: Barrier) -> Result<()> {
        self.last_barrier = Some(barrier);
        Ok(())
    }

    /// Lookup the data in the shared buffer.
    async fn lookup(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        let last_barrier = self
            .last_barrier
            .as_ref()
            .expect("data received before a barrier");
        let lookup_epoch = if self.arrangement.use_current_epoch {
            last_barrier.epoch.curr
        } else {
            last_barrier.epoch.prev
        };
        let chunk = chunk.compact()?;
        let (chunk, ops) = chunk.into_parts();

        let mut builder = StreamChunkBuilder::new(
            chunk.capacity(),
            &self.output_data_types,
            0,
            self.stream.col_types.len(),
        )?;

        for (op, row) in ops.iter().zip_eq(chunk.rows()) {
            for matched_row in self.lookup_one_row(&row, lookup_epoch).await? {
                builder.append_row(*op, &row, &matched_row)?;
            }
            // TODO: support outer join (return null if no rows are matched)
        }

        builder.finish()
    }

    /// Lookup all rows corresponding to a join key in shared buffer.
    async fn lookup_one_row(&mut self, row: &RowRef<'_>, lookup_epoch: u64) -> Result<Vec<Row>> {
        // TODO: add a cache for arrangement in an upstream executor

        // Serialize join key to a state store key.
        let key_prefix = {
            let mut key_prefix = vec![];
            self.arrangement
                .serializer
                .serialize_row_ref(row, &mut key_prefix);
            key_prefix
        };

        let arrange_keyspace = self
            .arrangement
            .keyspace
            .with_segment(Segment::raw(key_prefix));

        let all_cells = arrange_keyspace
            .scan_strip_prefix(None, lookup_epoch)
            .await?;
        let mut all_rows = vec![];

        for (pk_with_cell_id, cell) in all_cells {
            if let Some((_, row)) = self
                .arrangement
                .deserializer
                .deserialize(&pk_with_cell_id, &cell)?
            {
                all_rows.push(row);
            }
        }

        let (_, last_row) = self
            .arrangement
            .deserializer
            .take()
            .expect("unexpected end of cell-based row");

        all_rows.push(last_row);

        Ok(all_rows)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{I32Array, Op};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::column_nonnull;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use crate::executor::test_utils::*;
    use crate::executor::*;

    async fn create_arrangement(memory_state_store: MemoryStateStore) -> Box<dyn Executor + Send> {
        let table_id = TableId::new(1);

        // Two columns of int32 type, the second column is arrange key.
        let columns = vec![
            ColumnDesc {
                data_type: DataType::Int32,
                column_id: ColumnId::new(1),
                name: "rowid_column".to_string(),
            },
            ColumnDesc {
                data_type: DataType::Int32,
                column_id: ColumnId::new(2),
                name: "join_column".to_string(),
            },
        ];

        let column_ids = columns.iter().map(|c| c.column_id).collect_vec();

        // Prepare source chunks.
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I32Array, [1, 2, 3] },
                column_nonnull! { I32Array, [4, 5, 6] },
            ],
            None,
        );

        let chunk2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I32Array, [7, 3] },
                column_nonnull! { I32Array, [8, 6] },
            ],
            None,
        );

        // Prepare stream executors.
        let schema = Schema::new(
            columns
                .iter()
                .map(|col| Field::with_name(col.data_type.clone(), col.name.clone()))
                .collect_vec(),
        );

        let source = MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

        let keyspace = Keyspace::table_root(memory_state_store, &table_id);

        Box::new(MaterializeExecutor::new(
            Box::new(source),
            keyspace,
            vec![
                OrderPair::new(1, OrderType::Ascending),
                OrderPair::new(0, OrderType::Ascending),
            ],
            column_ids,
            1,
            "ArrangeExecutor".to_string(),
        ))
    }

    #[tokio::test]
    #[ignore]
    #[allow(dead_code)]
    async fn test_lookup() {
        let store = MemoryStateStore::new();
        create_arrangement(store.clone()).await;
    }
}
