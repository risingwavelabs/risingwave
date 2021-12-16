use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::message::ToBytes;
use risingwave_common::array::{DataChunk, Op, Row};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::deserialize_datum_from;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::{Keyspace, StateStore};

use super::PkIndicesRef;
use crate::stream_op::{
    serialize_cell, serialize_cell_idx, Executor, Message, OrderedArraysSerializer, PkIndices,
    StreamChunk,
};

#[async_trait]
pub trait TopNExecutor: Executor {
    /// Apply the chunk to the dirty state and get the diffs.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk>;

    /// Flush the buffered chunk to the storage backend.
    async fn flush_data(&mut self) -> Result<()>;

    fn input(&mut self) -> &mut dyn Executor;
}

/// We remark that topN executor diffs from aggregate executor as it must output diffs
/// whenever it applies a batch of input data. Therefore, topN executor flushes data only instead of
/// computing diffs and flushing when receiving a barrier.
pub async fn top_n_executor_next<S: StateStore>(
    executor: &mut AppendOnlyTopNExecutor<S>,
) -> Result<Message> {
    let msg = executor.input().next().await?;
    let res = match msg {
        Message::Chunk(chunk) => Ok(Message::Chunk(executor.apply_chunk(chunk).await?)),
        Message::Barrier(barrier) if barrier.stop => Ok(Message::Barrier(barrier)),
        Message::Barrier(barrier) => {
            executor.flush_data().await?;
            Ok(Message::Barrier(barrier))
        }
    };
    res
}

/// This state maintains a cache and a flush buffer with top-n cache policy.
/// This is because we need to compare the largest element to decide
/// whether a new element should be put into the state.
/// Additionally, it provides ability to query the largest element. Also, its value is a
/// row instead of a single `ScalarImpl`.
struct ManagedTopNState<S: StateStore> {
    /// Cache.
    top_n: BTreeMap<Bytes, Row>,
    /// Buffer for updates.
    flush_buffer: BTreeMap<Bytes, Option<Row>>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// Schema for serializing `Option<Row>`.
    schema: Schema,
}

impl<S: StateStore> ManagedTopNState<S> {
    pub fn new(
        top_n_count: Option<usize>,
        total_count: usize,
        keyspace: Keyspace<S>,
        schema: Schema,
    ) -> Self {
        Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count,
            top_n_count,
            keyspace,
            schema,
        }
    }

    #[cfg(test)]
    fn get_cache_len(&self) -> usize {
        self.top_n.len()
    }

    fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    fn retain_top_n(&mut self) {
        if let Some(count) = self.top_n_count {
            while self.top_n.len() > count {
                // Although it seems to pop the last element(supposedly with the largest key),
                // it is actually popping the element with the smallest key.
                // This is because we reverse serialize the key so that `scan` can fetch from
                // the larger end.
                self.top_n.pop_last();
            }
        }
    }

    async fn pop_top_element(&mut self) -> Result<Option<(Bytes, Row)>> {
        if self.total_count == 0 {
            Ok(None)
        } else {
            // Cache must always be non-empty when the state is not empty.
            debug_assert!(!self.top_n.is_empty(), "top_n is empty");
            // Similar as the comments in `retain_top_n`, it is actually popping
            // the element with the largest key.
            let element_to_pop = self.top_n.pop_first().unwrap();
            // We need to delete the element from the storage.
            self.flush_buffer.insert(element_to_pop.0.clone(), None);
            self.total_count -= 1;
            // If we have nothing in the cache, we have to scan from the storage.
            if self.top_n.is_empty() && self.total_count > 0 {
                self.flush().await?;
                self.fill_in_cache().await?;
            }
            Ok(Some(element_to_pop))
        }
    }

    fn top_element(&mut self) -> Option<(&Bytes, &Row)> {
        if self.total_count == 0 {
            None
        } else {
            self.top_n.first_key_value()
        }
    }

    async fn insert(&mut self, element: (Bytes, Row)) {
        self.top_n.insert(element.0.clone(), element.1.clone());
        self.flush_buffer.insert(element.0, Some(element.1));
        self.total_count += 1;
    }

    /// We can fill in the cache from storage only when state is not dirty, i.e. right after
    /// `flush`. We don't need to care about whether `self.top_n` is empty or not as the key is
    /// unique. An element with duplicated key scanned from the storage would just override the
    /// element with the same key in the cache, and their value must be the same.
    async fn fill_in_cache(&mut self) -> Result<()> {
        debug_assert!(!self.is_dirty());
        let pk_row_bytes = self
            .keyspace
            .scan_strip_prefix(
                self.top_n_count
                    .map(|top_n_count| top_n_count * self.schema.len()),
            )
            .await?;
        // We must have enough cells to restore a complete row.
        debug_assert_eq!(pk_row_bytes.len() % self.schema.len(), 0);
        // cell-based storage format, so `self.schema.len()`
        let mut cells = vec![];
        let mut cell_idx = 0;
        for (pk, cell_bytes) in pk_row_bytes {
            let mut deserializer = memcomparable::Deserializer::new(cell_bytes);
            let datum = deserialize_datum_from(
                &self.schema[cell_idx].data_type.data_type_kind(),
                &mut deserializer,
            )?;
            cells.push(datum);
            cell_idx += 1;
            cell_idx %= self.schema.len();
            if cells.len() == self.schema.len() {
                // format: [pk_buf | cell_idx (4B)]
                // Take `pk_buf` out.
                let pk_without_cell_idx = pk.slice(0..pk.len() - 4);
                let row = Row(std::mem::take(&mut cells));
                let prev_element = self.top_n.insert(pk_without_cell_idx, row.clone());
                if let Some(prev_row) = prev_element {
                    debug_assert_eq!(prev_row, row);
                }
            }
        }
        self.retain_top_n();
        Ok(())
    }

    /// `Flush` can be called by the executor when it receives a barrier and thus needs to
    /// checkpoint. TODO: `Flush` should also be called internally when `top_n` and
    /// `flush_buffer` exceeds certain limit.
    async fn flush(&mut self) -> Result<()> {
        if !self.is_dirty() {
            self.retain_top_n();
            return Ok(());
        }

        let mut write_batches = vec![];
        for (pk_buf, cells) in std::mem::take(&mut self.flush_buffer) {
            for cell_idx in 0..self.schema.len() {
                // format: [pk_buf | cell_idx (4B)]
                let key = [&pk_buf[..], &serialize_cell_idx(cell_idx as u32)?[..]].concat();
                // format: [keyspace prefix | pk_buf | cell_idx (4B)]
                let key = self.keyspace.prefixed_key(&key);
                let value = match &cells {
                    Some(cells) => Some(serialize_cell(&cells[cell_idx])?),
                    None => None,
                };
                write_batches.push((key.into(), value.map(Bytes::from)));
            }
        }
        self.keyspace
            .state_store()
            .ingest_batch(write_batches)
            .await?;

        self.retain_top_n();
        Ok(())
    }
}

/// If the input contains only append, `AppendOnlyTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
/// TODO: Optimization: primary key may contain several columns and is used to determine
/// the order, therefore the value part should not contain the same columns to save space.
pub struct AppendOnlyTopNExecutor<S: StateStore> {
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// The ordering
    order_types: Vec<OrderType>,
    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,
    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,
    /// The primary key indices of the `AppendOnlyTopNExecutor`
    pk_indices: PkIndices,
    /// We are only interested in which element is in the range of `[offset, offset+limit)`(right
    /// open interval) but not the rank of such element
    ///
    /// We keep two ordered sets. One set stores the elements in the range of `[0, offset)`, and
    /// another set stores the elements in the range of `[offset, offset+limit)`.
    managed_lower_state: ManagedTopNState<S>,
    managed_higher_state: ManagedTopNState<S>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// OrderedRowsSerializer
    ordered_arrays_serializer: OrderedArraysSerializer,
    /// Marks whether this is first-time execution. If yes, we need to fill in the cache from
    /// storage.
    first_execution: bool,
    /// Cache size for the two states.
    cache_size: Option<usize>,
}

impl<S: StateStore> AppendOnlyTopNExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        order_types: Vec<OrderType>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        cache_size: Option<usize>,
        total_count: (usize, usize),
    ) -> Self {
        let order_pairs = order_types
            .iter()
            .zip(pk_indices.iter())
            .map(|(order_type, pk_index)| {
                let order_type = match order_type {
                    OrderType::Ascending => OrderType::Descending,
                    OrderType::Descending => OrderType::Ascending,
                };
                (order_type, *pk_index)
            })
            .collect::<Vec<_>>();
        let lower_sub_keyspace = keyspace.keyspace("l/".to_bytes());
        let higher_sub_keyspace = keyspace.keyspace("h/".to_bytes());
        let ordered_arrays_serializer = OrderedArraysSerializer::new(order_pairs);
        let input_schema = input.schema().clone();
        Self {
            input,
            order_types,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_lower_state: ManagedTopNState::<S>::new(
                cache_size,
                total_count.0,
                lower_sub_keyspace,
                input_schema.clone(),
            ),
            managed_higher_state: ManagedTopNState::<S>::new(
                cache_size,
                total_count.1,
                higher_sub_keyspace,
                input_schema,
            ),
            pk_indices,
            keyspace,
            ordered_arrays_serializer,
            first_execution: true,
            cache_size,
        }
    }

    async fn flush_inner(&mut self) -> Result<()> {
        self.managed_higher_state.flush().await?;
        self.managed_lower_state.flush().await
    }
}

#[async_trait]
impl<S: StateStore> Executor for AppendOnlyTopNExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        top_n_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutor for AppendOnlyTopNExecutor<S> {
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        if self.first_execution {
            self.managed_lower_state.fill_in_cache().await?;
            self.managed_higher_state.fill_in_cache().await?;
            self.first_execution = false;
        }

        let StreamChunk {
            // Ops is useless as we have assumed the input is append-only.
            ops: _ops,
            columns,
            visibility,
        } = chunk;

        let mut data_chunk: DataChunk = DataChunk::builder().columns(columns.to_vec()).build();
        if let Some(vis_map) = &visibility {
            data_chunk = data_chunk.with_visibility(vis_map.clone()).compact()?;
        }
        let data_chunk = Arc::new(data_chunk);
        // As we have already compacted the data chunk with visibility map,
        // we don't check visibility anymore.
        // We also don't compact ops as they are always "Insert"s.

        let num_need_to_keep = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];

        let arrays_for_pks = self
            .pk_indices
            .iter()
            .map(|pk_index| Ok(data_chunk.columns()[*pk_index].array_ref()))
            .collect::<Result<Vec<_>>>()?;

        let mut all_rows_pk_bytes = vec![];
        self.ordered_arrays_serializer
            .order_based_scehmaed_serialize(&arrays_for_pks, &mut all_rows_pk_bytes);
        for (row_idx, pk_bytes) in all_rows_pk_bytes.into_iter().enumerate() {
            let pk_bytes: Bytes = pk_bytes.into();
            let row = data_chunk.row_at(row_idx)?.0.into();
            if self.managed_lower_state.total_count < self.offset {
                // `elem` is in the range of `[0, offset)`,
                // we ignored it for now as it is not in the result set.
                self.managed_lower_state.insert((pk_bytes, row)).await;
                continue;
            }

            // We remark that we reverse serialize the pk so that we can `scan` from the larger end.
            // So if `>` here, it is actually `<`.
            let element_to_compare_with_upper =
                if pk_bytes > self.managed_lower_state.top_element().unwrap().0 {
                    // If the new element is smaller than the largest element in [0, offset),
                    // the largest element may need to move to [offset, offset+limit).
                    self.managed_lower_state.pop_top_element().await?.unwrap()
                } else {
                    (pk_bytes, row)
                };

            if self.managed_higher_state.total_count < num_need_to_keep {
                self.managed_higher_state
                    .insert(element_to_compare_with_upper.clone())
                    .await;
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1);
            } else if self.managed_higher_state.top_element().unwrap().0
                < &element_to_compare_with_upper.0
            {
                // The same as above, `<` means `>`.
                let element_to_pop = self.managed_higher_state.pop_top_element().await?.unwrap();
                new_ops.push(Op::Delete);
                new_rows.push(element_to_pop.1);
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1.clone());
                self.managed_higher_state
                    .insert(element_to_compare_with_upper)
                    .await;
            }
            // The "else" case can only be that `element_to_compare_with_upper` is larger than
            // the largest element in [offset, offset+limit), which is already full.
            // Therefore, nothing happens.
        }

        if !new_rows.is_empty() {
            let mut data_chunk_builder =
                DataChunkBuilder::new_with_default_size(self.schema().data_types_clone());
            for row in new_rows {
                data_chunk_builder.append_one_row_ref((&row).into())?;
            }
            // since `new_rows` is not empty, we unwrap directly
            let new_data_chunk = data_chunk_builder.consume_all()?.unwrap();
            let new_stream_chunk =
                StreamChunk::new(new_ops, new_data_chunk.columns().to_vec(), None);
            Ok(new_stream_chunk)
        } else {
            Ok(StreamChunk::new(vec![], vec![], None))
        }
    }

    async fn flush_data(&mut self) -> Result<()> {
        self.flush_inner().await
    }

    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use bytes::Bytes;
    use risingwave_common::array::{Array, I64Array, Op, Row};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::{DataTypeKind, Int64Type, StringType};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::{Keyspace, StateStore};

    use crate::row_nonnull;
    use crate::stream_op::test_utils::MockSource;
    use crate::stream_op::top_n_appendonly::{AppendOnlyTopNExecutor, ManagedTopNState};
    use crate::stream_op::{
        Barrier, Executor, Message, OrderedRowsSerializer, PkIndices, StreamChunk,
    };

    fn create_managed_top_n_state<S: StateStore>(
        store: &S,
        row_count: usize,
    ) -> ManagedTopNState<S> {
        let schema = Schema::new(vec![
            Field::new(Arc::new(Int64Type::new(false))),
            Field::new(StringType::create(false, 5, DataTypeKind::Varchar)),
        ]);

        ManagedTopNState::new(
            Some(2),
            row_count,
            Keyspace::new(store.clone(), b"test_2333".to_vec()),
            schema,
        )
    }

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let store = MemoryStateStore::new();
        let mut managed_state = create_managed_top_n_state(&store, 0);
        let row1 = row_nonnull![2i64, "abc".to_string()];
        let row2 = row_nonnull![3i64, "abc".to_string()];
        let row3 = row_nonnull![3i64, "abd".to_string()];
        let row4 = row_nonnull![4i64, "ab".to_string()];
        let rows = vec![&row1, &row2, &row3, &row4];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let pk_indices = vec![1, 0];
        let order_pairs = orderings
            .into_iter()
            .zip(pk_indices.into_iter())
            .collect::<Vec<_>>();
        let ordered_row_serializer = OrderedRowsSerializer::new(order_pairs);
        let mut rows_bytes = vec![];
        ordered_row_serializer.order_based_scehmaed_serialize(&rows, &mut rows_bytes);
        managed_state
            .insert((rows_bytes[3].clone().into(), row4.clone()))
            .await;

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 1);

        managed_state
            .insert((rows_bytes[2].clone().into(), row3.clone()))
            .await;

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        managed_state
            .insert((rows_bytes[1].clone().into(), row2.clone()))
            .await;
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        assert_eq!(managed_state.get_cache_len(), 3);
        managed_state.flush().await.unwrap();
        assert!(!managed_state.is_dirty());
        let row_count = managed_state.total_count;
        assert_eq!(row_count, 3);
        // After flush, only 2 elements should be kept in the cache.
        assert_eq!(managed_state.get_cache_len(), 2);

        drop(managed_state);
        let mut managed_state = create_managed_top_n_state(&store, row_count);
        assert_eq!(managed_state.top_element(), None);
        managed_state.fill_in_cache().await.unwrap();
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        // Right after recovery.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        assert_eq!(
            managed_state.pop_top_element().await.unwrap(),
            Some((Bytes::from(rows_bytes[2].clone()), row3))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 2);
        assert_eq!(managed_state.get_cache_len(), 1);
        assert_eq!(
            managed_state.pop_top_element().await.unwrap(),
            Some((Bytes::from(rows_bytes[1].clone()), row2.clone()))
        );
        // Popping to 0 element but automatically get at most `2` elements from the storage.
        // However, here we only have one element left as the `total_count` indicates.
        // The state is not dirty as we first flush and then scan from the storage.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 1);
        assert_eq!(managed_state.get_cache_len(), 1);

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );

        managed_state
            .insert((rows_bytes[0].clone().into(), row1.clone()))
            .await;
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[0].clone()), &row1))
        );

        // Exclude the last `insert` as the state crashes before recovery.
        let row_count = managed_state.total_count - 1;
        drop(managed_state);
        let mut managed_state = create_managed_top_n_state(&store, row_count);
        managed_state.fill_in_cache().await.unwrap();
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert; 6],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 3, 10, 9, 8] }],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert; 4],
            columns: vec![column_nonnull! { I64Array, Int64Type, [7, 3, 1, 9] }],
            visibility: None,
        };
        let chunk3 = StreamChunk {
            ops: vec![Op::Insert; 4],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 1, 2, 3] }],
            visibility: None,
        };
        let schema = Schema {
            fields: vec![Field {
                data_type: Int64Type::create(false),
            }],
        };
        let order_types = vec![OrderType::Ascending];
        let source = Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Chunk(chunk1),
                Message::Barrier(Barrier {
                    epoch: 0,
                    stop: false,
                }),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier {
                    epoch: 1,
                    stop: false,
                }),
                Message::Chunk(chunk3),
            ],
        ));
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut top_n_executor = AppendOnlyTopNExecutor::new(
            source as Box<dyn Executor>,
            order_types,
            (3, Some(4)),
            vec![0],
            keyspace,
            Some(2),
            (0, 0),
        );
        let res = top_n_executor.next().await.unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(10), Some(9), Some(8)];
            let expected_ops = vec![Op::Insert; 3];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops, expected_ops);
        }
        // barrier
        assert_matches!(top_n_executor.next().await.unwrap(), Message::Barrier(_));
        let res = top_n_executor.next().await.unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(10), Some(3), Some(9), Some(3)];
            let expected_ops = vec![Op::Insert, Op::Delete, Op::Insert, Op::Delete, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops, expected_ops);
        }
        // barrier
        assert_matches!(top_n_executor.next().await.unwrap(), Message::Barrier(_));
        let res = top_n_executor.next().await.unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(2)];
            let expected_ops = vec![Op::Delete, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops, expected_ops);
        }
    }
}
