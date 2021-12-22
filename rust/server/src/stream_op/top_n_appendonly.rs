use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};

use super::PkIndicesRef;
use crate::stream_op::managed_state::top_n::ManagedTopNState;
use crate::stream_op::{Executor, Message, OrderedArraysSerializer, PkIndices, StreamChunk};

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
        let lower_sub_keyspace = keyspace.with_segment(Segment::FixedLength(b"l/".to_vec()));
        let higher_sub_keyspace = keyspace.with_segment(Segment::FixedLength(b"h/".to_vec()));
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
            if self.managed_lower_state.total_count() < self.offset {
                // `elem` is in the range of `[0, offset)`,
                // we ignored it for now as it is not in the result set.
                self.managed_lower_state.insert(pk_bytes, row).await;
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

            if self.managed_higher_state.total_count() < num_need_to_keep {
                self.managed_higher_state
                    .insert(
                        element_to_compare_with_upper.0,
                        element_to_compare_with_upper.1.clone(),
                    )
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
                    .insert(
                        element_to_compare_with_upper.0,
                        element_to_compare_with_upper.1,
                    )
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

    use assert_matches::assert_matches;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::Int64Type;
    use risingwave_common::util::sort_util::OrderType;

    use crate::stream_op::test_utils::{create_in_memory_keyspace, MockSource};
    use crate::stream_op::top_n_appendonly::AppendOnlyTopNExecutor;
    use crate::stream_op::{Barrier, Executor, Message, PkIndices, StreamChunk};

    #[tokio::test]
    async fn test_append_only_top_n_executor() {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert; 6],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3, 10, 9, 8] },
                column_nonnull! { I64Array, Int64Type, [0, 1, 2, 3, 4, 5] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert; 4],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [7, 3, 1, 9] },
                column_nonnull! { I64Array, Int64Type, [6, 7, 8, 9, 10, 11] },
            ],
            visibility: None,
        };
        let chunk3 = StreamChunk {
            ops: vec![Op::Insert; 4],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [12, 13, 14, 15] },
            ],
            visibility: None,
        };
        let schema = Schema {
            fields: vec![Field {
                data_type: Int64Type::create(false),
            }],
        };
        let order_types = vec![OrderType::Ascending, OrderType::Ascending];
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

        let keyspace = create_in_memory_keyspace();
        let mut top_n_executor = AppendOnlyTopNExecutor::new(
            source as Box<dyn Executor>,
            order_types,
            (3, Some(4)),
            vec![0, 1],
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
