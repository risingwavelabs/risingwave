use async_trait::async_trait;
use risingwave_common::array::{DataChunk, Op, Row, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::generate_output;
use crate::executor::managed_state::top_n::variants::{TOP_N_MAX, TOP_N_MIN};
use crate::executor::managed_state::top_n::{ManagedTopNBottomNState, ManagedTopNState};
use crate::executor_v2::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor_v2::top_n_executor::{TopNExecutorBase, TopNExecutorWrapper};
use crate::executor_v2::{BoxedMessageStream, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub type TopNExecutor<S> = TopNExecutorWrapper<InnerTopNExecutor<S>>;

impl<S: StateStore> TopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        pk_order_types: Vec<OrderType>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        cache_size: Option<usize>,
        total_count: (usize, usize, usize),
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerTopNExecutor::new(
                info,
                schema,
                pk_order_types,
                offset_and_limit,
                pk_indices,
                keyspace,
                cache_size,
                total_count,
                executor_id,
                key_indices,
            )?,
        })
    }
}

pub struct InnerTopNExecutor<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// The ordering
    pk_order_types: Vec<OrderType>,
    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,
    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `TopNExecutor`
    pk_indices: PkIndices,

    /// We are interested in which element is in the range of [offset, offset+limit). But we
    /// still need to record elements in the other two ranges.
    managed_lowest_state: ManagedTopNState<S, TOP_N_MAX>,
    managed_middle_state: ManagedTopNBottomNState<S>,
    managed_highest_state: ManagedTopNState<S, TOP_N_MIN>,

    /// Marks whether this is first-time execution. If yes, we need to fill in the cache from
    /// storage.
    first_execution: bool,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S: StateStore> InnerTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        pk_order_types: Vec<OrderType>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        cache_size: Option<usize>,
        total_count: (usize, usize, usize),
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let pk_data_types = pk_indices
            .iter()
            .map(|idx| schema.fields[*idx].data_type())
            .collect::<Vec<_>>();
        let row_data_types = schema
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>();
        let ordered_row_deserializer =
            OrderedRowDeserializer::new(pk_data_types, pk_order_types.clone());
        let table_column_descs = row_data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_column_descs);
        let lower_sub_keyspace = keyspace.append_u8(b'l');
        let middle_sub_keyspace = keyspace.append_u8(b'm');
        let higher_sub_keyspace = keyspace.append_u8(b'h');
        let managed_lowest_state = ManagedTopNState::<S, TOP_N_MAX>::new(
            cache_size,
            total_count.0,
            lower_sub_keyspace,
            row_data_types.clone(),
            ordered_row_deserializer.clone(),
            cell_based_row_deserializer.clone(),
        );
        let managed_middle_state = ManagedTopNBottomNState::new(
            cache_size,
            total_count.1,
            middle_sub_keyspace,
            row_data_types.clone(),
            ordered_row_deserializer.clone(),
            cell_based_row_deserializer.clone(),
        );
        let managed_highest_state = ManagedTopNState::<S, TOP_N_MIN>::new(
            cache_size,
            total_count.2,
            higher_sub_keyspace,
            row_data_types,
            ordered_row_deserializer,
            cell_based_row_deserializer,
        );
        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("TopNExecutor {:X}", executor_id),
            },
            schema,
            pk_order_types,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_lowest_state,
            managed_middle_state,
            managed_highest_state,
            pk_indices,
            first_execution: true,
            key_indices,
        })
    }

    async fn flush_inner(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_highest_state
            .flush(epoch)
            .await
            .map_err(StreamExecutorError::top_n_state_error)?;
        self.managed_middle_state
            .flush(epoch)
            .await
            .map_err(StreamExecutorError::top_n_state_error)?;
        self.managed_lowest_state
            .flush(epoch)
            .await
            .map_err(StreamExecutorError::top_n_state_error)
    }
}

impl<S: StateStore> Executor for InnerTopNExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        panic!("Should execute by wrapper");
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn clear_cache(&mut self) -> Result<()> {
        self.managed_lowest_state.clear_cache();
        self.managed_middle_state.clear_cache();
        self.managed_highest_state.clear_cache();
        self.first_execution = true;

        Ok(())
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerTopNExecutor<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        if self.first_execution {
            self.managed_lowest_state
                .fill_in_cache(epoch)
                .await
                .map_err(StreamExecutorError::top_n_state_error)?;
            self.managed_middle_state
                .fill_in_cache(epoch)
                .await
                .map_err(StreamExecutorError::top_n_state_error)?;
            self.managed_highest_state
                .fill_in_cache(epoch)
                .await
                .map_err(StreamExecutorError::top_n_state_error)?;
            self.first_execution = false;
        }

        let chunk = chunk.compact().map_err(StreamExecutorError::eval_error)?;

        let (ops, columns, _visibility) = chunk.into_inner();

        let data_chunk = DataChunk::builder().columns(columns).build();
        let num_limit = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];

        for (row_idx, op) in ops.iter().enumerate().take(data_chunk.capacity()) {
            let row_ref = data_chunk
                .row_at(row_idx)
                .map_err(StreamExecutorError::eval_error)?
                .0;
            let pk_row = Row(self
                .pk_indices
                .iter()
                .map(|idx| row_ref.0[*idx].to_owned_datum())
                .collect::<Vec<_>>());
            let ordered_pk_row = OrderedRow::new(pk_row, &self.pk_order_types);
            let row = row_ref.into();
            match *op {
                Op::Insert | Op::UpdateInsert => {
                    if self.managed_lowest_state.total_count() < self.offset {
                        // `elem` is in the range of `[0, offset)`,
                        // we ignored it for now as it is not in the result set.
                        self.managed_lowest_state
                            .insert(ordered_pk_row, row, epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?;
                        continue;
                    }

                    // We remark that when offset is 0, every input row has nothing to do with
                    // `managed_lower_state`.
                    let element_to_compare_with_middle = if self.offset > 0
                        && &ordered_pk_row < self.managed_lowest_state.top_element().unwrap().0
                    {
                        // If the new element is smaller than the largest element in [0, offset),
                        // the largest element need to move to [offset, offset+limit).
                        let res = self
                            .managed_lowest_state
                            .pop_top_element(epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?
                            .unwrap();
                        self.managed_lowest_state
                            .insert(ordered_pk_row, row, epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?;
                        res
                    } else {
                        (ordered_pk_row, row)
                    };

                    if self.managed_middle_state.total_count() < num_limit {
                        // `elem` is in the range of `[offset, offset+limit)`,
                        self.managed_middle_state
                            .insert(
                                element_to_compare_with_middle.0,
                                element_to_compare_with_middle.1.clone(),
                            )
                            .await;
                        new_ops.push(Op::Insert);
                        new_rows.push(element_to_compare_with_middle.1);
                        continue;
                    }

                    let element_to_compare_with_highest = if &element_to_compare_with_middle.0
                        < self.managed_middle_state.top_element().unwrap().0
                    {
                        let res = self
                            .managed_middle_state
                            .pop_top_element(epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?
                            .unwrap();
                        new_ops.push(Op::Delete);
                        new_rows.push(res.1.clone());
                        new_ops.push(Op::Insert);
                        new_rows.push(element_to_compare_with_middle.1.clone());
                        self.managed_middle_state
                            .insert(
                                element_to_compare_with_middle.0,
                                element_to_compare_with_middle.1,
                            )
                            .await;
                        res
                    } else {
                        element_to_compare_with_middle
                    };

                    // `elem` is in the range of `[offset+limit, +inf)`.
                    self.managed_highest_state
                        .insert(
                            element_to_compare_with_highest.0,
                            element_to_compare_with_highest.1,
                            epoch,
                        )
                        .await
                        .map_err(StreamExecutorError::top_n_state_error)?;
                }
                Op::Delete | Op::UpdateDelete => {
                    // The extra care we need to take for deletion is that when we delete an element
                    // from a managed state, we may need to move an element from
                    // a higher range to the current range. And this process may
                    // be recursive. Since this is a delete operator, the key
                    // must already exist in one of the three managed states. We
                    // first check whether the element is in the highest state.
                    if self.managed_middle_state.total_count() == num_limit
                        && ordered_pk_row > *self.managed_middle_state.top_element().unwrap().0
                    {
                        // The current element in in the range of `[offset+limit, +inf)`
                        self.managed_highest_state
                            .delete(&ordered_pk_row, epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?;
                    } else if self.managed_lowest_state.total_count() == self.offset
                        && (self.offset == 0
                            || ordered_pk_row > *self.managed_lowest_state.top_element().unwrap().0)
                    {
                        // The current element in in the range of `[offset, offset+limit)`
                        self.managed_middle_state
                            .delete(&ordered_pk_row, epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?;
                        new_ops.push(Op::Delete);
                        new_rows.push(row.clone());
                        // We need to bring one, if any, from highest to lowest.
                        if self.managed_highest_state.total_count() > 0 {
                            let smallest_element_from_highest_state = self
                                .managed_highest_state
                                .pop_top_element(epoch)
                                .await
                                .map_err(StreamExecutorError::top_n_state_error)?
                                .unwrap();
                            new_ops.push(Op::Insert);
                            new_rows.push(smallest_element_from_highest_state.1.clone());
                            self.managed_middle_state
                                .insert(
                                    smallest_element_from_highest_state.0,
                                    smallest_element_from_highest_state.1,
                                )
                                .await;
                        }
                    } else {
                        // The current element in in the range of `[0, offset)`
                        self.managed_lowest_state
                            .delete(&ordered_pk_row, epoch)
                            .await
                            .map_err(StreamExecutorError::top_n_state_error)?;
                        // We need to bring one, if any, from middle to lowest.
                        if self.managed_middle_state.total_count() > 0 {
                            let smallest_element_from_middle_state = self
                                .managed_middle_state
                                .pop_bottom_element(epoch)
                                .await
                                .map_err(StreamExecutorError::top_n_state_error)?
                                .unwrap();
                            new_ops.push(Op::Delete);
                            new_rows.push(smallest_element_from_middle_state.1.clone());
                            self.managed_lowest_state
                                .insert(
                                    smallest_element_from_middle_state.0,
                                    smallest_element_from_middle_state.1,
                                    epoch,
                                )
                                .await
                                .map_err(StreamExecutorError::top_n_state_error)?;
                        }
                        // We check whether we need to/can bring one from highest to middle.
                        // We remark that if `self.limit` is Some, it cannot be 0 as this should be
                        // optimized away in the frontend.
                        if self.managed_middle_state.total_count() == (num_limit - 1)
                            && self.managed_highest_state.total_count() > 0
                        {
                            let smallest_element_from_highest_state = self
                                .managed_highest_state
                                .pop_top_element(epoch)
                                .await
                                .map_err(StreamExecutorError::top_n_state_error)?
                                .unwrap();
                            new_ops.push(Op::Insert);
                            new_rows.push(smallest_element_from_highest_state.1.clone());
                            self.managed_middle_state
                                .insert(
                                    smallest_element_from_highest_state.0,
                                    smallest_element_from_highest_state.1,
                                )
                                .await;
                        }
                    }
                }
            }
        }
        generate_output(new_rows, new_ops, &self.schema).map_err(StreamExecutorError::eval_error)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.flush_inner(epoch).await
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::catalog::Field;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor_v2::test_utils::{create_in_memory_keyspace, MockSource};
    use crate::executor_v2::{Barrier, Message};

    fn create_stream_chunks() -> Vec<StreamChunk> {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert; 6],
            vec![
                column_nonnull! { I64Array, [1, 2, 3, 10, 9, 8] },
                column_nonnull! { I64Array, [0, 1, 2, 3, 4, 5] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![
                Op::Insert,
                Op::Delete,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ],
            vec![
                column_nonnull! { I64Array, [7, 3, 1, 5, 2, 11] },
                column_nonnull! { I64Array, [6, 2, 0, 7, 1, 8] },
            ],
            None,
        );
        let chunk3 = StreamChunk::new(
            vec![Op::Insert; 4],
            vec![
                column_nonnull! { I64Array, [6, 12, 13, 14] },
                column_nonnull! { I64Array, [9, 10, 11, 12] },
            ],
            None,
        );
        let chunk4 = StreamChunk::new(
            vec![Op::Delete; 3],
            vec![
                column_nonnull! { I64Array, [5, 6, 11]},
                column_nonnull! { I64Array, [7, 9, 8]},
            ],
            None,
        );
        vec![chunk1, chunk2, chunk3, chunk4]
    }

    fn create_schema() -> Schema {
        Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        }
    }

    fn create_order_types() -> Vec<OrderType> {
        vec![OrderType::Ascending, OrderType::Ascending]
    }

    fn create_source() -> Box<MockSource> {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(3)),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Barrier(Barrier::new_test_barrier(4)),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(5)),
            ],
        ))
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset() {
        let order_types = create_order_types();
        let source = create_source();
        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, None),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(10), Some(9), Some(8)];
            let expected_ops = vec![Op::Insert, Op::Insert, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // Now (1, 2, 3) -> (8, 9, 10)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(7), Some(8), Some(8), Some(8), Some(11)];
            let expected_ops = vec![
                Op::Insert,
                Op::Delete,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
            assert_eq!(
                res.column_at(0).array_ref().as_int64().iter().collect_vec(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (5, 7, 8) -> (9, 10, 11)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(8), Some(12), Some(13), Some(14)];
            let expected_ops = vec![Op::Insert; 4];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (5, 6, 7) -> (8, 9, 10, 11, 12, 13, 14)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(8), Some(9), Some(11)];
            let expected_ops = vec![Op::Delete, Op::Delete, Op::Delete];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (7, 8, 9) -> (10, 13, 14, _)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_limit() {
        let order_types = create_order_types();
        let source = create_source();
        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (0, Some(4)),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![
                Some(1),
                Some(2),
                Some(3),
                Some(10),
                Some(10),
                Some(9),
                Some(9),
                Some(8),
            ];
            let expected_ops = vec![
                Op::Insert,
                Op::Insert,
                Op::Insert,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // now () -> (1, 2, 3, 8) -> (9, 10)

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![
                Some(8),
                Some(7),
                Some(3),
                Some(8),
                Some(1),
                Some(9),
                Some(9),
                Some(5),
                Some(2),
                Some(9),
            ];
            let expected_ops = vec![
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // () -> (5, 7, 8, 9) -> (10, 11)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(9), Some(6)];
            let expected_ops = vec![Op::Delete, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // () -> (5, 6, 7, 8) -> (9, 10, 11, 12, 13, 14)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(5), Some(9), Some(6), Some(10)];
            let expected_ops = vec![Op::Delete, Op::Insert, Op::Delete, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // () -> (7, 8, 9, 10) -> (13, 14)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset_and_limit() {
        let order_types = create_order_types();
        let source = create_source();
        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, Some(4)),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(10), Some(9), Some(8)];
            let expected_ops = vec![Op::Insert, Op::Insert, Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // now (1, 2, 3) -> (8, 9, 10, _) -> ()

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(7), Some(8), Some(8), Some(8), Some(11)];
            let expected_ops = vec![
                Op::Insert,
                Op::Delete,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (5, 7, 8) -> (9, 10, 11, _) -> ()
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(8)];
            let expected_ops = vec![Op::Insert];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (5, 6, 7) -> (8, 9, 10, 11) -> (12, 13, 14)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(8), Some(12), Some(9), Some(13), Some(11), Some(14)];
            let expected_ops = vec![
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
                Op::Delete,
                Op::Insert,
            ];
            assert_eq!(
                res.columns()[0]
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                expected_values
            );
            assert_eq!(res.ops(), expected_ops);
        }
        // (7, 8, 9) -> (10, 13, 14, _)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }
}
