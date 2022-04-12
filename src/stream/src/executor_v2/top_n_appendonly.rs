use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{DataChunk, Op, Row, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::managed_state::top_n::variants::TOP_N_MAX;
use crate::executor::managed_state::top_n::ManagedTopNState;
use crate::executor_v2::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor_v2::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use crate::executor_v2::{BoxedMessageStream, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

/// If the input contains only append, `AppendOnlyTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
/// TODO: Optimization: primary key may contain several columns and is used to determine
/// the order, therefore the value part should not contain the same columns to save space.
pub type AppendOnlyTopNExecutor<S> = TopNExecutorWrapper<InnerAppendOnlyTopNExecutor<S>>;

impl<S: StateStore> AppendOnlyTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        pk_order_types: Vec<OrderType>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        cache_size: Option<usize>,
        total_count: (usize, usize),
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerAppendOnlyTopNExecutor::new(
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

pub struct InnerAppendOnlyTopNExecutor<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// The ordering
    pk_order_types: Vec<OrderType>,
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
    managed_lower_state: ManagedTopNState<S, TOP_N_MAX>,
    managed_higher_state: ManagedTopNState<S, TOP_N_MAX>,
    /// Marks whether this is first-time execution. If yes, we need to fill in the cache from
    /// storage.
    first_execution: bool,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S: StateStore> InnerAppendOnlyTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        pk_order_types: Vec<OrderType>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        cache_size: Option<usize>,
        total_count: (usize, usize),
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
        let lower_sub_keyspace = keyspace.append_u8(b'l');
        let higher_sub_keyspace = keyspace.append_u8(b'h');
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
        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("AppendOnlyTopNExecutor {:X}", executor_id),
            },
            schema,
            pk_order_types,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_lower_state: ManagedTopNState::<S, TOP_N_MAX>::new(
                cache_size,
                total_count.0,
                lower_sub_keyspace,
                row_data_types.clone(),
                ordered_row_deserializer.clone(),
                cell_based_row_deserializer.clone(),
            ),
            managed_higher_state: ManagedTopNState::<S, TOP_N_MAX>::new(
                cache_size,
                total_count.1,
                higher_sub_keyspace,
                row_data_types,
                ordered_row_deserializer,
                cell_based_row_deserializer,
            ),
            pk_indices,
            first_execution: true,
            key_indices,
        })
    }

    async fn flush_inner(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_higher_state
            .flush(epoch)
            .await
            .map_err(StreamExecutorError::top_n_state_error)?;
        self.managed_lower_state
            .flush(epoch)
            .await
            .map_err(StreamExecutorError::top_n_state_error)
    }
}

#[async_trait]
impl<S: StateStore> Executor for InnerAppendOnlyTopNExecutor<S> {
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
        self.managed_lower_state.clear_cache();
        self.managed_higher_state.clear_cache();
        self.first_execution = true;

        Ok(())
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerAppendOnlyTopNExecutor<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        if self.first_execution {
            self.managed_lower_state
                .fill_in_cache(epoch)
                .await
                .map_err(StreamExecutorError::top_n_state_error)?;
            self.managed_higher_state
                .fill_in_cache(epoch)
                .await
                .map_err(StreamExecutorError::top_n_state_error)?;
            self.first_execution = false;
        }

        // Ops is useless as we have assumed the input is append-only.
        let (_ops, columns, visibility) = chunk.into_inner();

        let mut data_chunk: DataChunk = DataChunk::builder().columns(columns.to_vec()).build();
        if let Some(vis_map) = &visibility {
            data_chunk = data_chunk
                .with_visibility(vis_map.clone())
                .compact()
                .map_err(StreamExecutorError::eval_error)?;
        }
        let data_chunk = Arc::new(data_chunk);
        // As we have already compacted the data chunk with visibility map,
        // we don't check visibility anymore.
        // We also don't compact ops as they are always "Insert"s.

        let num_need_to_keep = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];

        for row_idx in 0..data_chunk.capacity() {
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
            if self.managed_lower_state.total_count() < self.offset {
                // `elem` is in the range of `[0, offset)`,
                // we ignored it for now as it is not in the result set.
                self.managed_lower_state
                    .insert(ordered_pk_row, row, epoch)
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?;
                continue;
            }

            // We remark that when offset is 0, every input row has nothing to do with
            // `managed_lower_state`.
            let element_to_compare_with_upper = if self.offset > 0
                && &ordered_pk_row < self.managed_lower_state.top_element().unwrap().0
            {
                // If the new element is smaller than the largest element in [0, offset),
                // the largest element may need to move to [offset, offset+limit).
                let res = self
                    .managed_lower_state
                    .pop_top_element(epoch)
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?
                    .unwrap();
                self.managed_lower_state
                    .insert(ordered_pk_row, row, epoch)
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?;
                res
            } else {
                (ordered_pk_row, row)
            };

            if self.managed_higher_state.total_count() < num_need_to_keep {
                self.managed_higher_state
                    .insert(
                        element_to_compare_with_upper.0,
                        element_to_compare_with_upper.1.clone(),
                        epoch,
                    )
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?;
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1);
            } else if self.managed_higher_state.top_element().unwrap().0
                > &element_to_compare_with_upper.0
            {
                let element_to_pop = self
                    .managed_higher_state
                    .pop_top_element(epoch)
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?
                    .unwrap();
                new_ops.push(Op::Delete);
                new_rows.push(element_to_pop.1);
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1.clone());
                self.managed_higher_state
                    .insert(
                        element_to_compare_with_upper.0,
                        element_to_compare_with_upper.1,
                        epoch,
                    )
                    .await
                    .map_err(StreamExecutorError::top_n_state_error)?;
            }
            // The "else" case can only be that `element_to_compare_with_upper` is larger than
            // the largest element in [offset, offset+limit), which is already full.
            // Therefore, nothing happens.
        }
        generate_output(new_rows, new_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.flush_inner(epoch).await
    }
}

#[cfg(test)]
mod tests {

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::{Array, I64Array, Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use crate::executor::{Barrier, Epoch, Message, PkIndices};
    use crate::executor_v2::test_utils::{create_in_memory_keyspace, MockSource};
    use crate::executor_v2::top_n_appendonly::AppendOnlyTopNExecutor;
    use crate::executor_v2::Executor;

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
            vec![Op::Insert; 4],
            vec![
                column_nonnull! { I64Array, [7, 3, 1, 9] },
                column_nonnull! { I64Array, [6, 7, 8, 9] },
            ],
            None,
        );
        let chunk3 = StreamChunk::new(
            vec![Op::Insert; 4],
            vec![
                column_nonnull! { I64Array, [1, 1, 2, 3] },
                column_nonnull! { I64Array, [12, 13, 14, 15] },
            ],
            None,
        );
        vec![chunk1, chunk2, chunk3]
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
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(1),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(2),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(3),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[2])),
            ],
        ))
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_offset() {
        let order_types = create_order_types();
        let source = create_source();

        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, None),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init epoch
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
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
            assert_eq!(res.ops(), expected_ops);
        }
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3) -> (8, 9, 10)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(7), Some(3), Some(3), Some(9)];
            let expected_ops = vec![Op::Insert, Op::Insert, Op::Insert, Op::Insert];
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
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2) -> (3, 3, 7, 8, 9, 10)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(2), Some(1), Some(2), Some(3)];
            let expected_ops = vec![Op::Insert, Op::Insert, Op::Insert, Op::Insert];
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
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1) -> (1, 2, 2, 3, 3, 3, 7, 8, 9, 10)
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_limit() {
        let order_types = create_order_types();
        let source = create_source();

        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (0, Some(5)),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init epoch
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![
                Some(1),
                Some(2),
                Some(3),
                Some(10),
                Some(9),
                Some(10),
                Some(8),
            ];
            let expected_ops = vec![
                Op::Insert,
                Op::Insert,
                Op::Insert,
                Op::Insert,
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
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3, 8, 9)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(9), Some(7), Some(8), Some(3), Some(7), Some(1)];
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
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2, 3, 3)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(3), Some(1), Some(3), Some(1)];
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
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1, 1, 2)
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_offset_and_limit() {
        let order_types = create_order_types();
        let source = create_source();

        let keyspace = create_in_memory_keyspace();
        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, Some(4)),
                vec![0, 1],
                keyspace,
                Some(2),
                (0, 0),
                1,
                vec![],
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init epoch
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
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
            assert_eq!(res.ops(), expected_ops);
        }
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3) -> (8, 9, 10)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
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
            assert_eq!(res.ops(), expected_ops);
        }
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2) -> (3, 3, 7, 8)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_matches!(res, Message::Chunk(_));
        if let Message::Chunk(res) = res {
            let expected_values = vec![Some(8), Some(2), Some(7), Some(1), Some(3), Some(2)];
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
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1) -> (1, 2, 2, 3)
    }
}
