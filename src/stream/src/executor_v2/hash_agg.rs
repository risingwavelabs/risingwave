// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashCode, HashKey};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_storage::{Keyspace, StateStore};

use super::{Executor, ExecutorInfo, StreamExecutorResult};
use crate::executor::{pk_input_arrays, PkDataTypes, PkIndicesRef};
use crate::executor_v2::aggregation::{
    agg_input_arrays, generate_agg_schema, generate_agg_state, AggCall, AggState,
};
use crate::executor_v2::error::{StreamExecutorError, TracedStreamExecutorError};
use crate::executor_v2::{BoxedMessageStream, Message, PkIndices};

/// [`HashAggExecutor`] could process large amounts of data using a state backend. It works as
/// follows:
///
/// * The executor pulls data from the upstream, and apply the data chunks to the corresponding
///   aggregation states.
/// * While processing, it will record which keys have been modified in this epoch using
///   `modified_keys`.
/// * Upon a barrier is received, the executor will call `.flush` on the storage backend, so that
///   all modifications will be flushed to the storage backend. Meanwhile, the executor will go
///   through `modified_keys`, and produce a stream chunk based on the state changes.
pub struct HashAggExecutor<K: HashKey, S: StateStore> {
    input: Box<dyn Executor>,
    info: ExecutorInfo,

    /// Pk indices from input
    input_pk_indices: Vec<usize>,

    /// Schema from input
    input_schema: Schema,

    /// The executor operates on this keyspace.
    keyspace: Keyspace<S>,

    /// The cached states. `HashKey -> (prev_value, value)`.
    state_map: EvictableHashMap<K, Option<Box<AggState<S>>>>,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    key_indices: Vec<usize>,
}

impl<K: HashKey, S: StateStore> Executor for HashAggExecutor<K, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl<K: HashKey, S: StateStore> HashAggExecutor<K, S> {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let input_info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, Some(&key_indices));

        Ok(Self {
            input,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("HashAggExecutor-{:X}", executor_id),
            },
            input_pk_indices: input_info.pk_indices,
            input_schema: input_info.schema,
            keyspace,
            state_map: EvictableHashMap::new(1 << 16),
            agg_calls,
            key_indices,
        })
    }

    /// Get unique keys, hash codes and visibility map of each key in a batch.
    ///
    /// The returned order is the same as how we get distinct final columns from original columns.
    ///
    /// `keys` are Hash Keys of all the rows
    /// `key_hash_codes` are hash codes of the deserialized `keys`
    /// `visibility`, leave invisible ones out of aggregation
    fn get_unique_keys(
        keys: Vec<K>,
        key_hash_codes: Vec<HashCode>,
        visibility: &Option<Bitmap>,
    ) -> Result<Vec<(K, HashCode, Bitmap)>> {
        let total_num_rows = keys.len();
        assert_eq!(key_hash_codes.len(), total_num_rows);
        // Each hash key, e.g. `key1` corresponds to a visibility map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the
        // `input` The visibility map of each hash key will be passed into
        // `StreamingAggStateImpl`.
        let mut key_to_vis_maps = HashMap::new();

        // Give all the unique keys an order and iterate them later,
        // the order is the same as how we get distinct final columns from original columns.
        let mut unique_key_and_hash_codes = Vec::new();

        for (row_idx, (key, hash_code)) in keys.iter().zip_eq(key_hash_codes.iter()).enumerate() {
            // if the visibility map has already shadowed this row,
            // then we pass
            if let Some(vis_map) = visibility && !vis_map.is_set(row_idx).map_err(StreamExecutorError::eval_error)? {
                continue;
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                unique_key_and_hash_codes.push((key, hash_code));
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        let result = unique_key_and_hash_codes
            .into_iter()
            .map(|(key, hash_code)| {
                (
                    key.clone(),
                    hash_code.clone(),
                    key_to_vis_maps.remove(key).unwrap().try_into().unwrap(),
                )
            })
            .collect_vec();

        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_chunk(
        key_indices: &[usize],
        agg_calls: &[AggCall],
        input_pk_indices: &[usize],
        input_schema: &Schema,
        schema: &Schema,
        state_map: &mut EvictableHashMap<K, Option<Box<AggState<S>>>>,
        keyspace: &Keyspace<S>,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<()> {
        let (data_chunk, ops) = chunk.into_parts();

        // Compute hash code here before serializing keys to avoid duplicate hash code computation.
        let hash_codes = data_chunk
            .get_hash_values(key_indices, CRC32FastBuilder)
            .map_err(StreamExecutorError::eval_error)?;
        let keys = K::build_from_hash_code(key_indices, &data_chunk, hash_codes.clone())
            .map_err(StreamExecutorError::eval_error)?;
        let (columns, visibility) = data_chunk.into_parts();

        // --- Find unique keys in this batch and generate visibility map for each key ---
        // TODO: this might be inefficient if there are not too many duplicated keys in one batch.
        let unique_keys = Self::get_unique_keys(keys, hash_codes, &visibility)
            .map_err(StreamExecutorError::eval_error)?;

        // --- Retrieve all aggregation inputs in advance ---
        // Previously, this is done in `unique_keys` inner loop, which is very inefficient.
        let all_agg_input_arrays = agg_input_arrays(agg_calls, &columns);
        let pk_input_arrays = pk_input_arrays(input_pk_indices, &columns);

        let input_pk_data_types: PkDataTypes = input_pk_indices
            .iter()
            .map(|idx| input_schema.fields[*idx].data_type.clone())
            .collect();

        // When applying batch, we will send columns of primary keys to the last N columns.
        let all_agg_data = all_agg_input_arrays
            .into_iter()
            .map(|mut input_arrays| {
                input_arrays.extend(pk_input_arrays.iter().cloned());
                input_arrays
            })
            .collect_vec();

        let key_data_types = &schema.data_types()[..key_indices.len()];
        let mut futures = vec![];
        for (key, hash_code, vis_map) in unique_keys {
            // Retrieve previous state from the KeyedState.
            let states = state_map.put(key.to_owned(), None);

            let key = key.clone();
            // To leverage more parallelism in IO operations, fetching and updating states for every
            // unique keys is created as futures and run in parallel.
            futures.push(async {
                let vis_map = vis_map;

                // 1. If previous state didn't exist, the ManagedState will automatically create new
                // ones for them.
                let mut states = {
                    match states {
                        Some(s) => s.unwrap(),
                        None => Box::new(
                            generate_agg_state(
                                Some(
                                    &key.clone()
                                        .deserialize(key_data_types.iter())
                                        .map_err(StreamExecutorError::eval_error)?,
                                ),
                                agg_calls,
                                keyspace,
                                input_pk_data_types.clone(),
                                epoch,
                                Some(hash_code),
                            )
                            .await?,
                        ),
                    }
                };

                // 2. Mark the state as dirty by filling prev states
                states
                    .may_mark_as_dirty(epoch)
                    .await
                    .map_err(StreamExecutorError::agg_state_error)?;

                // 3. Apply batch to each of the state (per agg_call)
                for (agg_state, data) in
                    states.managed_states.iter_mut().zip_eq(all_agg_data.iter())
                {
                    let data = data.iter().map(|d| &**d).collect_vec();
                    agg_state
                        .apply_batch(&ops, Some(&vis_map), &data, epoch)
                        .await
                        .map_err(StreamExecutorError::agg_state_error)?;
                }

                Ok::<(_, Box<AggState<S>>), RwError>((key, states))
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10);
        while let Some(result) = buffered.next().await {
            let (key, state) = result.map_err(StreamExecutorError::agg_state_error)?;
            state_map.put(key, Some(state));
        }

        Ok(())
    }

    async fn flush_data(
        key_indices: &[usize],
        keyspace: &Keyspace<S>,
        schema: &Schema,
        state_map: &mut EvictableHashMap<K, Option<Box<AggState<S>>>>,
        epoch: u64,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.
        let (write_batch, dirty_cnt) = {
            let mut write_batch = keyspace.state_store().start_write_batch();
            let mut dirty_cnt = 0;

            for states in state_map.values_mut() {
                if states.as_ref().unwrap().is_dirty() {
                    dirty_cnt += 1;
                    for state in &mut states.as_mut().unwrap().managed_states {
                        state
                            .flush(&mut write_batch)
                            .map_err(StreamExecutorError::agg_state_error)?;
                    }
                }
            }
            (write_batch, dirty_cnt)
        };

        if dirty_cnt == 0 {
            // Nothing to flush.
            assert!(write_batch.is_empty());
            return Ok(None);
        }

        write_batch
            .ingest(epoch)
            .await
            .map_err(StreamExecutorError::agg_state_error)?;

        // --- Produce the stream chunk ---

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let mut builders = schema
            .create_array_builders(dirty_cnt * 2)
            .map_err(StreamExecutorError::eval_error)?;
        let mut new_ops = Vec::with_capacity(dirty_cnt);

        // --- Retrieve modified states and put the changes into the builders ---
        for (key, states) in state_map.iter_mut() {
            let appended = states
                .as_mut()
                .unwrap()
                .build_changes(&mut builders[key_indices.len()..], &mut new_ops, epoch)
                .await
                .map_err(StreamExecutorError::agg_state_error)?;

            for _ in 0..appended {
                key.clone()
                    .deserialize_to_builders(&mut builders[..key_indices.len()])
                    .map_err(StreamExecutorError::eval_error)?;
            }
        }

        // evict cache to target capacity
        // In current implementation, we need to fetch the RowCount from the state store once a key
        // is deleted and added again. We should find a way to eliminate this extra fetch.
        assert!(!state_map
            .values()
            .any(|state| state.as_ref().unwrap().is_dirty()));
        state_map.evict_to_target_cap();

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
            .try_collect()
            .map_err(StreamExecutorError::eval_error)?;

        let chunk = StreamChunk::new(new_ops, columns, None);

        trace!("output_chunk: {:?}", &chunk);
        Ok(Some(chunk))
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let HashAggExecutor {
            input,
            info,
            input_pk_indices,
            input_schema,
            keyspace,
            mut state_map,
            agg_calls,
            key_indices,
        } = self;
        let mut input = input.execute();
        let first_msg = input.next().await.unwrap()?;
        let barrier = first_msg
            .into_barrier()
            .expect("the first message received by agg executor must be a barrier");
        let mut epoch = barrier.epoch.curr;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    Self::apply_chunk(
                        &key_indices,
                        &agg_calls,
                        &input_pk_indices,
                        &input_schema,
                        &info.schema,
                        &mut state_map,
                        &keyspace,
                        chunk,
                        epoch,
                    )
                    .await?;
                }
                Message::Barrier(barrier) => {
                    let next_epoch = barrier.epoch.curr;
                    if let Some(chunk) = Self::flush_data(
                        &key_indices,
                        &keyspace,
                        &info.schema,
                        &mut state_map,
                        epoch,
                    )
                    .await?
                    {
                        assert_eq!(epoch, barrier.epoch.prev);
                        yield Message::Chunk(chunk);
                    }
                    yield Message::Barrier(barrier);
                    epoch = next_epoch;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::data_chunk_iter::Row;
    use risingwave_common::array::{I64Array, Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::error::Result;
    use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::*;
    use risingwave_storage::{Keyspace, StateStore};

    use crate::executor_v2::aggregation::{AggArgs, AggCall};
    use crate::executor_v2::test_utils::*;
    use crate::executor_v2::{Executor, HashAggExecutor, Message, PkIndices};
    use crate::row_nonnull;

    struct HashAggExecutorDispatcher<S: StateStore>(PhantomData<S>);

    struct HashAggExecutorDispatcherArgs<S: StateStore> {
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
    }

    impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcher<S> {
        type Input = HashAggExecutorDispatcherArgs<S>;
        type Output = Result<Box<dyn Executor>>;

        fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
            Ok(Box::new(HashAggExecutor::<K, S>::new(
                args.input,
                args.agg_calls,
                args.keyspace,
                args.pk_indices,
                args.executor_id,
                args.key_indices,
            )?))
        }
    }

    fn new_boxed_hash_agg_executor(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace: Keyspace<impl StateStore>,
        pk_indices: PkIndices,
        executor_id: u64,
    ) -> Box<dyn Executor> {
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].data_type())
            .collect_vec();
        let args = HashAggExecutorDispatcherArgs {
            input,
            agg_calls,
            key_indices,
            keyspace,
            pk_indices,
            executor_id,
        };
        let kind = calc_hash_key_kind(&keys);
        HashAggExecutorDispatcher::dispatch_by_kind(kind, args).unwrap()
    }

    // --- Test HashAgg with in-memory KeyedState ---

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory() {
        test_local_hash_aggregation_count(create_in_memory_keyspace()).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory() {
        test_global_hash_aggregation_count(create_in_memory_keyspace()).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_max_in_memory() {
        test_local_hash_aggregation_max(create_in_memory_keyspace()).await
    }

    async fn test_local_hash_aggregation_count(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![column_nonnull! { I64Array, [1, 2, 2] }],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Delete, Op::Delete, Op::Delete],
            vec![column_nonnull! { I64Array, [1, 2, 2] }],
            Some((vec![true, false, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let mut source = MockSource::new(schema, PkIndices::new());
        source.push_barrier(1, false);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(2, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
            },
        ];

        let hash_agg =
            new_boxed_hash_agg_executor(Box::new(source), agg_calls, keys, keyspace, vec![], 1);
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();

            assert_eq!(ops, vec![Op::Insert, Op::Insert]);

            let rows = data_chunk.rows().map(Row::from).sorted().collect_vec();
            let expected_rows = [
                row_nonnull![1i64, 1i64, 1i64, 1i64],
                row_nonnull![2i64, 2i64, 2i64, 2i64],
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .sorted()
                .collect_vec();
            let expected_rows = [
                (Op::Delete, row_nonnull![1i64, 1i64, 1i64, 1i64]),
                (Op::UpdateDelete, row_nonnull![2i64, 2i64, 2i64, 2i64]),
                (Op::UpdateInsert, row_nonnull![2i64, 1i64, 1i64, 1i64]),
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }
    }

    async fn test_global_hash_aggregation_count(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 2] },
                column_nonnull! { I64Array, [1, 2, 2] },
                column_nonnull! { I64Array, [1, 2, 2] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 2, 3] },
                column_nonnull! { I64Array, [1, 2, 2, 3] },
                column_nonnull! { I64Array, [1, 2, 2, 3] },
            ],
            Some((vec![true, false, true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let mut source = MockSource::new(schema, PkIndices::new());
        source.push_barrier(1, false);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(2, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(3, false);

        // This is local hash aggregation, so we add another sum state
        let key_indices = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 2),
                return_type: DataType::Int64,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            Box::new(source),
            agg_calls,
            key_indices,
            keyspace,
            vec![],
            1,
        );
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap().unwrap() {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .sorted()
                .collect_vec();

            let expected_rows = [
                (Op::Insert, row_nonnull![1i64, 1i64, 1i64, 1i64]),
                (Op::Insert, row_nonnull![2i64, 2i64, 4i64, 4i64]),
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!();
        }

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap().unwrap() {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .sorted()
                .collect_vec();

            let expected_rows = [
                (Op::Delete, row_nonnull![1i64, 1i64, 1i64, 1i64]),
                (Op::UpdateDelete, row_nonnull![2i64, 2i64, 4i64, 4i64]),
                (Op::UpdateInsert, row_nonnull![2i64, 1i64, 2i64, 2i64]),
                (Op::Insert, row_nonnull![3i64, 1i64, 3i64, 3i64]),
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!();
        }
    }

    async fn test_local_hash_aggregation_max(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert; 3],
            vec![
                // group key column
                column_nonnull! { I64Array, [1, 1, 2] },
                // data column to get minimum
                column_nonnull! { I64Array, [233, 23333, 2333] },
                // primary key column
                column_nonnull! { I64Array, [1001, 1002, 1003] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Delete; 3],
            vec![
                // group key column
                column_nonnull! { I64Array, [1, 1, 2] },
                // data column to get minimum
                column_nonnull! { I64Array, [233, 23333, 2333] },
                // primary key column
                column_nonnull! { I64Array, [1001, 1002, 1003] },
            ],
            Some((vec![true, false, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let mut source = MockSource::new(schema, vec![2]); // pk
        source.push_barrier(1, false);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(2, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
            },
        ];

        let hash_agg =
            new_boxed_hash_agg_executor(Box::new(source), agg_calls, keys, keyspace, vec![], 1);
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .sorted()
                .collect_vec();

            let expected_rows = [
                // group key, row count, min data
                (Op::Insert, row_nonnull![1i64, 2i64, 233i64]),
                (Op::Insert, row_nonnull![2i64, 1i64, 2333i64]),
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .sorted()
                .collect_vec();
            let expected_rows = [
                // group key, row count, min data
                (Op::Delete, row_nonnull![2i64, 1i64, 2333i64]),
                (Op::UpdateDelete, row_nonnull![1i64, 2i64, 233i64]),
                (Op::UpdateInsert, row_nonnull![1i64, 1i64, 23333i64]),
            ]
            .into_iter()
            .sorted()
            .collect_vec();

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }
    }
}
