//! Global Streaming Hash Aggregators

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Row, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::Result;
use risingwave_storage::{Keyspace, StateStore};

use super::aggregation::{AggState, HashKey};
use super::{
    agg_executor_next, agg_input_arrays, generate_agg_schema, generate_agg_state, pk_input_arrays,
    AggCall, AggExecutor, Barrier, Executor, Message, PkIndicesRef,
};
use crate::executor::PkIndices;

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
pub struct HashAggExecutor<S: StateStore> {
    /// Schema of the executor.
    schema: Schema,

    /// Primary key indices.
    pk_indices: PkIndices,

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    cached_barrier_message: Option<Barrier>,

    /// The executor operates on this keyspace.
    keyspace: Keyspace<S>,

    /// The cached states. `HashKey -> (prev_value, value)`.
    state_map: EvictableHashMap<HashKey, AggState<S>>,

    /// The input of the current executor
    input: Box<dyn Executor>,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    key_indices: Vec<usize>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,

    /// Epoch
    epoch: Option<u64>,
}

impl<S: StateStore> HashAggExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, Some(&key_indices));

        Self {
            cached_barrier_message: None,
            keyspace,
            input,
            agg_calls,
            schema,
            key_indices,
            state_map: EvictableHashMap::new(1 << 16), // TODO: decide the target cap
            pk_indices,
            identity: format!("HashAggExecutor {:X}", executor_id),
            op_info,
            epoch: None,
        }
    }

    /// Get unique keys and visibility map of each key in a batch.
    ///
    /// The returned order is the same as how we get distinct final columns from original columns.
    ///
    /// `keys` are Hash Keys of all the rows
    /// `visibility`, leave invisible ones out of aggregation
    /// `state_entries`, the current state to check whether a key has existed or not
    fn get_unique_keys<'a, 'b>(
        &self,
        keys: &'a [HashKey],
        visibility: &Option<Bitmap>,
    ) -> Result<Vec<(&'b HashKey, Bitmap)>>
    where
        'a: 'b,
    {
        let total_num_rows = keys.len();
        // Each hash key, e.g. `key1` corresponds to a visibility map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the
        // `input` The visibility map of each hash key will be passed into
        // `StreamingAggStateImpl`.
        let mut key_to_vis_maps = HashMap::new();

        // Give all the unique keys an order and iterate them later,
        // the order is the same as how we get distinct final columns from original columns.
        let mut unique_keys = Vec::new();

        for (row_idx, key) in keys.iter().enumerate() {
            // if the visibility map has already shadowed this row,
            // then we pass
            if let Some(vis_map) = visibility && !vis_map.is_set(row_idx)? {
                continue;
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                unique_keys.push(key);
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        let result = unique_keys
            .into_iter()
            .map(|key| {
                (
                    key,
                    key_to_vis_maps.remove(key).unwrap().try_into().unwrap(),
                )
            })
            .collect_vec();

        Ok(result)
    }

    fn is_dirty(&self) -> bool {
        self.state_map.values().any(|state| state.is_dirty())
    }
}

#[async_trait]
impl<S: StateStore> AggExecutor for HashAggExecutor<S> {
    fn current_epoch(&self) -> Option<u64> {
        self.epoch
    }

    fn update_epoch(&mut self, new_epoch: u64) {
        self.epoch = Some(new_epoch);
    }

    fn cached_barrier_message_mut(&mut self) -> &mut Option<Barrier> {
        &mut self.cached_barrier_message
    }

    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let epoch = self.current_epoch().unwrap();
        let (ops, columns, visibility) = chunk.into_inner();

        // --- Retrieve grouped keys into Row format ---
        let keys = {
            let total_num_rows = ops.len();
            let mut keys = vec![Row(vec![]); total_num_rows];

            // we retrieve the key column-by-column
            for key_idx in &self.key_indices {
                let col = &columns[*key_idx];
                // for each column, we push all items in that column into the `keys` vector.
                for (row_idx, key) in keys.iter_mut().enumerate() {
                    key.0.push(col.array().datum_at(row_idx));
                }
            }

            keys
        };

        // --- Find unique keys in this batch and generate visibility map for each key ---
        // TODO: this might be inefficient if there are not too many duplicated keys in one batch.
        let unique_keys = self.get_unique_keys(&keys, &visibility)?;

        // --- Retrieve all aggregation inputs in advance ---
        // Previously, this is done in `unique_keys` inner loop, which is very inefficient.
        let all_agg_input_arrays = agg_input_arrays(&self.agg_calls, &columns);
        let pk_input_arrays = pk_input_arrays(self.input.pk_indices(), &columns);
        let input_pk_data_types = self.input.pk_data_types();

        // When applying batch, we will send columns of primary keys to the last N columns.
        let all_agg_data = all_agg_input_arrays
            .into_iter()
            .map(|mut input_arrays| {
                input_arrays.extend(pk_input_arrays.iter());
                input_arrays
            })
            .collect_vec();

        for (key, vis_map) in unique_keys {
            // 1. Retrieve previous state from the KeyedState. If they didn't exist, the
            // ManagedState will automatically create new ones for them.
            let states = {
                if !self.state_map.contains(key) {
                    let state = generate_agg_state(
                        Some(key),
                        &self.agg_calls,
                        &self.keyspace,
                        input_pk_data_types.clone(),
                        epoch,
                    )
                    .await?;
                    self.state_map.put(key.to_owned(), state);
                }
                self.state_map.get_mut(key).unwrap()
            };

            // 2. Mark the state as dirty by filling prev states
            states.may_mark_as_dirty(epoch).await?;

            // 3. Apply batch to each of the state (per agg_call)
            for (agg_state, data) in states.managed_states.iter_mut().zip_eq(all_agg_data.iter()) {
                agg_state
                    .apply_batch(&ops, Some(&vis_map), data, epoch)
                    .await?;
            }
        }

        Ok(())
    }

    async fn flush_data(&mut self) -> Result<Option<StreamChunk>> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.
        let epoch = match self.current_epoch() {
            Some(e) => e,
            None => return Ok(None),
        };

        let (write_batch, dirty_cnt) = {
            let mut write_batch = self.keyspace.state_store().start_write_batch();
            let mut dirty_cnt = 0;

            for states in self.state_map.values_mut() {
                if states.is_dirty() {
                    dirty_cnt += 1;
                    for state in &mut states.managed_states {
                        state.flush(&mut write_batch)?;
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

        write_batch.ingest(epoch).await.unwrap();

        // --- Produce the stream chunk ---

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let mut builders = self.schema.create_array_builders(dirty_cnt * 2)?;
        let mut new_ops = Vec::with_capacity(dirty_cnt);

        // --- Retrieve modified states and put the changes into the builders ---
        for (key, states) in self.state_map.iter_mut() {
            let _is_empty = states
                .build_changes(&mut builders, &mut new_ops, Some(key), epoch)
                .await?;
        }

        // evict cache to target capacity
        // In current implementation, we need to fetch the RowCount from the state store once a key
        // is deleted and added again. We should find a way to eliminate this extra fetch.
        assert!(!self.is_dirty());
        self.state_map.evict_to_target_cap();

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
            .try_collect()?;

        let chunk = StreamChunk::new(new_ops, columns, None);

        trace!("output_chunk: {:?}", &chunk);
        Ok(Some(chunk))
    }

    fn input(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}

impl<S: StateStore> std::fmt::Debug for HashAggExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateExecutor")
            .field("input", &self.input)
            .field("agg_calls", &self.agg_calls)
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl<S: StateStore> Executor for HashAggExecutor<S> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    async fn next(&mut self) -> Result<Message> {
        agg_executor_next(self).await
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn clear_cache(&mut self) -> Result<()> {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while states of hash agg are dirty"
        );
        self.state_map.clear();
        Ok(())
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

#[cfg(test)]
mod tests {

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::data_chunk_iter::Row;
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::Field;
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::*;

    use super::*;
    use crate::executor::test_utils::*;
    use crate::executor::*;
    use crate::*;

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

        let mut hash_agg = HashAggExecutor::new(
            Box::new(source),
            agg_calls,
            keys,
            keyspace,
            vec![],
            1,
            "HashAggExecutor".to_string(),
        );

        // Consume the init barrier
        hash_agg.next().await.unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap();
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

        assert_matches!(hash_agg.next().await.unwrap(), Message::Barrier { .. });

        let msg = hash_agg.next().await.unwrap();
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

        let mut hash_agg = HashAggExecutor::new(
            Box::new(source),
            agg_calls,
            key_indices,
            keyspace,
            vec![],
            1,
            "HashAggExecutor".to_string(),
        );

        // Consume the init barrier
        hash_agg.next().await.unwrap();
        // Consume stream chunk
        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
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

        assert_matches!(hash_agg.next().await.unwrap(), Message::Barrier { .. });

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
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

        let mut hash_agg = HashAggExecutor::new(
            Box::new(source),
            agg_calls,
            keys,
            keyspace,
            vec![],
            1,
            "HashAggExecutor".to_string(),
        );

        // Consume the init barrier
        hash_agg.next().await.unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap();
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

        assert_matches!(hash_agg.next().await.unwrap(), Message::Barrier { .. });

        let msg = hash_agg.next().await.unwrap();
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
