//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::keyspace::{Keyspace, StateStore};
use super::state_aggregation::{ManagedState, ManagedValueState};
use super::{AggCall, Barrier, Executor, Message};
use async_trait::async_trait;
use bytes::BufMut;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::Row;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use std::collections::HashMap;
use std::sync::Arc;

pub struct HashValue<S: StateStore> {
    managed_states: Vec<ManagedState<S>>,
    prev_states: Option<Vec<Datum>>,
}

impl<S: StateStore> HashValue<S> {
    pub async fn row_count(&mut self) -> Result<i64> {
        Ok(self.managed_states[0]
            .get_output()
            .await?
            .map(|x| *x.as_int64())
            .unwrap_or(0))
    }

    pub fn prev_row_count(&self) -> i64 {
        match &self.prev_states {
            Some(states) => states[0].as_ref().map(|x| *x.as_int64()).unwrap_or(0),
            None => 0,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.prev_states.is_some()
    }
}

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

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    // TODO: Create a `Barrier` struct.
    next_barrier_message: Option<Barrier>,

    /// The executor operates on this keyspace.
    keyspace: Keyspace<S>,

    /// The cached states. `HashKey -> (prev_value, value)`.
    state_map: HashMap<HashKey, HashValue<S>>,

    /// The input of the current executor
    input: Box<dyn Executor>,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    key_indices: Vec<usize>,
}

/// Generate [`HashAgg`]'s schema from `input`, `agg_calls` and `key_indices`.
pub fn generate_hash_agg_schema(
    input: &dyn Executor,
    agg_calls: &[AggCall],
    key_indices: &[usize],
) -> Schema {
    let mut fields = Vec::with_capacity(key_indices.len() + agg_calls.len());
    let input_schema = input.schema();
    fields.extend(
        key_indices
            .iter()
            .map(|idx| input_schema.fields[*idx].clone()),
    );
    fields.extend(agg_calls.iter().map(|agg| Field {
        data_type: agg.return_type.clone(),
    }));
    Schema { fields }
}

impl<S: StateStore> HashAggExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace: Keyspace<S>,
        schema: Schema,
    ) -> Self {
        Self {
            next_barrier_message: None,
            keyspace,
            input,
            agg_calls,
            schema,
            key_indices,
            state_map: HashMap::new(),
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
        // Each hash key, e.g. `key1` corresponds to a vis map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the
        // `input` The vis map of each hash key will be passed into StreamingAggStateImpl
        let mut key_to_vis_maps = HashMap::new();

        // Give all the unique keys an order and iterate them later,
        // the order is the same as how we get distinct final columns from original columns.
        let mut unique_keys = Vec::new();

        for (row_idx, key) in keys.iter().enumerate() {
            // if the visibility map has already shadowed this row,
            // then we pass
            if let Some(vis_map) = visibility {
                if !vis_map.is_set(row_idx)? {
                    continue;
                }
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                unique_keys.push(key);
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        let result = unique_keys
            .into_iter()
            // TODO: handle bitmap conversion error. When will it panic???
            .map(|key| {
                (
                    key,
                    key_to_vis_maps.remove(key).unwrap().try_into().unwrap(),
                )
            })
            .collect_vec();

        Ok(result)
    }
}

impl<S: StateStore> HashAggExecutor<S> {
    /// Flush the buffered chunk to the storage backend, and get the edits of the states.
    async fn flush_data(&mut self) -> Result<StreamChunk> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.

        let mut write_batch = vec![];
        let mut dirty_cnt = 0;

        for (_, states) in self.state_map.iter_mut() {
            if states.is_dirty() {
                dirty_cnt += 1;
                for state in &mut states.managed_states {
                    state.flush(&mut write_batch).unwrap();
                }
            }
        }

        self.keyspace
            .state_store()
            .ingest_batch(write_batch)
            .await?;

        // --- Produce the stream chunk ---

        let key_len = self.key_indices.len();
        let dirty_cnt = dirty_cnt;

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let datatypes = self
            .schema
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();

        let mut builders = datatypes
            .iter()
            .map(|ty| ty.create_array_builder(dirty_cnt * 2).unwrap())
            .collect_vec();

        // --- Retrieve modified states and put the changes into the builders ---

        let mut new_ops = Vec::with_capacity(dirty_cnt);

        let mut keys_to_delete = vec![];

        for (key, states) in self.state_map.iter_mut() {
            if !states.is_dirty() {
                continue;
            }

            // We assume the first state of aggregation is always `StreamingRowCountAgg`.
            let row_count = states.row_count().await?;

            if row_count == 0 {
                keys_to_delete.push(key.clone());
            }

            let prev_row_count = states.prev_row_count();

            match (prev_row_count, row_count) {
                (0, 0) => {
                    // previous state is empty, current state is also empty.
                    continue;
                }
                (0, _) => {
                    // previous state is empty, current state is not empty, insert one `Insert` op.
                    new_ops.push(Op::Insert);
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                    for (builder, state) in &mut builders[key_len..]
                        .iter_mut()
                        .zip(states.managed_states.iter_mut())
                    {
                        builder.append_datum(&state.get_output().await?)?;
                    }
                }
                (_, 0) => {
                    // previous state is not empty, current state is empty, insert one `Delete` op.
                    new_ops.push(Op::Delete);
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                    for (builder, state) in &mut builders[key_len..]
                        .iter_mut()
                        .zip(states.prev_states.as_ref().unwrap().iter())
                    {
                        builder.append_datum(state)?;
                    }
                }
                _ => {
                    // previous state is not empty, current state is not empty, insert two `Update`
                    // op.
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                        builder.append_datum(datum)?;
                    }
                    for (builder, prev_state, cur_state) in itertools::multizip((
                        builders[key_len..].iter_mut(),
                        states.prev_states.as_ref().unwrap().iter(),
                        states.managed_states.iter_mut(),
                    )) {
                        builder.append_datum(prev_state)?;
                        builder.append_datum(&cur_state.get_output().await?)?;
                    }
                }
            }

            // unmark dirty
            states.prev_states = None;
        }

        // vacuum unused states
        // TODO: find better way to evict empty states and clean state
        // In current implementation, we need to fetch the RowCount from the state store once a key
        // is deleted and added again. We should find a way to eliminate this extra fetch.
        for key_to_delete in keys_to_delete {
            self.state_map.remove(&key_to_delete);
        }

        let columns: Vec<Column> = builders
            .into_iter()
            .zip(datatypes.into_iter())
            .map(|(builder, data_type)| -> Result<_> {
                Ok(Column::new(Arc::new(builder.finish()?), data_type))
            })
            .try_collect()?;

        let chunk = StreamChunk {
            columns,
            ops: new_ops,
            visibility: None,
        };

        Ok(chunk)
    }

    /// Apply the chunk to the dirty state.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        let total_num_rows = ops.len();

        // --- Retrieve grouped keys into Row format ---
        let mut keys = vec![Row(vec![]); total_num_rows];

        // we retrieve the key column-by-column
        for key_idx in &self.key_indices {
            let col = &columns[*key_idx];

            // for each column, we push all items in that column into the `keys` vector.
            for (row_idx, key) in keys.iter_mut().enumerate() {
                key.0.push(col.array().datum_at(row_idx));
            }
        }

        // --- Find unique keys in this batch and generate visibility map for each key ---
        // TODO: this might be inefficient if there are not too many duplicated keys in one batch.
        let unique_keys = self.get_unique_keys(&keys, &visibility)?;

        // --- Retrieve all aggregation inputs in advance ---
        // Previously, this is done in `unique_keys` inner loop, which is very inefficient.
        let all_agg_input_arrays = self
            .agg_calls
            .iter()
            .map(|agg| {
                agg.args
                    .val_indices()
                    .iter()
                    .map(|val_idx| columns[*val_idx].array_ref())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (key, vis_map) in unique_keys {
            // 1. Retrieve previous state from the KeyedState. If they didn't exist, the
            // ManagedState will automatically create new ones for them.

            if !self.state_map.contains_key(key) {
                let mut managed_states = vec![];

                let mut row_count = None;
                for (idx, agg_call) in self.agg_calls.iter().enumerate() {
                    // TODO: in pure in-memory engine, we should not do this serialization.
                    let mut encoded_group_key = key.serialize().unwrap();
                    encoded_group_key.push(b'/');
                    encoded_group_key.put_u16(idx as u16);
                    encoded_group_key.push(b'/');

                    // The prefix of the state is <group key / state id />
                    let keyspace = self.keyspace.keyspace(&encoded_group_key);
                    let mut managed_state;
                    if idx == 0 {
                        // For the rowcount state, we should record the rowcount.
                        managed_state = ManagedState::Value(
                            ManagedValueState::new(agg_call.clone(), keyspace, None).await?,
                        );
                        let output = managed_state.get_output().await?;
                        row_count =
                            Some(output.as_ref().map(|x| *x.as_int64() as usize).unwrap_or(0));
                    } else {
                        // For other states, we can feed the previously-read rowcount into the
                        // managed state.
                        managed_state = ManagedState::Value(
                            ManagedValueState::new(agg_call.clone(), keyspace, row_count).await?,
                        );
                    }
                    managed_states.push(managed_state);
                }
                self.state_map.insert(
                    key.clone(),
                    HashValue {
                        managed_states,
                        prev_states: None,
                    },
                );
            }
            let states = self.state_map.get_mut(key).unwrap();

            // 2. Mark the state as dirty by filling prev states
            if !states.is_dirty() {
                let mut outputs = vec![];
                for state in states.managed_states.iter_mut() {
                    outputs.push(state.get_output().await?);
                }
                states.prev_states = Some(outputs);
            }

            // 3. Apply batch to each of the state

            for (agg_state, input_arrays) in states
                .managed_states
                .iter_mut()
                .zip(all_agg_input_arrays.iter())
            {
                if input_arrays.is_empty() {
                    agg_state.apply_batch(&ops, Some(&vis_map), &[]).await?;
                } else {
                    agg_state
                        .apply_batch(&ops, Some(&vis_map), &[input_arrays[0]])
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<S: StateStore> Executor for HashAggExecutor<S> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn next(&mut self) -> Result<Message> {
        if let Some(barrier) = self.next_barrier_message {
            self.next_barrier_message = None;
            return Ok(Message::Barrier(Barrier {
                epoch: barrier.epoch,
                stop: barrier.stop,
            }));
        }
        while let Ok(msg) = self.input.next().await {
            match msg {
                Message::Chunk(chunk) => self.apply_chunk(chunk).await?,
                Message::Barrier(barrier) => {
                    if barrier.stop {
                        return Ok(Message::Barrier(barrier));
                    }
                    let is_dirty = self.state_map.iter().any(|(_, v)| v.is_dirty());
                    if !is_dirty {
                        // No fresh data need to flush, just forward the barrier.
                        return Ok(Message::Barrier(barrier));
                    }
                    // Cache the barrier_msg and send it later.
                    self.next_barrier_message = Some(barrier);
                    let chunk = self.flush_data().await?;
                    return Ok(Message::Chunk(chunk));
                }
            }
        }
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use super::super::keyspace::MemoryStateStore;
    use super::*;
    use crate::stream_op::test_utils::*;
    use crate::stream_op::*;
    use crate::*;
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::data_chunk_iter::Row;
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::Field;
    use risingwave_common::expr::*;
    use risingwave_common::types::{Int64Type, Scalar};

    fn create_in_memory_keyspace() -> Keyspace<impl StateStore> {
        Keyspace::new(MemoryStateStore::new(), b"test_executor_2333".to_vec())
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

    async fn test_local_hash_aggregation_count(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 2] }],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Delete, Op::Delete, Op::Delete],
            columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 2] }],
            visibility: Some((vec![true, false, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![Field {
                data_type: Int64Type::create(false),
            }],
        };
        let mut source = MockSource::new(schema);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(1, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(2, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: Int64Type::create(false),
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::Unary(Int64Type::create(false), 0),
                return_type: Int64Type::create(false),
            },
            // This is local hash aggregation, so we add another row count state
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: Int64Type::create(false),
            },
        ];

        let schema = generate_hash_agg_schema(&source, &agg_calls, &keys);
        let mut hash_agg =
            HashAggExecutor::new(Box::new(source), agg_calls, keys, keyspace, schema);

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
                .zip(data_chunk.rows().map(Row::from))
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
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
                column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
                column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
            ],
            visibility: Some((vec![true, false, true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let mut source = MockSource::new(schema);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(1, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(2, false);

        // This is local hash aggregation, so we add another sum state
        let key_indices = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: Int64Type::create(false),
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Int64Type::create(false), 1),
                return_type: Int64Type::create(false),
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Int64Type::create(false), 2),
                return_type: Int64Type::create(false),
            },
        ];

        let schema = generate_hash_agg_schema(&source, &agg_calls, &key_indices);
        let mut hash_agg =
            HashAggExecutor::new(Box::new(source), agg_calls, key_indices, keyspace, schema);

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip(data_chunk.rows().map(Row::from))
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
                .zip(data_chunk.rows().map(Row::from))
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
}
