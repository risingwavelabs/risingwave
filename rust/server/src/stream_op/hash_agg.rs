//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::{AggCall, Barrier, Executor, Message};
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, Row};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use std::collections::HashMap;
use std::sync::Arc;

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
pub struct HashAggExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, AggStateSerializer>,
{
    /// Schema of the executor.
    schema: Schema,

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    // TODO: Create a `Barrier` struct.
    next_barrier_message: Option<Barrier>,

    /// Aggregation state before last barrier.
    /// The map will be updated iff [`Message::Barrier`] was received.
    state_entries: StateBackend,

    /// Keys modified in one epoch. This HashMap stores the states before an epoch begins.
    modified_states: HashMap<HashKey, Option<HashValue>>,

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

impl<StateBackend> HashAggExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, AggStateSerializer>,
{
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        state_entries: StateBackend,
        schema: Schema,
    ) -> Self {
        Self {
            next_barrier_message: None,
            state_entries,
            input,
            agg_calls,
            schema,
            key_indices,
            modified_states: HashMap::new(),
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

impl<StateBackend> HashAggExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, AggStateSerializer>,
{
    /// Flush the buffered chunk to the storage backend, and get the edits of the states.
    async fn flush_data(&mut self) -> Result<StreamChunk> {
        let dirty_cnt = self.modified_states.len();
        let key_len = self.key_indices.len();

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

        for (key, prev_states) in self.modified_states.drain() {
            let cur_states = self.state_entries.get(&key).await?;
            // If a state is modified in this epoch, it must be available from the state entries.
            let cur_states = if let Some(cur_states) = cur_states {
                cur_states
            } else {
                return Err(ErrorCode::InternalError(format!(
            "state for key {:?} not found in state backend, this might be a bug of state store.",
            key
          ))
                .into());
            };

            // We assume the first state of aggregation is always `StreamingRowCountAgg`.
            let row_cnt = {
                get_one_output_from_state_impl(&*cur_states.agg_states[0])?
                    .as_int64()
                    .value_at(0)
                    .unwrap()
            };

            if row_cnt == 0 {
                self.state_entries.delete(&key);
            }

            match prev_states {
                None if row_cnt == 0 => {
                    // previous state is empty, current state is also empty.
                    continue;
                }
                None => {
                    // previous state is empty, current state is not empty, insert one `Insert` op.
                    new_ops.push(Op::Insert);
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                    for (builder, state) in &mut builders[key_len..]
                        .iter_mut()
                        .zip(cur_states.agg_states.iter())
                    {
                        state.get_output(builder)?;
                    }
                }
                Some(prev_states) if row_cnt == 0 => {
                    // previous state is not empty, current state is not empty, insert one `Delete`
                    // op.
                    new_ops.push(Op::Delete);
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                    for (builder, state) in &mut builders[key_len..]
                        .iter_mut()
                        .zip(prev_states.agg_states.iter())
                    {
                        state.get_output(builder)?;
                    }
                }
                Some(prev_states) => {
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
                        prev_states.agg_states.iter(),
                        cur_states.agg_states.iter(),
                    )) {
                        prev_state.get_output(builder)?;
                        cur_state.get_output(builder)?;
                    }
                }
            }
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

        self.state_entries.flush().await?;

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
            // Retrieve previous state from the KeyedState.
            let prev_state = self.state_entries.get(key).await?;

            // Mark the key as dirty, and store state, so that we can know what change to output
            // when flushing chunks. If the key is already marked dirty, we do not need
            // to do anything here.
            //
            // For example, now we find [1, 2, 3]'s previous state, we need to go through
            // the following steps:
            //
            // 1. Check if `[1, 2, 3]` is already in `modified_states`. If so, do not change the
            // already-stored state and go ahead. Otherwise, store the state in `modified_states`,
            // thus marking the key as dirty.
            // 2. If we find that the previous state is empty, create a new state for the key.
            // 3. Apply the aggregation, and store the state back to the `state_entries`.

            // Store the key and mark it as dirty.
            if !self.modified_states.contains_key(key) {
                // Store the previous state
                self.modified_states.insert(key.clone(), prev_state.clone());
            }

            // Create new state entries for keys not existing before.
            let mut state = match prev_state {
                Some(prev_state) => prev_state,
                None => {
                    // The previous state is not existing, create new states.
                    let agg_states = self
                        .agg_calls
                        .iter()
                        .map(|agg| {
                            create_streaming_agg_state(
                                agg.args.arg_types(),
                                &agg.kind,
                                &agg.return_type,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    HashValue::new(agg_states)
                }
            };

            // Apply the batch to the state.
            state.apply_batch(&ops, Some(&vis_map), &all_agg_input_arrays)?;

            // Store the state back to the state backend.
            self.state_entries.put(key.clone(), state);
        }
        Ok(())
    }
}

#[async_trait]
impl<StateBackend> Executor for HashAggExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, AggStateSerializer>,
{
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
                    let dirty_cnt = self.modified_states.len();
                    if dirty_cnt == 0 {
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

    fn create_in_memory_keyed_state(
        schema: &Schema,
        agg_calls: &[AggCall],
    ) -> impl KeyedState<RowSerializer, AggStateSerializer> {
        InMemoryKeyedState::new(
            RowSerializer::new(schema.clone()),
            AggStateSerializer::new(agg_calls.to_vec()),
        )
    }

    fn create_serialized_keyed_state(
        schema: &Schema,
        agg_calls: &[AggCall],
    ) -> impl KeyedState<RowSerializer, AggStateSerializer> {
        SerializedKeyedState::new(
            RowSerializer::new(schema.clone()),
            AggStateSerializer::new(agg_calls.to_vec()),
        )
    }

    // --- Test HashAgg with in-memory KeyedState ---

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory() {
        test_local_hash_aggregation_count(create_in_memory_keyed_state).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory() {
        test_global_hash_aggregation_count(create_in_memory_keyed_state).await
    }

    // --- Test HashAgg with in-memory KeyedState with serialization ---
    // TODO: remove this when we have Hummock state backend

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory_serialized() {
        test_local_hash_aggregation_count(create_serialized_keyed_state).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory_serialized() {
        test_global_hash_aggregation_count(create_serialized_keyed_state).await
    }

    async fn test_local_hash_aggregation_count<F, KS>(create_keyed_state: F)
    where
        F: Fn(&Schema, &[AggCall]) -> KS,
        KS: KeyedState<RowSerializer, AggStateSerializer>,
    {
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
        let keyed_state = create_keyed_state(&schema, &agg_calls);
        let mut hash_agg =
            HashAggExecutor::new(Box::new(source), agg_calls, keys, keyed_state, schema);

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

    async fn test_global_hash_aggregation_count<F, KS>(create_keyed_state: F)
    where
        F: Fn(&Schema, &[AggCall]) -> KS,
        KS: KeyedState<RowSerializer, AggStateSerializer>,
    {
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
        let keyed_state = create_keyed_state(&schema, &agg_calls);
        let mut hash_agg = HashAggExecutor::new(
            Box::new(source),
            agg_calls,
            key_indices,
            keyed_state,
            schema,
        );

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
