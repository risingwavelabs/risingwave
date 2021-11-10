//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::AggCall;
use super::{Executor, Message, Op, StreamChunk};
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_pb::data::Barrier;
use std::collections::HashMap;
use std::sync::Arc;

pub type HashKey = Vec<Datum>;

#[derive(Clone, Debug)]
struct HashValue {
    /// one or more aggregation states, all corresponding to the same key
    agg_states: Vec<Box<dyn StreamingAggStateImpl>>,
}

impl HashValue {
    pub fn new(agg_states: Vec<Box<dyn StreamingAggStateImpl>>) -> Self {
        HashValue { agg_states }
    }

    fn record_states(&self, agg_array_builder: &mut [ArrayBuilderImpl]) -> Result<()> {
        self.agg_states
            .iter()
            .zip(agg_array_builder.iter_mut())
            .try_for_each(|(agg_state, agg_array_builder)| agg_state.get_output(agg_array_builder))
    }

    fn apply_batch(
        &mut self,
        ops: &[Op],
        visibility: Option<&Bitmap>,
        all_agg_input_arrays: &[Vec<&ArrayImpl>],
    ) -> Result<()> {
        self.agg_states
            .iter_mut()
            .zip(all_agg_input_arrays.iter())
            .try_for_each(|(agg_state, input_arrays)| {
                if input_arrays.is_empty() {
                    agg_state.apply_batch(ops, visibility, &[])
                } else {
                    agg_state.apply_batch(ops, visibility, &[input_arrays[0]])
                }
            })
    }

    fn new_builders(&self) -> Vec<ArrayBuilderImpl> {
        self.agg_states
            .iter()
            .map(|agg_state| agg_state.new_builder())
            .collect::<Vec<_>>()
    }
}

pub struct HashAggExecutor {
    schema: Schema,

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    // TODO: Create a `Barrier` struct.
    next_barrier_message: Option<Barrier>,

    /// Aggregation state before last barrier.
    /// The map will be updated iff [`Message::Barrier`] was received.
    /// TODO: We will use state backend instead later.
    prev_state_entries: HashMap<HashKey, HashValue>,
    /// Aggregation state after last barrier.
    dirty_state_entries: HashMap<HashKey, (Option<HashValue>, HashValue)>,
    /// The input of the current executor
    input: Box<dyn Executor>,
    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,
    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    key_indices: Vec<usize>,
}

impl HashAggExecutor {
    pub fn new(input: Box<dyn Executor>, agg_calls: Vec<AggCall>, key_indices: Vec<usize>) -> Self {
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
        Self {
            schema: Schema { fields },
            next_barrier_message: None,
            prev_state_entries: HashMap::new(),
            dirty_state_entries: HashMap::new(),
            input,
            agg_calls,
            key_indices,
        }
    }

    /// `keys` are Hash Keys of all the rows
    /// `visibility`, leave invisible ones out of aggregation
    /// `state_entries`, the current state to check whether a key has existed or not
    fn get_unique_keys<'a, 'b>(
        &self,
        keys: &'a [HashKey],
        visibility: &Option<Bitmap>,
    ) -> Result<(Vec<&'b HashKey>, HashMap<&'b HashKey, Bitmap>)>
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

        // turn the Vec of bool into Bitmap
        let key_to_vis_maps = key_to_vis_maps
            .into_iter()
            .map(|(key, vis_map)| Ok((key, (vis_map).try_into()?)))
            .collect::<Result<HashMap<&HashKey, Bitmap>>>();

        Ok((unique_keys, key_to_vis_maps?))
    }
}

impl HashAggExecutor {
    fn flush_data(&mut self) -> Result<StreamChunk> {
        let dirty_state_entries = std::mem::take(&mut self.dirty_state_entries);
        let dirty_cnt = dirty_state_entries.len();
        // TODO: if the dirty_cnt is large, consider break to multiple chunks.
        let data_types = self
            .schema
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();
        let mut agg_builders = data_types
            .iter()
            .map(|data_type| data_type.clone().create_array_builder(dirty_cnt))
            .collect::<Result<Vec<_>>>()?;
        let key_len = self.key_indices.len();
        let mut new_ops = Vec::with_capacity(dirty_cnt);
        for (key, (prev_value, cur_value)) in dirty_state_entries.into_iter() {
            // These builders are for storing the aggregated value for each aggregation function.
            match prev_value {
                None => {
                    // We assume the first state of aggregation is always `StreamingRowCountAgg`.
                    let row_cnt = {
                        let mut builder = ArrayBuilderImpl::Int64(I64ArrayBuilder::new(1)?);
                        cur_value.agg_states[0].get_output(&mut builder)?;
                        builder.finish()?.as_int64().value_at(0).unwrap()
                    };
                    if row_cnt != 0 {
                        new_ops.push(Op::Insert);
                        for (builder, datum) in agg_builders.iter_mut().zip(key.iter()) {
                            builder.append_datum(datum)?;
                        }
                        for (builder, state) in agg_builders[key_len..]
                            .iter_mut()
                            .zip(cur_value.agg_states.iter())
                        {
                            state.get_output(builder)?;
                        }
                        self.prev_state_entries.insert(key, cur_value);
                    }
                }
                Some(prev_value) => {
                    // We assume the first state of aggregation is always `StreamingRowCountAgg`.
                    let row_cnt = {
                        let mut builder = ArrayBuilderImpl::Int64(I64ArrayBuilder::new(1)?);
                        cur_value.agg_states[0].get_output(&mut builder)?;
                        builder.finish()?.as_int64().value_at(0).unwrap()
                    };
                    if row_cnt == 0 {
                        new_ops.push(Op::Delete);
                        for (builder, datum) in agg_builders.iter_mut().zip(key.iter()) {
                            builder.append_datum(datum)?;
                        }
                        for (builder, state) in agg_builders[key_len..]
                            .iter_mut()
                            .zip(prev_value.agg_states.iter())
                        {
                            state.get_output(builder)?;
                        }
                        self.prev_state_entries.remove(&key);
                    } else {
                        new_ops.push(Op::UpdateDelete);
                        new_ops.push(Op::UpdateInsert);
                        for (builder, datum) in agg_builders.iter_mut().zip(key.iter()) {
                            builder.append_datum(datum)?;
                            builder.append_datum(datum)?;
                        }
                        for (builder, prev_state, cur_state) in itertools::multizip((
                            agg_builders[key_len..].iter_mut(),
                            prev_value.agg_states.iter(),
                            cur_value.agg_states.iter(),
                        )) {
                            prev_state.get_output(builder)?;
                            cur_state.get_output(builder)?;
                        }
                        self.prev_state_entries.insert(key, cur_value);
                    }
                }
            };
        }

        let columns: Vec<Column> = agg_builders
            .into_iter()
            .zip(data_types.into_iter())
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
    fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        let total_num_rows = ops.len();
        // get the grouped keys for each row
        let mut keys = vec![vec![]; total_num_rows];
        let mut key_array_builders = Vec::with_capacity(self.key_indices.len());
        let mut key_data_types = Vec::with_capacity(self.key_indices.len());
        for key_idx in &self.key_indices {
            let col = &columns[*key_idx];
            for (row_idx, key) in keys.iter_mut().enumerate() {
                key.push(col.array().datum_at(row_idx));
            }
            key_data_types.push(col.data_type());
            key_array_builders.push(col.data_type().create_array_builder(0)?);
        }

        let (unique_keys, mut key_to_vis_maps) = self.get_unique_keys(&keys, &visibility)?;

        unique_keys.into_iter().try_for_each(|key| -> Result<()> {
            let cur_vis_map = key_to_vis_maps.remove(key).ok_or_else(|| {
                ErrorCode::InternalError(format!("Visibility does not exist for key {:?}", key))
            })?;

            if self.dirty_state_entries.contains_key(key) {
            } else if let Some(prev_value) = self.prev_state_entries.get(key) {
                self.dirty_state_entries
                    .insert(key.to_vec(), (Some(prev_value.clone()), prev_value.clone()));
            } else {
                let agg_states = self
                    .agg_calls
                    .iter()
                    .map(|agg| {
                        create_streaming_agg_state(
                            agg.args.arg_types(),
                            &agg.kind,
                            &agg.return_type,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let hash_value = HashValue::new(agg_states);
                self.dirty_state_entries
                    .insert(key.to_vec(), (None, hash_value));
            };

            // since we just checked existence, the key must exist so we `unwrap` directly
            let (_, value) = self.dirty_state_entries.get_mut(key).unwrap();

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

            value.apply_batch(&ops, Some(&cur_vis_map), &all_agg_input_arrays)
        })
    }
}

#[async_trait]
impl Executor for HashAggExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn next(&mut self) -> Result<Message> {
        if let Some(barrier) = self.next_barrier_message.clone() {
            self.next_barrier_message = None;
            return Ok(Message::Barrier {
                epoch: barrier.epoch,
                stop: barrier.stop,
            });
        }
        while let Ok(msg) = self.input.next().await {
            match msg {
                Message::Chunk(chunk) => self.apply_chunk(chunk)?,
                Message::Barrier { epoch, stop } => {
                    let dirty_cnt = self.dirty_state_entries.len();
                    if dirty_cnt == 0 {
                        // No fresh data need to flush, just forward the barrier.
                        return Ok(Message::Barrier { epoch, stop });
                    }
                    // Cache the barrier_msg and send it later.
                    self.next_barrier_message = Some(Barrier { epoch, stop });
                    let chunk = self.flush_data()?;
                    return Ok(Message::Chunk(chunk));
                }
                m @ Message::Terminate => return Ok(m),
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
    use risingwave_common::array::I64Array;
    use risingwave_common::catalog::Field;
    use risingwave_common::expr::*;
    use risingwave_common::types::{Int64Type, Scalar};

    #[tokio::test]
    async fn test_local_hash_aggregation_count() {
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

        let mut hash_agg = HashAggExecutor::new(Box::new(source), agg_calls, keys);

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

    #[tokio::test]
    async fn test_global_hash_aggregation_count() {
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
        let mut hash_agg = HashAggExecutor::new(Box::new(source), agg_calls, key_indices);

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
