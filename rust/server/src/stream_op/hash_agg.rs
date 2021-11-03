//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::{Executor, Message, Op, SimpleExecutor, StreamChunk};
use risingwave_common::array::column::Column;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{Datum, ScalarRefImpl};

use super::AggCall;
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;

pub type HashKey = Vec<Datum>;

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
    /// Aggregation state of the current operator
    state_entries: HashMap<HashKey, HashValue>,
    /// The input of the current operator
    input: Box<dyn Executor>,
    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,
    /// Indices of the columns
    /// all of the aggregation functions in this operator should depend on same group of keys
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
            state_entries: HashMap::new(),
            input,
            agg_calls,
            key_indices,
        }
    }

    /// `keys` are Hash Keys of all the rows
    /// `visibility`, leave invisible ones out of aggregation
    /// `state_entries`, the current state to check whether a key has existed or not
    #[allow(clippy::complexity)]
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
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the `input`
        // The vis map of each hash key will be passed into StreamingAggStateImpl
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

#[async_trait]
impl Executor for HashAggExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SimpleExecutor for HashAggExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
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

        // These builders are for storing the aggregated value for each aggregation function
        let mut agg_array_builders: Vec<ArrayBuilderImpl> = self
            .agg_calls
            .iter()
            .map(|agg_call| agg_call.return_type.clone().create_array_builder(0))
            .try_collect()?;
        let mut new_ops = Vec::new();

        unique_keys.into_iter().try_for_each(|key| -> Result<()> {
            let cur_vis_map = key_to_vis_maps.remove(key).ok_or_else(|| {
                ErrorCode::InternalError(format!("Visibility does not exist for key {:?}", key))
            })?;

            let not_first_data = self.state_entries.contains_key(key);

            // check existence to avoid paying the cost of copy in `entry(...).or_insert()` everytime
            if !not_first_data {
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
                self.state_entries.insert(key.to_vec(), hash_value);
            }
            // since we just checked existence, the key must exist so we `unwrap` directly
            let value = self.state_entries.get_mut(key).unwrap();
            let mut builders = value.new_builders();
            if not_first_data {
                // record the last state into builder
                value.record_states(&mut builders)?;
            }

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

            value.apply_batch(&ops, Some(&cur_vis_map), &all_agg_input_arrays)?;

            let mut builder = value.agg_states[0].new_builder();
            value.agg_states[0].get_output(&mut builder).unwrap();
            let num = &builder.finish()?;
            if let ScalarRefImpl::Int64(row_cnt) = num.value_at(0).unwrap() {
                // output the current state into builder
                if row_cnt == 0 {
                    // remove the kv pair
                    self.state_entries.remove(key);
                    new_ops.push(Op::Delete);
                    key_array_builders
                        .iter_mut()
                        .zip(key.iter())
                        .try_for_each(|(key_col, datum)| key_col.append_datum(datum))?;
                }
                // same logic from [`super::SimpleAggExecutor`]
                else if not_first_data {
                    // output the current state into builder
                    value.record_states(&mut builders)?;
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                    key_array_builders.iter_mut().zip(key.iter()).try_for_each(
                        |(key_col, datum)| {
                            key_col.append_datum(datum)?;
                            key_col.append_datum(datum)
                        },
                    )?;
                } else {
                    // output the current state into builder
                    value.record_states(&mut builders)?;
                    new_ops.push(Op::Insert);
                    key_array_builders
                        .iter_mut()
                        .zip(key.iter())
                        .try_for_each(|(key_col, datum)| key_col.append_datum(datum))?;
                }

                agg_array_builders
                    .iter_mut()
                    .zip(builders.into_iter())
                    .try_for_each(|(agg_array_builder, builder)| {
                        agg_array_builder.append_array(&builder.finish()?)
                    })?;

                Ok(())
            } else {
                panic!("Should be Int64 type as row_count is supposed to be here");
            }
        })?;

        // all the columns of aggregated value
        let agg_columns: Vec<Column> = agg_array_builders
            .into_iter()
            .zip(self.agg_calls.iter().map(|agg| agg.return_type.clone()))
            .map(|(agg_array_builder, return_type)| {
                Ok::<_, RwError>(Column::new(
                    Arc::new(agg_array_builder.finish()?),
                    return_type.clone(),
                ))
            })
            .try_collect()?;

        let mut new_columns: Vec<Column> = key_array_builders
            .into_iter()
            .zip(key_data_types.into_iter())
            .map(|(builder, dt)| -> Result<Column> {
                Ok(Column::new(Arc::new(builder.finish()?), dt))
            })
            .try_collect()?;
        new_columns.extend(agg_columns.into_iter());

        let chunk = StreamChunk {
            ops: new_ops,
            visibility: None,
            columns: new_columns,
        };
        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {

    use crate::stream_op::test_utils::*;
    use crate::stream_op::*;
    use crate::*;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::Field;
    use risingwave_common::expr::*;
    use risingwave_common::types::Int64Type;

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
        let source = MockSource::with_chunks(schema, vec![chunk1, chunk2]);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
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

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert]);

            assert_eq!(chunk.columns.len(), 3);
            // test key
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2)]
            );
            // test count first row
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2)]
            );
            // test count(*)
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2)]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
            assert_eq!(
                chunk.ops,
                vec![Op::Delete, Op::UpdateDelete, Op::UpdateInsert]
            );

            assert_eq!(chunk.columns.len(), 3);
            // test key
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(2)]
            );
            // test count first row
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(1)]
            );
            // test count(*)
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(1)]
            );
        } else {
            unreachable!();
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
        let source = MockSource::with_chunks(schema, vec![chunk1, chunk2]);

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
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert]);

            assert_eq!(chunk.columns.len(), 4);
            // test key_column
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2)],
            );
            // test agg_column
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(4)]
            );
            // test row_sum_column
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(4)]
            );
        } else {
            unreachable!();
        }

        if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
            assert_eq!(
                chunk.ops,
                vec![Op::Delete, Op::UpdateDelete, Op::UpdateInsert, Op::Insert,]
            );

            assert_eq!(chunk.columns.len(), 4);

            // test key_column
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(2), Some(3)]
            );
            // test row_count
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(1), Some(1)]
            );
            // test agg_column
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(4), Some(2), Some(3),]
            );
            // test row_sum_column
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(4), Some(2), Some(3),]
            );
        } else {
            unreachable!();
        }
    }
}
