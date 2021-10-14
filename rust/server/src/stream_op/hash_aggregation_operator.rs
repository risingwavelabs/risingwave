//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::{Message, Op, SimpleStreamOperator, StreamChunk, StreamOperator};
use crate::array::column::Column;
use crate::array::*;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result, RwError};
use crate::impl_consume_barrier_default;
use crate::types::Datum;

use super::AggCall;
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;

pub type HashKey = Vec<Datum>;

pub struct HashValue {
    /// indicating whether this is a key saw by the first time
    /// if it is the first time, the aggregation operator would only
    /// output one row for it, e.g. `Insert`.
    /// if it is not the first time, the aggregation operator would
    /// output two rows for it, e.g. `UpdateDelete` and `UpdateInsert`
    first_data: bool,
    /// one or more aggregation states, all corresponding to the same key
    agg_states: Vec<Box<dyn StreamingAggStateImpl>>,
}

impl HashValue {
    pub fn new(first_data: bool, agg_states: Vec<Box<dyn StreamingAggStateImpl>>) -> Self {
        HashValue {
            first_data,
            agg_states,
        }
    }

    fn is_first_data(&self) -> bool {
        self.first_data
    }

    fn unset_first_data(&mut self) {
        self.first_data = false;
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
    /// Aggregation state of the current operator
    state_entries: HashMap<HashKey, HashValue>,
    /// The input of the current operator
    input: Box<dyn StreamOperator>,
    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,
    /// Indices of the columns
    /// all of the aggregation functions in this operator should depend on same group of keys
    key_indices: Vec<usize>,
}

impl HashAggExecutor {
    pub fn new(
        input: Box<dyn StreamOperator>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
    ) -> Self {
        Self {
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
    pub fn get_unique_keys<'a, 'b>(
        &self,
        keys: &'a [HashKey],
        visibility: &Option<Bitmap>,
        state_entries: &HashMap<HashKey, HashValue>,
    ) -> Result<(Vec<&'b HashKey>, Vec<usize>, HashMap<&'b HashKey, Bitmap>)>
    where
        'a: 'b,
    {
        let total_num_rows = keys.len();
        // Each hash key, e.g. `key1` corresponds to a vis map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the `input`
        // The vis map of each hash key will be passed into StreamingAggStateImpl
        let mut key_to_vis_maps = HashMap::new();
        // Some grouped keys are the same and their corresponding rows will be collapsed together after aggregation.
        // This vec records the row indices so that we can pick these rows from all the arrays into output chunk.
        // Some row may get recorded twice as its key may produce two rows `UpdateDelete` and `UpdateInsert` in the final chunk.
        let mut distinct_rows = Vec::new();
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
                // Check whether the `key` shows up first time in the whole history, if not,
                // then we need a row for `UpdateDelete` and another row for `UpdateInsert`.
                // Otherwise, we will have only one row for `Insert`.
                if state_entries.contains_key(key) {
                    distinct_rows.push(row_idx);
                }
                distinct_rows.push(row_idx);
                unique_keys.push(key);
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        // turn the Vec of bool into Bitmap
        let key_to_vis_maps = key_to_vis_maps
            .into_iter()
            .map(|(key, vis_map)| Ok((key, Bitmap::from_vec(vis_map)?)))
            .collect::<Result<HashMap<&HashKey, Bitmap>>>();

        Ok((unique_keys, distinct_rows, key_to_vis_maps?))
    }
}

impl_consume_barrier_default!(HashAggExecutor, StreamOperator);

impl SimpleStreamOperator for HashAggExecutor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
        } = chunk;

        let total_num_rows = ops.len();
        // get the grouped keys for each row
        let mut keys = vec![vec![]; total_num_rows];
        for key_idx in &self.key_indices {
            let col = &arrays[*key_idx];
            for (row_idx, key) in keys.iter_mut().enumerate() {
                key.push(col.array().scalar_value_at(row_idx));
            }
        }

        let (unique_keys, distinct_rows, mut key_to_vis_maps) =
            self.get_unique_keys(&keys, &visibility, &self.state_entries)?;

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

            // check existence to avoid paying the cost of copy in `entry(...).or_insert()` everytime
            if !self.state_entries.contains_key(key) {
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
                let hash_value = HashValue::new(true, agg_states);
                self.state_entries.insert(key.to_vec(), hash_value);
            }

            // since we just checked existence, the key must exist so we `unwrap` directly
            let value = self.state_entries.get_mut(key).unwrap();
            let mut builders = value.new_builders();
            if !value.is_first_data() {
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
                        .map(|val_idx| arrays[*val_idx].array_ref())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            value.apply_batch(&ops, Some(&cur_vis_map), &all_agg_input_arrays)?;

            // output the current state into builder
            value.record_states(&mut builders)?;

            agg_array_builders
                .iter_mut()
                .zip(builders.into_iter())
                .try_for_each(|(agg_array_builder, builder)| {
                    agg_array_builder.append_array(&builder.finish()?)
                })?;

            // same logic from `simple` LocalAggregationOperator
            if !value.is_first_data() {
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);
            } else {
                new_ops.push(Op::Insert);
            }

            value.unset_first_data();
            Ok(())
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

        // compose all the columns together
        // rows with the same key collapse into one or two rows
        let mut new_columns = Vec::new();
        for key_idx in &self.key_indices {
            let column = &arrays[*key_idx];
            let mut array_builder = column
                .data_type()
                .create_array_builder(distinct_rows.len())?;
            for row_idx in &distinct_rows {
                array_builder.append_scalar_ref(column.array_ref().value_at(*row_idx))?;
            }
            new_columns.push(Column::new(
                Arc::new(array_builder.finish()?),
                column.data_type(),
            ));
        }
        new_columns.extend(agg_columns.into_iter());

        let chunk = StreamChunk {
            ops: new_ops,
            visibility: None,
            columns: new_columns,
        };
        Ok(Message::Chunk(chunk))
    }
}
