//! Global Streaming Hash Aggregators

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::AggKind;
use crate::impl_consume_barrier_default;
use crate::types::{DataTypeRef, Datum};

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
                agg_state.apply_batch(ops, visibility, input_arrays[0])
            })
    }

    fn new_builders(&self) -> Vec<ArrayBuilderImpl> {
        self.agg_states
            .iter()
            .map(|agg_state| agg_state.new_builder())
            .collect::<Vec<_>>()
    }
}

pub struct HashAggregationOperator {
    /// Aggregation state of the current operator
    /// `Vec<HashKey>`
    state_entries: HashMap<HashKey, HashValue>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// the inner vector due to aggregate functions that supports more than one input
    /// the outer vector due to multi-state support
    input_types: Vec<Option<DataTypeRef>>,
    /// vector due to multi-state support
    return_types: Vec<DataTypeRef>,
    /// Indices of the columns
    /// all of the aggregation functions in this operator should depend on same group of keys
    key_indices: Vec<usize>,
    /// Index of the column being aggregated.
    /// the inner vector due to multiple arguments of the aggregation function
    /// the outer vector due to multi-state support
    /// TODO: Need to change after expression support
    val_indices: Vec<Vec<usize>>,
    /// Aggregation Kind for constructing StreamingAggStateImpl
    /// TODO: Use Streaming specific AggKind instead of borrowing from OLAP
    agg_types: Vec<AggKind>,
}

impl HashAggregationOperator {
    pub fn new(
        output: Box<dyn Output>,
        input_types: Vec<Option<DataTypeRef>>,
        return_types: Vec<DataTypeRef>,
        key_indices: Vec<usize>,
        val_indices: Vec<Vec<usize>>,
        agg_types: Vec<AggKind>,
    ) -> Self {
        assert_eq!(input_types.len(), return_types.len());
        assert_eq!(input_types.len(), val_indices.len());
        assert_eq!(input_types.len(), agg_types.len());
        Self {
            state_entries: HashMap::new(),
            output,
            input_types,
            return_types,
            key_indices,
            val_indices,
            agg_types,
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

impl_consume_barrier_default!(HashAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for HashAggregationOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
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
            col.array().insert_key(&mut keys);
        }

        let (unique_keys, distinct_rows, mut key_to_vis_maps) =
            self.get_unique_keys(&keys, &visibility, &self.state_entries)?;

        // These builders are for storing the aggregated value for each aggregation function
        let mut agg_array_builders: Vec<ArrayBuilderImpl> = self
            .return_types
            .iter()
            .map(|return_type| return_type.clone().create_array_builder(0))
            .try_collect()?;
        let mut new_ops = Vec::new();

        unique_keys.into_iter().try_for_each(|key| -> Result<()> {
            let cur_vis_map = key_to_vis_maps.remove(key).ok_or_else(|| {
                ErrorCode::InternalError(format!("Visibility does not exist for key {:?}", key))
            })?;

            // check existence to avoid paying the cost of copy in `entry(...).or_insert()` everytime
            if !self.state_entries.contains_key(key) {
                let mut agg_states = Vec::with_capacity(self.input_types.len());
                for ((input_type, agg_kind), return_type) in self
                    .input_types
                    .iter()
                    .zip(self.agg_types.iter())
                    .zip(self.return_types.iter())
                {
                    agg_states.push(create_streaming_agg_state(
                        input_type,
                        agg_kind,
                        return_type,
                    )?);
                }
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
                .val_indices
                .iter()
                .map(|input_indices| {
                    input_indices
                        .iter()
                        .map(|idx| arrays[*idx].array_ref())
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
            .zip(self.return_types.iter())
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
            let array = &arrays[*key_idx];
            new_columns.push(Column::new(
                Arc::new(array.array_ref().get_sub_array(&distinct_rows)),
                array.data_type(),
            ));
        }
        new_columns.extend(agg_columns.into_iter());

        let chunk = StreamChunk {
            ops: new_ops,
            visibility: None,
            columns: new_columns,
        };
        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}
