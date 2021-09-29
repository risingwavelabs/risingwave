//! Local Streaming Hash Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result};
use crate::expr::AggKind;
use crate::impl_consume_barrier_default;
use crate::stream_op::{StreamingFloatSumAgg, StreamingSumAgg};
use crate::types::{DataType, DataTypeKind, DataTypeRef, Datum};

use async_trait::async_trait;
use std::collections::HashMap;

pub type HashKey = Datum;

pub fn create_streaming_local_agg_state(
    input_type: &dyn DataType,
    agg_type: &AggKind,
    return_type: &dyn DataType,
) -> Result<Box<dyn StreamingAggStateImpl>> {
    let state: Box<dyn StreamingAggStateImpl> = match (
        input_type.data_type_kind(),
        agg_type,
        return_type.data_type_kind(),
    ) {
        (DataTypeKind::Int64, AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingFoldAgg::<
            I64Array,
            I64Array,
            Countable<<I64Array as Array>::OwnedItem>,
        >::new()),
        (DataTypeKind::Int32, AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingFoldAgg::<
            I64Array,
            I32Array,
            Countable<<I32Array as Array>::OwnedItem>,
        >::new()),
        (DataTypeKind::Int16, AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingFoldAgg::<
            I64Array,
            I16Array,
            Countable<<I16Array as Array>::OwnedItem>,
        >::new()),
        (DataTypeKind::Float32, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(StreamingFoldAgg::<
                I64Array,
                F32Array,
                Countable<<F32Array as Array>::OwnedItem>,
            >::new())
        }
        (DataTypeKind::Float64, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(StreamingFoldAgg::<
                I64Array,
                F64Array,
                Countable<<F64Array as Array>::OwnedItem>,
            >::new())
        }
        (DataTypeKind::Char, AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingFoldAgg::<
            I64Array,
            UTF8Array,
            Countable<<UTF8Array as Array>::OwnedItem>,
        >::new()),
        (DataTypeKind::Boolean, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(StreamingFoldAgg::<
                I64Array,
                BoolArray,
                Countable<<BoolArray as Array>::OwnedItem>,
            >::new())
        }
        (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int16) => {
            Box::new(StreamingSumAgg::<I16Array>::new())
        }
        (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int32) => {
            Box::new(StreamingSumAgg::<I32Array>::new())
        }
        (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32) => {
            Box::new(StreamingFloatSumAgg::<F32Array>::new())
        }
        (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64) => {
            Box::new(StreamingFloatSumAgg::<F64Array>::new())
        }
        _ => unimplemented!(),
    };
    Ok(state)
}

pub trait HashValue: Send + Sync + 'static {
    /// check whether the key to this value is seen first time or not
    fn is_first_data(&self) -> bool;
    fn unset_first_data(&mut self);
    /// get the output after last apply but before current apply
    fn get_last_output(
        &self,
        agg_array_builder: &mut ArrayBuilderImpl,
        row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()>;
    /// apply the data to aggregation
    fn apply_batch(
        &mut self,
        ops: &[Op],
        visibility: Option<&Bitmap>,
        aggregate_array: &ArrayImpl,
        row_count_array: &ArrayImpl,
    ) -> Result<()>;
    fn get_after_apply_output(
        &self,
        agg_array_builder: &mut ArrayBuilderImpl,
        row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()>;
    fn agg_state_new_builder(&self) -> ArrayBuilderImpl;
    fn row_count_state_new_builder(&self) -> ArrayBuilderImpl;
}

pub struct HashLocalValue {
    /// see HashGlobalValue for explanation
    first_data: bool,
    agg_state: Box<dyn StreamingAggStateImpl>,
    row_count_state: StreamingRowCountAgg,
}

impl HashLocalValue {
    pub fn new(
        first_data: bool,
        agg_state: Box<dyn StreamingAggStateImpl>,
        row_count_state: StreamingRowCountAgg,
    ) -> Self {
        HashLocalValue {
            first_data,
            agg_state,
            row_count_state,
        }
    }
}

impl HashValue for HashLocalValue {
    fn is_first_data(&self) -> bool {
        self.first_data
    }

    fn unset_first_data(&mut self) {
        self.first_data = false;
    }

    fn get_last_output(
        &self,
        agg_array_builder: &mut ArrayBuilderImpl,
        row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()> {
        self.agg_state.get_output(agg_array_builder)?;
        self.row_count_state.get_output(row_count_array_builder)
    }

    fn apply_batch(
        &mut self,
        ops: &[Op],
        visibility: Option<&Bitmap>,
        aggregate_array: &ArrayImpl,
        row_count_array: &ArrayImpl,
    ) -> Result<()> {
        self.agg_state
            .apply_batch(ops, visibility, aggregate_array)?;
        self.row_count_state
            .apply_batch(ops, visibility, row_count_array)
    }

    fn get_after_apply_output(
        &self,
        agg_array_builder: &mut ArrayBuilderImpl,
        row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()> {
        // output the current state into builder
        self.agg_state.get_output(agg_array_builder)?;
        // output the current row count into builder
        self.row_count_state.get_output(row_count_array_builder)
    }

    fn agg_state_new_builder(&self) -> ArrayBuilderImpl {
        self.agg_state.new_builder()
    }

    fn row_count_state_new_builder(&self) -> ArrayBuilderImpl {
        self.row_count_state.new_builder()
    }
}

/// `HashLocalAggregationOperator` supports local hash aggregation.
pub struct HashLocalAggregationOperator {
    /// Aggregation state of the current operator
    state_entries: HashMap<Vec<HashKey>, Box<dyn HashValue>>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Input Type of the current aggregator
    input_type: DataTypeRef,
    /// Return type of current aggregator
    return_type: DataTypeRef,
    /// Indices of the columns
    key_indices: Vec<usize>,
    /// Index of the column being aggregated.
    val_index: Option<usize>,
    /// Aggregation Kind for constructing StreamingAggStateImpl
    /// TODO: Use Streaming specific AggKind instead of borrowing from OLAP
    agg_type: AggKind,
}

impl HashLocalAggregationOperator {
    pub fn new(
        input_type: DataTypeRef,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
        keys: Vec<usize>,
        val: Option<usize>,
        agg_type: AggKind,
    ) -> Self {
        Self {
            state_entries: HashMap::new(),
            output,
            input_type,
            return_type,
            key_indices: keys,
            val_index: val,
            agg_type,
        }
    }
}

#[allow(clippy::complexity)]
pub fn get_unique_keys<'a, 'b>(
    keys: &'a [Vec<HashKey>],
    visibility: &Option<Bitmap>,
    state_entries: &HashMap<Vec<HashKey>, Box<dyn HashValue>>,
) -> Result<(
    Vec<&'b Vec<HashKey>>,
    Vec<usize>,
    HashMap<&'b Vec<HashKey>, Bitmap>,
)>
where
    'a: 'b,
{
    let total_num_rows = keys.len();
    // Each vis map will be passed into StreamingAggStateImpl
    let mut key_to_vis_maps = HashMap::new();
    // Some grouped keys are the same and their corresponding rows will be collapsed together after aggregation.
    // This vec records the row indices so that we can pick these rows from all the array into output chunk.
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
        .collect::<Result<HashMap<&Vec<HashKey>, Bitmap>>>();

    Ok((unique_keys, distinct_rows, key_to_vis_maps?))
}

impl_consume_barrier_default!(HashLocalAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for HashLocalAggregationOperator {
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
            get_unique_keys(&keys, &visibility, &self.state_entries)?;

        // This builder is for the array to store the aggregated value for each grouped key.
        let mut agg_array_builder = self.return_type.clone().create_array_builder(0)?;
        let mut row_count_array_builder = StreamingRowCountAgg::create_array_builder(0)?;
        let mut new_ops = Vec::new();

        unique_keys.into_iter().try_for_each(|key| -> Result<()> {
            let cur_vis_map = key_to_vis_maps.remove(key).ok_or_else(|| {
                ErrorCode::InternalError(format!("Visibility does not exist for key {:?}", key))
            })?;

            // check existence to avoid paying the cost of `to_vec` everytime
            if !self.state_entries.contains_key(key) {
                self.state_entries.insert(
                    key.to_vec(),
                    Box::new(HashLocalValue::new(
                        true,
                        create_streaming_local_agg_state(
                            &*self.input_type,
                            &self.agg_type,
                            &*self.return_type,
                        )?,
                        StreamingRowCountAgg::new(),
                    )) as Box<dyn HashValue>,
                );
            }

            // since we checked existence, the key must exist so we `unwrap` directly
            let value = self.state_entries.get_mut(key).unwrap();
            let mut agg_state_builder = value.agg_state_new_builder();
            let mut row_count_builder = value.row_count_state_new_builder();
            if !value.is_first_data() {
                // record the last state into builder
                value.get_after_apply_output(&mut agg_state_builder, &mut row_count_builder)?;
            }

            // do we have a particular column to aggregate? Not for Count
            // min, max, sum needs this
            let aggregate_array = match self.val_index {
                Some(agg_idx) => arrays[agg_idx].array_ref(),
                None => arrays[0].array_ref(),
            };

            value.apply_batch(&ops, Some(&cur_vis_map), aggregate_array, aggregate_array)?;

            value.get_after_apply_output(&mut agg_state_builder, &mut row_count_builder)?;

            agg_array_builder.append_array(&agg_state_builder.finish()?)?;
            row_count_array_builder.append_array(&row_count_builder.finish()?)?;

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

        // compose the aggregation column
        let agg_column = Column::new(
            Arc::new(agg_array_builder.finish()?),
            self.return_type.clone(),
        );
        // compose the row count column
        let row_count_column = Column::new(
            Arc::new(row_count_array_builder.finish()?),
            StreamingRowCountAgg::return_type(),
        );

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
        new_columns.push(agg_column);
        new_columns.push(row_count_column);

        let chunk = StreamChunk {
            ops: new_ops,
            visibility: None,
            columns: new_columns,
        };
        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}
