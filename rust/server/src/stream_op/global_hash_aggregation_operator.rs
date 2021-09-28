//! Global Streaming Hash Aggregators

use super::{aggregation::*, StreamingFloatSumAgg, StreamingSumAgg};
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result};
use crate::expr::AggKind;
use crate::impl_consume_barrier_default;
use crate::stream_op::local_hash_aggregation_operator::{get_unique_keys, HashKey, HashValue};
use crate::types::{DataType, DataTypeKind, DataTypeRef};

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub fn create_streaming_global_agg_state(
    input_type: &dyn DataType,
    agg_type: &AggKind,
    return_type: &dyn DataType,
) -> Result<Box<dyn StreamingAggStateImpl>> {
    let state: Box<dyn StreamingAggStateImpl> = match (
        input_type.data_type_kind(),
        agg_type,
        return_type.data_type_kind(),
    ) {
        (_, AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingSumAgg::<I64Array>::new()),
        (_, AggKind::Count, DataTypeKind::Int32) => Box::new(StreamingSumAgg::<I32Array>::new()),
        (_, AggKind::Count, DataTypeKind::Int16) => Box::new(StreamingSumAgg::<I16Array>::new()),
        (_, AggKind::Sum, DataTypeKind::Int64) => Box::new(StreamingSumAgg::<I64Array>::new()),
        (_, AggKind::Sum, DataTypeKind::Int32) => Box::new(StreamingSumAgg::<I32Array>::new()),
        (_, AggKind::Sum, DataTypeKind::Int16) => Box::new(StreamingSumAgg::<I16Array>::new()),
        (_, AggKind::Sum, DataTypeKind::Float64) => {
            Box::new(StreamingFloatSumAgg::<F64Array>::new())
        }
        (_, AggKind::Sum, DataTypeKind::Float32) => {
            Box::new(StreamingFloatSumAgg::<F32Array>::new())
        }
        _ => unimplemented!(),
    };
    Ok(state)
}

pub struct HashGlobalValue {
    /// indicating whether this is a key saw by first time
    first_data: bool,
    /// the aggregation state
    agg_state: Box<dyn StreamingAggStateImpl>,
    /// number of rows processed by this operator.
    /// The difference between two row count aggregators in local and global
    /// is that the local one never return `None`.
    row_count_state: StreamingSumAgg<I64Array>,
}

impl HashGlobalValue {
    pub fn new(
        first_data: bool,
        agg_state: Box<dyn StreamingAggStateImpl>,
        row_count_state: StreamingSumAgg<I64Array>,
    ) -> Self {
        HashGlobalValue {
            first_data,
            agg_state,
            row_count_state,
        }
    }
}

impl HashValue for HashGlobalValue {
    fn is_first_data(&self) -> bool {
        self.first_data
    }

    fn unset_first_data(&mut self) {
        self.first_data = false;
    }

    fn get_last_output(
        &self,
        agg_array_builder: &mut ArrayBuilderImpl,
        _row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()> {
        self.agg_state.get_output(agg_array_builder)
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
        _row_count_array_builder: &mut ArrayBuilderImpl,
    ) -> Result<()> {
        let rows = self.row_count_state.get_state();
        match rows {
            None | Some(0) => {
                // If there are no rows, output a `None`.
                agg_array_builder.append_null()
            }
            Some(_) => {
                // Otherwise, output the concrete datum
                self.agg_state.get_output(agg_array_builder)
            }
        }
    }

    fn agg_state_new_builder(&self) -> ArrayBuilderImpl {
        self.agg_state.new_builder()
    }

    fn row_count_state_new_builder(&self) -> ArrayBuilderImpl {
        self.row_count_state.new_builder()
    }
}

pub struct HashGlobalAggregationOperator {
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
    /// This is a must for global aggregator.
    /// TODO: This, together with the one in local aggregator,
    /// should be changed if we support multiple aggregations
    /// in one operator.
    val_index: usize,
    /// Aggregation Kind for constructing StreamingAggStateImpl
    /// TODO: Use Streaming specific AggKind instead of borrowing from OLAP
    agg_type: AggKind,
}

impl HashGlobalAggregationOperator {
    pub fn new(
        input_type: DataTypeRef,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
        keys: Vec<usize>,
        val: usize,
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

impl_consume_barrier_default!(HashGlobalAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for HashGlobalAggregationOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
            cardinality: _,
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
        let mut new_ops = Vec::new();
        let output_cardinality = distinct_rows.len();

        unique_keys.into_iter().try_for_each(|key| -> Result<()> {
            let cur_vis_map = key_to_vis_maps.remove(key).ok_or_else(|| {
                ErrorCode::InternalError(format!("Visibility does not exist for key {:?}", key))
            })?;

            // check existence to avoid paying the cost of `to_vec` everytime
            if !self.state_entries.contains_key(key) {
                self.state_entries.insert(
                    key.to_vec(),
                    Box::new(HashGlobalValue::new(
                        true,
                        create_streaming_global_agg_state(
                            &*self.input_type,
                            &self.agg_type,
                            &*self.return_type,
                        )?,
                        StreamingSumAgg::<I64Array>::new(),
                    )) as Box<dyn HashValue>,
                );
            }

            // since we just checked existence, the key must exist so we `unwrap` directly
            let value = self.state_entries.get_mut(key).unwrap();
            let mut agg_state_builder = value.agg_state_new_builder();
            let mut row_count_state_builder = value.row_count_state_new_builder();
            if !value.is_first_data() {
                // record the last state into builder
                value.get_last_output(&mut agg_state_builder, &mut row_count_state_builder)?;
            }

            // do we have a particular column to aggregate? Not for Count
            // min, max, sum needs this
            let aggregate_array = arrays[self.val_index].array_ref();

            // TODO: here we assume that the row count is at the last column
            let row_count_array = arrays
                .last()
                .ok_or_else(|| {
                    ErrorCode::InternalError("The last column does not exists".to_string())
                })?
                .array_ref();

            value.apply_batch(&ops, Some(&cur_vis_map), aggregate_array, row_count_array)?;

            // output the current state into builder
            value.get_after_apply_output(&mut agg_state_builder, &mut row_count_state_builder)?;

            agg_array_builder.append_array(&agg_state_builder.finish()?)?;

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

        let chunk = StreamChunk {
            ops: new_ops,
            visibility: None,
            cardinality: output_cardinality,
            columns: new_columns,
        };
        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}
