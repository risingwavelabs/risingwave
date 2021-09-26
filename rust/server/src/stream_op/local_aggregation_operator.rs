//! Streaming Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::expr::AggKind;
use crate::impl_consume_barrier_default;
use crate::types::DataTypeKind;
use crate::types::Int64Type;
use crate::types::{DataTypeRef, ScalarImpl};
use std::collections::HashMap;

use async_trait::async_trait;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R> = StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingFloatSumAgg` sums data of the same float type.
pub type StreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

/// `LocalAggregationOperator` is the aggregation operator for streaming system.
/// To create an aggregation operator, a state should be passed along the
/// constructor.
///
/// `LocalAggregationOperator` counts rows and outputs two columns, one is concrete
/// data stored by state, and one is the count. These data will be further sent
/// to `GlobalAggregationOperator` and become the real aggregated data.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `LocalAggregationOperator` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `LocalAggregationOperator`.
pub struct LocalAggregationOperator {
    /// Aggregation state of the current operator
    state: Box<dyn StreamingAggStateImpl>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Whether this is the first time of consuming data
    first_data: bool,
    /// Number of rows processed by this operator
    row_state: StreamingRowCountAgg,
    /// Return type of current aggregator
    return_type: DataTypeRef,
    /// Column to process
    col_idx: usize,
}

impl LocalAggregationOperator {
    pub fn new(
        state: Box<dyn StreamingAggStateImpl>,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
        col_idx: usize,
    ) -> Self {
        Self {
            state,
            output,
            first_data: true,
            row_state: StreamingRowCountAgg::new(),
            return_type,
            col_idx,
        }
    }
}

impl_consume_barrier_default!(LocalAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for LocalAggregationOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
            cardinality: _,
        } = chunk;

        let mut builder = self.state.new_builder();
        let mut row_count_builder = self.row_state.new_builder();

        if !self.first_data {
            // record the last state into builder
            self.state.get_output(&mut builder)?;
            self.row_state.get_output(&mut row_count_builder)?;
        }

        self.state
            .apply_batch(&ops, visibility.as_ref(), arrays[self.col_idx].array_ref())?;

        self.row_state
            .apply_batch(&ops, visibility.as_ref(), arrays[self.col_idx].array_ref())?;

        // output the current state into builder
        self.state.get_output(&mut builder)?;
        self.row_state.get_output(&mut row_count_builder)?;

        let chunk;

        let array = Arc::new(builder.finish()?);
        let column = Column::new(array, self.return_type.clone());

        let array_rowcnt = Arc::new(row_count_builder.finish()?);
        let column_rowcnt = Column::new(array_rowcnt, Arc::new(Int64Type::new(true)));

        let columns = vec![column, column_rowcnt];

        // There should be only one column in output. Meanwhile, for the first update,
        // cardinality is 1. For the rest, cardinalty is 2, which includes a deletion and
        // a update.
        if self.first_data {
            chunk = StreamChunk {
                ops: vec![Op::Insert],
                visibility: None,
                cardinality: 1,
                columns,
            };
        } else {
            chunk = StreamChunk {
                ops: vec![Op::UpdateDelete, Op::UpdateInsert],
                visibility: None,
                cardinality: 2,
                columns,
            };
        }

        self.first_data = false;

        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}

// mimic the aggregator in `server/src/vector_op/agg.rs`
pub fn create_agg_state(
    input_type: DataTypeRef,
    agg_type: &AggKind,
    return_type: DataTypeRef,
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

type Key = Option<ScalarImpl>;

/// `HashLocalAggregationOperator` supports local hash aggregation.
pub struct HashLocalAggregationOperator {
    /// Aggregation state of the current operator
    /// first_data: bool, indicating whether this is a key saw by first time
    state_entries: HashMap<Vec<Key>, (bool, Box<dyn StreamingAggStateImpl>)>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Input Type of the current aggregator
    input_type: DataTypeRef,
    /// Return type of current aggregator
    return_type: DataTypeRef,
    /// Indices of the columns
    keys: Vec<usize>,
    /// Index of the column being aggregated.
    val: Option<usize>,
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
            keys,
            val,
            agg_type,
        }
    }
}

impl_consume_barrier_default!(HashLocalAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for HashLocalAggregationOperator {
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
        for key_idx in &self.keys {
            let col = &arrays[*key_idx];
            col.array().insert_key(&mut keys);
        }

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
            if let Some(vis_map) = &visibility {
                if !vis_map.is_set(row_idx).unwrap() {
                    continue;
                }
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                // Check whether the `key` shows up first time in the whole history, if not,
                // then we need a row for `UpdateDelete` and another row for `UpdateInsert`.
                // Otherwise, we will have only one row for `Insert`.
                if self.state_entries.contains_key(key) {
                    distinct_rows.push(row_idx);
                }
                distinct_rows.push(row_idx);
                unique_keys.push(key);
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }
        let output_cardinality = distinct_rows.len();
        // turn the Vec of bool into Bitmap
        let mut key_to_vis_maps = key_to_vis_maps
            .into_iter()
            .map(|(key, vis_map)| (key, Bitmap::from_vec(vis_map).unwrap()))
            .collect::<HashMap<_, _>>();

        // This builder is for the array to store the aggregated value for each grouped key.
        let mut agg_array_builder = self.return_type.clone().create_array_builder(0).unwrap();
        let mut new_ops = Vec::new();

        unique_keys.into_iter().for_each(|key| {
            let cur_vis_map = key_to_vis_maps.remove(key).unwrap();

            // check existence to avoid paying the cost of `to_vec` everytime
            if !self.state_entries.contains_key(key) {
                self.state_entries.insert(
                    key.to_vec(),
                    (
                        true,
                        create_agg_state(
                            self.input_type.clone(),
                            &self.agg_type,
                            self.return_type.clone(),
                        )
                        .unwrap(),
                    ),
                );
            }

            // since we checked existence, the key must exist.
            let (first_data, state) = self.state_entries.get_mut(key).unwrap();
            let mut builder = state.new_builder();
            if !*first_data {
                // record the last state into builder
                state.get_output(&mut builder).unwrap();
            }

            // do we have a particular column to aggregate? Not for Count
            // min, max, sum needs this
            let aggregate_array = match self.val {
                Some(agg_idx) => arrays[agg_idx].array_ref(),
                None => arrays[0].array_ref(),
            };

            // Question: shouldn't the visibility map be replaced with some simple indices dictating
            // which rows belong to this key
            state
                .apply_batch(&ops, Some(&cur_vis_map), aggregate_array)
                .unwrap();
            // output the current state into builder
            state.get_output(&mut builder).unwrap();

            agg_array_builder
                .append_array(&builder.finish().unwrap())
                .unwrap();

            // same logic from `simple` LocalAggregationOperator
            if !*first_data {
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);
            } else {
                new_ops.push(Op::Insert);
            }

            *first_data = false;
        });

        // compose the aggregation column
        let agg_column = Column::new(
            Arc::new(agg_array_builder.finish().unwrap()),
            self.return_type.clone(),
        );
        // compose all the columns together
        // rows with the same key collapse into one or two rows
        let mut new_columns = Vec::new();
        for array in arrays {
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
