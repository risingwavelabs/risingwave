use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{AggCall, SchemaedSerializable, StreamingAggStateImpl};
use crate::stream_op::Op;

/// A key stored in `KeyedState`.
pub type HashKey = Row;

/// A value stored in `KeyedState`,
#[derive(Clone, Debug)]
pub struct HashValue {
    /// one or more aggregation states, all corresponding to the same key
    pub agg_states: Vec<Box<dyn StreamingAggStateImpl>>,
}

impl HashValue {
    pub fn new(agg_states: Vec<Box<dyn StreamingAggStateImpl>>) -> Self {
        HashValue { agg_states }
    }

    pub fn apply_batch(
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
}

/// Serialize and deserialize a row into memcomparable bytes.
#[derive(Clone)]
pub struct RowSerializer {
    /// Schema of the row. This field won't be used for now, as our encoding already embeds the
    /// type info inside the bytes. In the future, we should decode and encode data with
    /// schema, and remove type info from the encoded data.
    _schema: Schema,
}

impl RowSerializer {
    pub fn new(schema: Schema) -> Self {
        Self { _schema: schema }
    }
}

impl SchemaedSerializable for RowSerializer {
    type Output = Row;

    fn schemaed_serialize(_data: &Row) -> Vec<u8> {
        todo!()
    }

    fn schemaed_deserialize(_data: &[u8]) -> Row {
        todo!()
    }
}

/// Serialize and deserialize a aggregation state into bytes.
#[derive(Clone)]
pub struct AggStateSerializer {
    // The aggregation call description of the current schema.
    agg_calls: Vec<AggCall>,
}

impl AggStateSerializer {
    pub fn new(agg_calls: Vec<AggCall>) -> Self {
        Self { agg_calls }
    }
}

impl SchemaedSerializable for AggStateSerializer {
    type Output = HashValue;

    fn schemaed_serialize(_data: &HashValue) -> Vec<u8> {
        todo!()
    }

    fn schemaed_deserialize(_data: &[u8]) -> HashValue {
        todo!()
    }
}
