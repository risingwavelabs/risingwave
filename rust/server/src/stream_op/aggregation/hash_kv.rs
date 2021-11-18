use itertools::Itertools;

use risingwave_common::array::data_chunk_iter::RowDeserializer;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{get_one_output_from_state_impl, AggCall, SchemaedSerializable, StreamingAggStateImpl};
use crate::stream_op::{create_streaming_agg_state, Op};

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

/// Serialize and deserialize a row into memcomparable bytes, used in state backends.
pub struct RowSerializer {
    /// Schema of the row. This field won't be used for now, as our encoding already embeds the
    /// type info inside the bytes. In the future, we should decode and encode data with
    /// schema, and remove type info from the encoded data.
    schema: Schema,

    /// Provide the schema to RowDeserializer to deserialize a row.
    deserializer: RowDeserializer,
}

impl RowSerializer {
    pub fn new(schema: Schema) -> Self {
        Self {
            deserializer: RowDeserializer::new(
                schema
                    .fields()
                    .iter()
                    .map(|x| x.data_type_ref().data_type_kind())
                    .collect_vec(),
            ),
            schema,
        }
    }
}

impl SchemaedSerializable for RowSerializer {
    type Output = Row;

    fn schemaed_serialize(&self, data: &Row) -> Vec<u8> {
        data.serialize().unwrap()
    }

    fn schemaed_deserialize(&self, data: &[u8]) -> Row {
        self.deserializer.deserialize(data).unwrap()
    }
}

/// Serialize and deserialize a aggregation state into bytes.
pub struct AggStateSerializer {
    /// The aggregation call description of the current schema.
    agg_calls: Vec<AggCall>,

    /// The internal representation of data is Row, so we need a Row deserializer.
    deserializer: RowDeserializer,
}

impl AggStateSerializer {
    pub fn new(agg_calls: Vec<AggCall>) -> Self {
        Self {
            deserializer: RowDeserializer::new(
                agg_calls
                    .iter()
                    .map(|x| x.return_type.data_type_kind())
                    .collect_vec(),
            ),
            agg_calls,
        }
    }
}

impl SchemaedSerializable for AggStateSerializer {
    type Output = HashValue;

    fn schemaed_serialize(&self, data: &HashValue) -> Vec<u8> {
        let row = data
            .agg_states
            .iter()
            .map(|state| get_one_output_from_state_impl(&**state).unwrap().to_datum())
            .collect_vec();
        Row(row).serialize().unwrap()
    }

    fn schemaed_deserialize(&self, data: &[u8]) -> HashValue {
        let row = self.deserializer.deserialize(data).unwrap();
        HashValue::new(
            row.0
                .into_iter()
                .zip(self.agg_calls.iter())
                .map(|(datum, agg_call)| {
                    create_streaming_agg_state(
                        agg_call.args.arg_types(),
                        &agg_call.kind,
                        &agg_call.return_type,
                        Some(datum),
                    )
                    .unwrap()
                })
                .collect_vec(),
        )
    }
}
