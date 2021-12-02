use itertools::Itertools;

use risingwave_common::array::data_chunk_iter::RowDeserializer;
use risingwave_common::array::{ArrayImpl, Op, Row, RwError};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataTypeKind;

use super::{AggCall, CellBasedSchemaedSerializable, SchemaedSerializable, StreamingAggStateImpl};
use crate::stream_op::create_streaming_agg_state;

/// A key stored in `KeyedState`.
pub type HashKey = Row;

/// A value stored in `KeyedState`,
#[derive(Clone, Debug)]
pub struct HashValue {
    /// one or more aggregation states, all corresponding to the same key
    pub agg_states: Vec<Box<dyn StreamingAggStateImpl>>,
}

impl std::ops::Index<usize> for HashValue {
    type Output = <Vec<Box<dyn StreamingAggStateImpl>> as std::ops::Index<usize>>::Output;
    fn index(&self, index: usize) -> &Self::Output {
        &self.agg_states[index]
    }
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

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let row = self
            .agg_states
            .iter()
            .map(|state| state.get_output().unwrap())
            .collect_vec();
        Row(row)
            .serialize()
            .map_err(|e| ErrorCode::MemComparableError(e).into())
    }

    pub fn serialize_cell(&self, cell_idx: usize) -> Result<Vec<u8>> {
        let datum = self.agg_states[cell_idx].get_output().unwrap();
        Row(vec![datum])
            .serialize()
            .map_err(|e| ErrorCode::MemComparableError(e).into())
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

impl CellBasedSchemaedSerializable for RowSerializer {
    type Output = Row;

    fn cells(&self) -> usize {
        self.schema.fields().len()
    }

    fn cell_based_schemaed_serialize(&self, data: &Self::Output, cell_idx: usize) -> Vec<u8> {
        data.serialize_datum(cell_idx).unwrap()
    }

    fn cell_based_schemaed_deserialize(
        &self,
        data: &[u8],
        cell_idx: usize,
    ) -> <Self::Output as std::ops::Index<usize>>::Output {
        self.deserializer.deserialize_datum(data, cell_idx).unwrap()
    }

    fn schemaed_serialize(&self, data: &Self::Output) -> Vec<u8> {
        data.serialize().unwrap()
    }

    fn schemaed_deserialize(&self, data: &[u8]) -> Self::Output {
        self.deserializer.deserialize(data).unwrap()
    }
}

struct AggStateDeserializer {
    /// The internal representation of data is Row, so we need a Row deserializer.
    inner: RowDeserializer,
    /// The aggregation call description of the current schema.
    agg_calls: Vec<AggCall>,
}

impl AggStateDeserializer {
    pub fn new(schema: Vec<DataTypeKind>, agg_calls: Vec<AggCall>) -> Self {
        Self {
            inner: RowDeserializer::new(schema),
            agg_calls,
        }
    }

    pub fn deserialize(&self, data: &[u8]) -> Result<HashValue> {
        let row = self
            .inner
            .deserialize(data)
            .map_err(|e| RwError::from(ErrorCode::MemComparableError(e)))?;
        Ok(HashValue::new(
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
        ))
    }

    pub fn deserialize_cell(
        &self,
        data: &[u8],
        cell_idx: usize,
    ) -> Result<Box<dyn StreamingAggStateImpl>> {
        let datum = self
            .inner
            .deserialize_datum(data, cell_idx)
            .map_err(|e| RwError::from(ErrorCode::MemComparableError(e)))?;
        let agg_call = &self.agg_calls[cell_idx];
        Ok(create_streaming_agg_state(
            agg_call.args.arg_types(),
            &agg_call.kind,
            &agg_call.return_type,
            Some(datum),
        )
        .unwrap())
    }
}

/// Serialize and deserialize a aggregation state into bytes.
pub struct AggStateSerializer {
    /// The internal representation of data is Row, so we need a Row deserializer.
    deserializer: AggStateDeserializer,
}

impl AggStateSerializer {
    pub fn new(agg_calls: Vec<AggCall>) -> Self {
        Self {
            deserializer: AggStateDeserializer::new(
                agg_calls
                    .iter()
                    .map(|x| x.return_type.data_type_kind())
                    .collect_vec(),
                agg_calls,
            ),
        }
    }
}

impl SchemaedSerializable for AggStateSerializer {
    type Output = HashValue;

    fn schemaed_serialize(&self, data: &HashValue) -> Vec<u8> {
        data.serialize().unwrap()
    }

    fn schemaed_deserialize(&self, data: &[u8]) -> HashValue {
        self.deserializer.deserialize(data).unwrap()
    }
}

impl CellBasedSchemaedSerializable for AggStateSerializer {
    type Output = HashValue;

    fn cells(&self) -> usize {
        self.deserializer.agg_calls.len()
    }

    fn cell_based_schemaed_serialize(&self, data: &Self::Output, cell_idx: usize) -> Vec<u8> {
        data.serialize_cell(cell_idx).unwrap()
    }

    fn cell_based_schemaed_deserialize(
        &self,
        data: &[u8],
        cell_idx: usize,
    ) -> <Self::Output as std::ops::Index<usize>>::Output {
        self.deserializer.deserialize_cell(data, cell_idx).unwrap()
    }

    fn schemaed_serialize(&self, data: &Self::Output) -> Vec<u8> {
        data.serialize().unwrap()
    }

    fn schemaed_deserialize(&self, data: &[u8]) -> Self::Output {
        self.deserializer.deserialize(data).unwrap()
    }
}
