use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{deserialize_datum_from, serialize_datum_into, Datum};

use crate::stream_op::{create_streaming_agg_state, AggCall};

use super::super::aggregation::StreamingAggStateImpl;
use super::Keyspace;

/// A wrapper around [`StreamingAggStateImpl`], which fetches data from the state store and helps
/// update the state. We don't use any trait to wrap around all `ManagedXxxState`, so as to reduce
/// the overhead of creating boxed async future.
pub struct ManagedValueState {
    /// The internal single-value state.
    state: Box<dyn StreamingAggStateImpl>,

    /// The keyspace to operate on.
    keyspace: Keyspace,

    /// Indicates whether this managed state is dirty. If this state is dirty, we cannot evict the
    /// state from memory.
    is_dirty: bool,
}

impl ManagedValueState {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub async fn new(agg_call: AggCall, keyspace: Keyspace) -> Result<Self> {
        // View the keyspace as a single-value space, and get the value.
        let raw_data = keyspace.get().await?;

        // Decode the Datum from the value.
        let data = if let Some(raw_data) = raw_data {
            let mut deserializer = memcomparable::Deserializer::from_slice(&raw_data[..]);
            Some(
                deserialize_datum_from(&agg_call.return_type.data_type_kind(), &mut deserializer)
                    .map_err(ErrorCode::MemComparableError)?,
            )
        } else {
            None
        };

        // Create the internal state based on the value we get.
        Ok(Self {
            state: create_streaming_agg_state(
                agg_call.args.arg_types(),
                &agg_call.kind,
                &agg_call.return_type,
                data,
            )?,
            is_dirty: false,
            keyspace,
        })
    }

    /// Apply a batch of data to the state.
    pub async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        self.is_dirty = true;
        self.state.apply_batch(ops, visibility, data)
    }

    /// Get the output of the state. Note that in our case, getting the output is very easy, as the
    /// output is the same as the aggregation state. In other aggregators, like min and max,
    /// `get_output` might involve a scan from the state store.
    async fn get_output(&self) -> Result<Datum> {
        self.state.get_output()
    }

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Flush the state to a write batch. TODO: add `WriteBatch` to Hummock.
    fn flush(&mut self, write_batch: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<()> {
        // If the managed state is not dirty, the caller should not flush. But forcing a flush won't
        // cause incorrect result: it will only produce more I/O.
        debug_assert!(self.is_dirty);

        let v = self.state.get_output()?;
        let mut serializer = memcomparable::Serializer::default();
        serialize_datum_into(&v, &mut serializer).map_err(ErrorCode::MemComparableError)?;
        write_batch.push((
            self.keyspace.prefix().to_vec(),
            Some(serializer.into_inner()),
        ));
        self.is_dirty = false;
        Ok(())
    }
}
