use bytes::Bytes;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::{deserialize_datum_from, serialize_datum_into, Datum};

use crate::stream_op::{create_streaming_agg_state, AggCall};

use super::super::aggregation::StreamingAggStateImpl;
use super::super::keyspace::{Keyspace, StateStore};

/// A wrapper around [`StreamingAggStateImpl`], which fetches data from the state store and helps
/// update the state. We don't use any trait to wrap around all `ManagedXxxState`, so as to reduce
/// the overhead of creating boxed async future.
pub struct ManagedValueState<S: StateStore> {
    /// The internal single-value state.
    state: Box<dyn StreamingAggStateImpl>,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,

    /// Indicates whether this managed state is dirty. If this state is dirty, we cannot evict the
    /// state from memory.
    is_dirty: bool,
}

impl<S: StateStore> ManagedValueState<S> {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub async fn new(
        agg_call: AggCall,
        keyspace: Keyspace<S>,
        row_count: Option<usize>,
    ) -> Result<Self> {
        let data = if row_count != Some(0) {
            // View the keyspace as a single-value space, and get the value.
            let raw_data = keyspace.get().await?;

            // Decode the Datum from the value.
            if let Some(raw_data) = raw_data {
                let mut deserializer = memcomparable::Deserializer::new(&raw_data[..]);
                Some(deserialize_datum_from(
                    &agg_call.return_type.data_type_kind(),
                    &mut deserializer,
                )?)
            } else {
                None
            }
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
        debug_assert!(super::verify_batch(ops, visibility, data));
        self.is_dirty = true;
        self.state.apply_batch(ops, visibility, data)
    }

    /// Get the output of the state. Note that in our case, getting the output is very easy, as the
    /// output is the same as the aggregation state. In other aggregators, like min and max,
    /// `get_output` might involve a scan from the state store.
    pub async fn get_output(&mut self) -> Result<Datum> {
        debug_assert!(!self.is_dirty());
        self.state.get_output()
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    pub fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // If the managed state is not dirty, the caller should not flush. But forcing a flush won't
        // cause incorrect result: it will only produce more I/O.
        debug_assert!(self.is_dirty());

        let v = self.state.get_output()?;
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serialize_datum_into(&v, &mut serializer)?;
        write_batch.push((
            self.keyspace.prefix().to_vec().into(),
            Some(serializer.into_inner().into()),
        ));
        self.is_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{Int64Type, ScalarImpl};

    use super::*;
    use crate::stream_op::keyspace::MemoryStateStore;
    use crate::stream_op::AggArgs;
    use risingwave_common::array::{I64Array, Op};

    fn create_test_count_state() -> AggCall {
        AggCall {
            kind: risingwave_common::expr::AggKind::Count,
            args: AggArgs::Unary(Int64Type::create(true), 0),
            return_type: Int64Type::create(true),
        }
    }

    #[tokio::test]
    async fn test_managed_value_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedValueState::new(create_test_count_state(), keyspace, Some(0))
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        // apply a batch and get the output
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(1), Some(2), None])
                    .unwrap()
                    .into()],
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // get output
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );

        // reload the state and check the output
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state = ManagedValueState::new(create_test_count_state(), keyspace, None)
            .await
            .unwrap();
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );
    }
}
