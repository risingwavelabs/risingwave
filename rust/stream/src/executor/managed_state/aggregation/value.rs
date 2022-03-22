// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::value_encoding::{deserialize_cell, serialize_cell};
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{create_streaming_agg_state, AggCall, StreamingAggStateImpl};

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
            // TODO: use the correct epoch
            let epoch = u64::MAX;
            // View the keyspace as a single-value space, and get the value.
            let raw_data = keyspace.value(epoch).await?;

            // Decode the Datum from the value.
            if let Some(raw_data) = raw_data {
                let mut deserializer = value_encoding::Deserializer::new(raw_data.to_bytes());
                Some(deserialize_cell(&mut deserializer, &agg_call.return_type)?)
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

    /// Flush the internal state to a write batch.
    pub fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
        // If the managed state is not dirty, the caller should not flush. But forcing a flush won't
        // cause incorrect result: it will only produce more I/O.
        debug_assert!(self.is_dirty());

        let mut local = write_batch.prefixify(&self.keyspace);
        let v = self.state.get_output()?;
        local.put_single(serialize_cell(&v)?);
        self.is_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;
    use crate::executor::test_utils::create_in_memory_keyspace;
    use crate::executor::AggArgs;

    fn create_test_count_state() -> AggCall {
        AggCall {
            kind: risingwave_common::expr::AggKind::Count,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state() {
        let keyspace = create_in_memory_keyspace();
        let mut managed_state =
            ManagedValueState::new(create_test_count_state(), keyspace.clone(), Some(0))
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
        let epoch: u64 = 0;
        let mut write_batch = keyspace.state_store().start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();

        // get output
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );

        // reload the state and check the output
        let mut managed_state = ManagedValueState::new(create_test_count_state(), keyspace, None)
            .await
            .unwrap();
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );
    }
}
