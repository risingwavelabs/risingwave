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

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;

use crate::executor::aggregation::{create_streaming_aggregator, AggCall, StreamingAggImpl};
use crate::executor::error::StreamExecutorResult;

/// A wrapper around [`StreamingAggImpl`], which fetches data from the state store and helps
/// update the state. We don't use any trait to wrap around all `ManagedXxxState`, so as to reduce
/// the overhead of creating boxed async future.
pub struct ManagedValueState {
    /// Upstream column indices of agg arguments.
    arg_indices: Vec<usize>,

    /// The internal single-value state.
    inner: Box<dyn StreamingAggImpl>,
}

impl ManagedValueState {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub fn new(agg_call: &AggCall, prev_output: Option<Datum>) -> StreamExecutorResult<Self> {
        // Create the internal state based on the value we get.
        Ok(Self {
            arg_indices: agg_call.args.val_indices().to_vec(),
            inner: create_streaming_aggregator(
                agg_call.args.arg_types(),
                &agg_call.kind,
                &agg_call.return_type,
                prev_output,
            )?,
        })
    }

    /// Apply a chunk of data to the state.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, columns));
        let data = self
            .arg_indices
            .iter()
            .map(|col_idx| columns[*col_idx])
            .collect_vec();
        self.inner.apply_batch(ops, visibility, &data)
    }

    /// Get the output of the state. Note that in our case, getting the output is very easy, as the
    /// output is the same as the aggregation state. In other aggregators, like min and max,
    /// `get_output` might involve a scan from the state store.
    pub fn get_output(&self) -> Datum {
        self.inner
            .get_output()
            .expect("agg call throw an error in streamAgg")
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;
    use crate::executor::aggregation::AggArgs;

    fn create_test_count_agg() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Count,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state_count() {
        let agg_call = create_test_count_agg();
        let mut managed_state = ManagedValueState::new(&agg_call, None).unwrap();

        // apply a batch and get the output
        managed_state
            .apply_chunk(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(1), Some(2), None]).into()],
            )
            .unwrap();

        // get output
        let output = managed_state.get_output();
        assert_eq!(output, Some(ScalarImpl::Int64(3)));

        // check recovery
        let mut managed_state = ManagedValueState::new(&agg_call, Some(output)).unwrap();
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(3)));
        managed_state
            .apply_chunk(
                &[Op::Insert, Op::Insert, Op::Delete, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(42), None, Some(2), Some(8)]).into()],
            )
            .unwrap();
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(4)));
    }

    fn create_test_max_agg_append_only() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Max,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            order_pairs: vec![],
            append_only: true,
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state_append_only_max() {
        let agg_call = create_test_max_agg_append_only();
        let mut managed_state = ManagedValueState::new(&agg_call, None).unwrap();

        // apply a batch and get the output
        managed_state
            .apply_chunk(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(-1), Some(0), Some(2), Some(1), None]).into()],
            )
            .unwrap();

        // get output
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(2)));
    }
}
