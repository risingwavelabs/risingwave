// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use dyn_clone::DynClone;
use risingwave_common::array::StreamChunk;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::{DataType, DataTypeName, Datum};

use crate::sig::FuncSigDebug;
use crate::{ExprError, Result};

// aggregate definition
mod def;

// concrete aggregators
mod approx_count_distinct;
mod array_agg;
mod general;
mod jsonb_agg;
mod mode;
mod percentile_cont;
mod percentile_disc;
mod string_agg;

pub use self::def::*;

/// An `Aggregator` supports `update` data and `output` result.
#[async_trait::async_trait]
pub trait Aggregator: Send + Sync + DynClone + 'static {
    fn return_type(&self) -> DataType;

    /// `update_multi` update the aggregator with multiple rows with type checked at runtime.
    async fn update_multi(
        &mut self,
        input: &StreamChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()>;

    /// Update the aggregator with all rows in the chunk.
    async fn update(&mut self, input: &StreamChunk) -> Result<()> {
        self.update_multi(input, 0, input.capacity()).await
    }

    /// `update_single` update the aggregator with a single row with type checked at runtime.
    async fn update_single(&mut self, input: &StreamChunk, row_id: usize) -> Result<()> {
        self.update_multi(input, row_id, row_id + 1).await
    }

    /// Output the aggregate state and reset to initial state.
    fn output(&mut self) -> Result<Datum>;

    /// Reset the state.
    fn reset(&mut self);

    /// Get the current value state.
    fn get(&self) -> Datum;

    /// Set the current value state.
    fn set(&mut self, state: Datum);

    /// The estimated size of the state.
    fn estimated_size(&self) -> usize;
}

dyn_clone::clone_trait_object!(Aggregator);

pub type BoxedAggState = Box<dyn Aggregator>;

impl EstimateSize for BoxedAggState {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().estimated_size()
    }
}

/// Build an `Aggregator` from `AggCall`.
///
/// NOTE: This function ignores argument indices, `column_orders`, `filter` and `distinct` in
/// `AggCall`. Such operations should be done in batch or streaming executors.
pub fn build(agg: AggCall) -> Result<BoxedAggState> {
    // NOTE: The function signature is checked by `AggCall::infer_return_type` in the frontend.

    let args = (agg.args.arg_types().iter())
        .map(|t| t.into())
        .collect::<Vec<DataTypeName>>();
    let ret_type = (&agg.return_type).into();
    let desc = crate::sig::agg::AGG_FUNC_SIG_MAP
        .get(agg.kind, &args, ret_type)
        .ok_or_else(|| {
            ExprError::UnsupportedFunction(format!(
                "{:?}",
                FuncSigDebug {
                    func: agg.kind,
                    inputs_type: &args,
                    ret_type,
                    set_returning: false
                }
            ))
        })?;

    (desc.build)(agg.clone())
}
