// Copyright 2024 RisingWave Labs
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

use std::ops::Range;

use risingwave_common::array::*;
use risingwave_common::types::*;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggStateDyn, AggregateFunction, AggregateState};
use risingwave_expr::{build_aggregate, Result};

#[build_aggregate("approx_percentile(float8) -> float8")]
fn build(agg: &AggCall) -> Result<Box<dyn AggregateFunction>> {
    let fraction = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into());
    Ok(Box::new(ApproxPercentile { fraction }))
}

#[allow(dead_code)]
pub struct ApproxPercentile {
    fraction: Option<f64>,
}

#[derive(Debug, Default, EstimateSize)]
struct State(Vec<f64>);

impl AggStateDyn for State {}

#[async_trait::async_trait]
impl AggregateFunction for ApproxPercentile {
    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn create_state(&self) -> Result<AggregateState> {
        todo!()
    }

    async fn update(&self, _state: &mut AggregateState, _input: &StreamChunk) -> Result<()> {
        todo!()
    }

    async fn update_range(
        &self,
        _state: &mut AggregateState,
        _input: &StreamChunk,
        _range: Range<usize>,
    ) -> Result<()> {
        todo!()
    }

    async fn get_result(&self, _state: &AggregateState) -> Result<Datum> {
        todo!()
    }
}
