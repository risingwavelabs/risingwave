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

use std::collections::BTreeMap;
use std::ops::Range;

use risingwave_common::array::*;
use risingwave_common::types::*;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggStateDyn, AggregateFunction, AggregateState};
use risingwave_expr::{build_aggregate, Result};

#[build_aggregate("approx_percentile(float8) -> float8")]
fn build(agg: &AggCall) -> Result<Box<dyn AggregateFunction>> {
    let quantile = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into())
        .unwrap();
    let relative_error = agg.direct_args[1]
        .literal()
        .map(|x| (*x.as_float64()).into())
        .unwrap();
    Ok(Box::new(ApproxPercentile {
        quantile,
        relative_error,
    }))
}

#[allow(dead_code)]
pub struct ApproxPercentile {
    quantile: f64,
    relative_error: f64,
}

type BucketCount = u64;
type BucketId = u64;
type Count = u64;

#[derive(Debug, Default)]
struct State {
    count: BucketCount,
    buckets: BTreeMap<BucketId, Count>,
}

impl State {
    fn new() -> Self {
        Self {
            count: 0,
            buckets: BTreeMap::new(),
        }
    }
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        let count_size = 1;
        let bucket_size = self.buckets.len() * 2;
        count_size + bucket_size
    }
}

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
