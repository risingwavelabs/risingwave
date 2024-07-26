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
use risingwave_common::row::Row;
use risingwave_common::types::*;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggStateDyn, AggregateFunction, AggregateState};
use risingwave_expr::{build_aggregate, Result};

/// TODO(kwannoel): for single phase agg, we can actually support `UDDSketch`.
/// For two phase agg, we still use `DDSketch`.
/// Then we also need to store the `relative_error` of the sketch, so we can report it
/// in an internal table, if it changes.
/// TODO(kwannoel): We also need to test 0 < x < 1 range.
#[build_aggregate("approx_percentile(float8) -> float8", state = "bytea")]
fn build(agg: &AggCall) -> Result<Box<dyn AggregateFunction>> {
    let quantile = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into())
        .unwrap();
    let relative_error: f64 = agg.direct_args[1]
        .literal()
        .map(|x| (*x.as_float64()).into())
        .unwrap();
    let base = (1.0 + relative_error) / (1.0 - relative_error);
    Ok(Box::new(ApproxPercentile { quantile, base }))
}

#[allow(dead_code)]
pub struct ApproxPercentile {
    quantile: f64,
    base: f64,
}

type BucketCount = u64;
type BucketId = i32;
type Count = u64;

// TODO(kwannoel): handle `0` and negative values.
#[derive(Debug, Default)]
struct State {
    count: BucketCount,
    buckets: BTreeMap<BucketId, Count>,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        let count_size = 1;
        let bucket_size = self.buckets.len() * 2;
        count_size + bucket_size
    }
}

impl AggStateDyn for State {}

impl ApproxPercentile {
    fn add_datum(&self, state: &mut State, op: Op, datum: DatumRef<'_>) {
        if let Some(value) = datum {
            let prim_value = value.into_float64().into_inner();
            let bucket_id = prim_value.log(self.base).ceil() as BucketId;
            match op {
                Op::Delete | Op::UpdateDelete => {
                    let count = state.buckets.entry(bucket_id).or_insert(0);
                    *count -= 1;
                    state.count -= 1;
                }
                Op::Insert | Op::UpdateInsert => {
                    let count = state.buckets.entry(bucket_id).or_insert(0);
                    *count += 1;
                    state.count += 1;
                }
            }
        };
    }
}

#[async_trait::async_trait]
impl AggregateFunction for ApproxPercentile {
    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::<State>::default()))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state: &mut State = state.downcast_mut();
        for (op, row) in input.rows() {
            let datum = row.datum_at(0);
            self.add_datum(state, op, datum);
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut();
        for (op, row) in input.rows_in(range) {
            self.add_datum(state, op, row.datum_at(0));
        }
        Ok(())
    }

    // TODO(kwannoel): Instead of iterating over all buckets, we can maintain the
    // approximate quantile bucket on the fly.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let quantile_count = (state.count as f64 * self.quantile) as u64;
        let mut acc_count = 0;
        for (bucket_id, count) in &state.buckets {
            acc_count += count;
            if acc_count >= quantile_count {
                // approx value = 2 * y^i / (y + 1)
                let approx_percentile = 2.0 * self.base.powi(*bucket_id) / (self.base + 1.0);
                let approx_percentile = ScalarImpl::Float64(approx_percentile.into());
                return Ok(Datum::from(approx_percentile));
            }
        }
        return Ok(None);
    }

    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let mut encoded_state = Vec::with_capacity(state.estimated_heap_size());
        encoded_state.extend_from_slice(&state.count.to_be_bytes());
        for (bucket_id, count) in &state.buckets {
            encoded_state.extend_from_slice(&bucket_id.to_be_bytes());
            encoded_state.extend_from_slice(&count.to_be_bytes());
        }
        let encoded_scalar = ScalarImpl::Bytea(encoded_state.into());
        Ok(Datum::from(encoded_scalar))
    }

    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let mut state = State::default();
        let Some(scalar_state) = datum else {
            return Ok(AggregateState::Any(Box::new(state)));
        };
        let encoded_state: Box<[u8]> = scalar_state.into_bytea();
        let mut cursor = 0;
        state.count = u64::from_be_bytes(encoded_state[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        while cursor < encoded_state.len() {
            let bucket_id =
                i32::from_be_bytes(encoded_state[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            let count = u64::from_be_bytes(encoded_state[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            state.buckets.insert(bucket_id, count);
        }
        Ok(AggregateState::Any(Box::new(state)))
    }
}
