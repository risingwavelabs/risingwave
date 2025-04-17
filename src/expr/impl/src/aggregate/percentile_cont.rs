// Copyright 2025 RisingWave Labs
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
use risingwave_common::row::Row;
use risingwave_common::types::*;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggStateDyn, AggregateFunction, AggregateState};
use risingwave_expr::{Result, build_aggregate};

/// Computes the continuous percentile, a value corresponding to the specified fraction within the
/// ordered set of aggregated argument values. This will interpolate between adjacent input items if
/// needed.
///
/// ```slt
/// statement ok
/// create table t(x int, y bigint, z real, w double, v varchar);
///
/// statement ok
/// insert into t values(1,10,100,1000,'10000'),(2,20,200,2000,'20000'),(3,30,300,3000,'30000');
///
/// query R
/// select percentile_cont(0.45) within group (order by x desc) from t;
/// ----
/// 2.1
///
/// query R
/// select percentile_cont(0.45) within group (order by y desc) from t;
/// ----
/// 21
///
/// query R
/// select percentile_cont(0.45) within group (order by z desc) from t;
/// ----
/// 210
///
/// query R
/// select percentile_cont(0.45) within group (order by w desc) from t;
/// ----
/// 2100
///
/// query R
/// select percentile_cont(NULL) within group (order by w desc) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[build_aggregate("percentile_cont(float8) -> float8")]
fn build(agg: &AggCall) -> Result<Box<dyn AggregateFunction>> {
    let fraction = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into());
    Ok(Box::new(PercentileCont { fraction }))
}

pub struct PercentileCont {
    fraction: Option<f64>,
}

#[derive(Debug, Default, EstimateSize)]
struct State(Vec<f64>);

impl AggStateDyn for State {}

impl PercentileCont {
    fn add_datum(&self, state: &mut State, datum_ref: DatumRef<'_>) {
        if let Some(datum) = datum_ref.to_owned_datum() {
            state.0.push((*datum.as_float64()).into());
        }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for PercentileCont {
    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::<State>::default()))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut();
        for (_, row) in input.rows() {
            self.add_datum(state, row.datum_at(0));
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
        for (_, row) in input.rows_in(range) {
            self.add_datum(state, row.datum_at(0));
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        Ok(
            if let Some(fraction) = self.fraction
                && !state.is_empty()
            {
                let rn = fraction * (state.len() - 1) as f64;
                let crn = f64::ceil(rn);
                let frn = f64::floor(rn);
                let result = if crn == frn {
                    state[crn as usize]
                } else {
                    (crn - rn) * state[frn as usize] + (rn - frn) * state[crn as usize]
                };
                Some(result.into())
            } else {
                None
            },
        )
    }
}
