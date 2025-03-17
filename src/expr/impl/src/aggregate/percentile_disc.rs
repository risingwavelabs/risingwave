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
use risingwave_expr::aggregate::{
    AggCall, AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction,
};
use risingwave_expr::{Result, build_aggregate};

/// Computes the discrete percentile, the first value within the ordered set of aggregated argument
/// values whose position in the ordering equals or exceeds the specified fraction. The aggregated
/// argument must be of a sortable type.
///
/// ```slt
/// statement ok
/// create table t(x int, y bigint, z real, w double, v varchar);
///
/// statement ok
/// insert into t values(1,10,100,1000,'10000'),(2,20,200,2000,'20000'),(3,30,300,3000,'30000');
///
/// query R
/// select percentile_disc(0) within group (order by x) from t;
/// ----
/// 1
///
/// query R
/// select percentile_disc(0.33) within group (order by y) from t;
/// ----
/// 10
///
/// query R
/// select percentile_disc(0.34) within group (order by z) from t;
/// ----
/// 200
///
/// query R
/// select percentile_disc(0.67) within group (order by w) from t
/// ----
/// 3000
///
/// query R
/// select percentile_disc(1) within group (order by v) from t;
/// ----
/// 30000
///
/// query R
/// select percentile_disc(NULL) within group (order by w) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[build_aggregate("percentile_disc(any) -> any")]
fn build(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    let fractions = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into());
    Ok(Box::new(PercentileDisc::new(
        fractions,
        agg.return_type.clone(),
    )))
}

#[derive(Clone)]
pub struct PercentileDisc {
    fractions: Option<f64>,
    return_type: DataType,
}

#[derive(Debug, Default)]
struct State(Vec<ScalarImpl>);

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        std::mem::size_of_val(self.0.as_slice())
    }
}

impl AggStateDyn for State {}

impl PercentileDisc {
    pub fn new(fractions: Option<f64>, return_type: DataType) -> Self {
        Self {
            fractions,
            return_type,
        }
    }

    fn add_datum(&self, state: &mut State, datum_ref: DatumRef<'_>) {
        if let Some(datum) = datum_ref.to_owned_datum() {
            state.0.push(datum);
        }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for PercentileDisc {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
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
            if let Some(fractions) = self.fractions
                && !state.is_empty()
            {
                let idx = if fractions == 0.0 {
                    0
                } else {
                    f64::ceil(fractions * state.len() as f64) as usize - 1
                };
                Some(state[idx].clone())
            } else {
                None
            },
        )
    }
}
