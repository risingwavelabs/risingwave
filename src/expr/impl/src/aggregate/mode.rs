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

#[build_aggregate("mode(any) -> any")]
fn build(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    Ok(Box::new(Mode {
        return_type: agg.return_type.clone(),
    }))
}

/// Computes the mode, the most frequent value of the aggregated argument (arbitrarily choosing the
/// first one if there are multiple equally-frequent values). The aggregated argument must be of a
/// sortable type.
///
/// ```slt
/// query I
/// select mode() within group (order by unnest) from unnest(array[1]);
/// ----
/// 1
///
/// query I
/// select mode() within group (order by unnest) from unnest(array[1,2,2,3,3,4,4,4]);
/// ----
/// 4
///
/// query R
/// select mode() within group (order by unnest) from unnest(array[0.1,0.2,0.2,0.4,0.4,0.3,0.3,0.4]);
/// ----
/// 0.4
///
/// query R
/// select mode() within group (order by unnest) from unnest(array[1,2,2,3,3,4,4,4,3]);
/// ----
/// 3
///
/// query T
/// select mode() within group (order by unnest) from unnest(array['1','2','2','3','3','4','4','4','3']);
/// ----
/// 3
///
/// query I
/// select mode() within group (order by unnest) from unnest(array[]::int[]);
/// ----
/// NULL
/// ```
struct Mode {
    return_type: DataType,
}

#[derive(Debug, Clone, EstimateSize, Default)]
struct State {
    cur_mode: Datum,
    cur_mode_freq: usize,
    cur_item: Datum,
    cur_item_freq: usize,
}

impl AggStateDyn for State {}

impl State {
    fn add_datum(&mut self, datum_ref: DatumRef<'_>) {
        let datum = datum_ref.to_owned_datum();
        if datum.is_some() && self.cur_item == datum {
            self.cur_item_freq += 1;
        } else if datum.is_some() {
            self.cur_item = datum;
            self.cur_item_freq = 1;
        }
        if self.cur_item_freq > self.cur_mode_freq {
            self.cur_mode.clone_from(&self.cur_item);
            self.cur_mode_freq = self.cur_item_freq;
        }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Mode {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::<State>::default()))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<State>();
        for (_, row) in input.rows() {
            state.add_datum(row.datum_at(0));
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<State>();
        for (_, row) in input.rows_in(range) {
            state.add_datum(row.datum_at(0));
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        Ok(state.cur_mode.clone())
    }
}
