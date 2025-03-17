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

use std::collections::HashSet;
use std::ops::Range;

use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::Result;
use risingwave_expr::aggregate::{
    AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction,
};

/// `Distinct` is a wrapper of `Aggregator` that only keeps distinct rows.
pub struct Distinct {
    inner: BoxedAggregateFunction,
}

/// The intermediate state for distinct aggregation.
#[derive(Debug)]
struct State {
    /// Inner aggregate function state.
    inner: AggregateState,
    /// The set of distinct rows.
    exists: HashSet<OwnedRow>, // TODO: optimize for small rows
    exists_estimated_heap_size: usize,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_size()
            + self.exists.capacity() * std::mem::size_of::<OwnedRow>()
            + self.exists_estimated_heap_size
    }
}

impl AggStateDyn for State {}

impl Distinct {
    pub fn new(inner: BoxedAggregateFunction) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Distinct {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::new(State {
            inner: self.inner.create_state()?,
            exists: HashSet::new(),
            exists_estimated_heap_size: 0,
        })))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        self.update_range(state, input, 0..input.capacity()).await
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<State>();

        let mut bitmap_builder = BitmapBuilder::with_capacity(input.capacity());
        bitmap_builder.append_bitmap(input.data_chunk().visibility());
        for row_id in range.clone() {
            let (row_ref, vis) = input.data_chunk().row_at(row_id);
            let row = row_ref.to_owned_row();
            let row_size = row.estimated_heap_size();
            let b = vis && state.exists.insert(row);
            if b {
                state.exists_estimated_heap_size += row_size;
            }
            bitmap_builder.set(row_id, b);
        }
        let input = input.clone_with_vis(bitmap_builder.finish());
        self.inner
            .update_range(&mut state.inner, &input, range)
            .await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        self.inner.get_result(&state.inner).await
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{Datum, Decimal};
    use risingwave_expr::aggregate::AggCall;

    use super::super::build;

    #[test]
    fn distinct_sum_int32() {
        let input = StreamChunk::from_pretty(
            " i
            + 1
            + 1
            + 3",
        );
        test_agg("(sum:int8 $0:int4 distinct)", input, Some(4i64.into()));
    }

    #[test]
    fn distinct_sum_int64() {
        let input = StreamChunk::from_pretty(
            " I
            + 1
            + 1
            + 3",
        );
        test_agg(
            "(sum:decimal $0:int8 distinct)",
            input,
            Some(Decimal::from(4).into()),
        );
    }

    #[test]
    fn distinct_min_float32() {
        let input = StreamChunk::from_pretty(
            " f
            + 1.0
            + 2.0
            + 3.0",
        );
        test_agg(
            "(min:float4 $0:float4 distinct)",
            input,
            Some(1.0f32.into()),
        );
    }

    #[test]
    fn distinct_min_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg(
            "(min:varchar $0:varchar distinct)",
            input,
            Some("aa".into()),
        );
    }

    #[test]
    fn distinct_max_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg("(max:varchar $0:varchar distinct)", input, Some("b".into()));
    }

    #[test]
    fn distinct_count_int32() {
        let input = StreamChunk::from_pretty(
            " i
            + 1
            + 1
            + 3",
        );
        test_agg("(count:int8 $0:int4 distinct)", input, Some(2i64.into()));

        let input = StreamChunk::from_pretty("i");
        test_agg("(count:int8 $0:int4 distinct)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            + .",
        );
        test_agg("(count:int8 $0:int4 distinct)", input, Some(0i64.into()));
    }

    fn test_agg(pretty: &str, input: StreamChunk, expected: Datum) {
        let agg = build(&AggCall::from_pretty(pretty)).unwrap();
        let mut state = agg.create_state().unwrap();
        agg.update(&mut state, &input)
            .now_or_never()
            .unwrap()
            .unwrap();
        let actual = agg.get_result(&state).now_or_never().unwrap().unwrap();
        assert_eq!(actual, expected);
    }
}
