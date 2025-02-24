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

use anyhow::Context;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::Result;
use risingwave_expr::aggregate::{
    AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction,
};

/// `ProjectionOrderBy` is a wrapper of `AggregateFunction` that sorts rows by given columns and
/// then projects columns.
pub struct ProjectionOrderBy {
    inner: BoxedAggregateFunction,
    arg_types: Vec<DataType>,
    arg_indices: Vec<usize>,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
}

#[derive(Debug)]
struct State {
    unordered_values: Vec<(OrderKey, OwnedRow)>,
    unordered_values_estimated_heap_size: usize,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.unordered_values.capacity() * std::mem::size_of::<(OrderKey, OwnedRow)>()
            + self.unordered_values_estimated_heap_size
    }
}

impl AggStateDyn for State {}

type OrderKey = Box<[u8]>;

impl ProjectionOrderBy {
    pub fn new(
        arg_types: Vec<DataType>,
        arg_indices: Vec<usize>,
        column_orders: Vec<ColumnOrder>,
        inner: BoxedAggregateFunction,
    ) -> Self {
        let (order_col_indices, order_types) = column_orders
            .into_iter()
            .map(|c| (c.column_index, c.order_type))
            .unzip();
        Self {
            inner,
            arg_types,
            arg_indices,
            order_col_indices,
            order_types,
        }
    }

    fn push_row(&self, state: &mut State, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .context("failed to encode row")?;
        let projected_row = row.project(&self.arg_indices).to_owned_row();

        state.unordered_values_estimated_heap_size +=
            key.len() + projected_row.estimated_heap_size();
        state.unordered_values.push((key.into(), projected_row));
        Ok(())
    }
}

#[async_trait::async_trait]
impl AggregateFunction for ProjectionOrderBy {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::new(State {
            unordered_values: vec![],
            unordered_values_estimated_heap_size: 0,
        })))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<State>();
        state.unordered_values.reserve(input.cardinality());
        for (op, row) in input.rows() {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(state, row)?;
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
        state.unordered_values.reserve(range.len());
        for (op, row) in input.rows_in(range) {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(state, row)?;
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let mut inner_state = self.inner.create_state()?;
        // sort
        let mut rows = state.unordered_values.clone();
        rows.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        // build chunk
        let mut chunk_builder = DataChunkBuilder::new(self.arg_types.clone(), 1024);
        for (_, row) in rows {
            if let Some(data_chunk) = chunk_builder.append_one_row(row) {
                let chunk = StreamChunk::from(data_chunk);
                self.inner.update(&mut inner_state, &chunk).await?;
            }
        }
        if let Some(data_chunk) = chunk_builder.consume_all() {
            let chunk = StreamChunk::from(data_chunk);
            self.inner.update(&mut inner_state, &chunk).await?;
        }
        self.inner.get_result(&inner_state).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StreamChunk};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::aggregate::AggCall;

    use super::super::build;

    #[tokio::test]
    async fn array_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " i    i
            + 123  3
            + 456  2
            + 789  2
            + 321  9",
        );
        let agg = build(&AggCall::from_pretty(
            "(array_agg:int4[] $0:int4 orderby $1:asc $0:desc)",
        ))
        .unwrap();
        let mut state = agg.create_state().unwrap();
        agg.update(&mut state, &chunk).await.unwrap();
        assert_eq!(
            agg.get_result(&state).await.unwrap(),
            Some(ListValue::from_iter([789, 456, 123, 321]).into())
        );
    }

    #[tokio::test]
    async fn string_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " T   T i i
            + aaa _ 1 3
            + bbb _ 0 4
            + ccc _ 0 8
            + ddd _ 1 3",
        );
        let agg = build(&AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar orderby $2:asc $3:desc $0:desc)",
        ))
        .unwrap();
        let mut state = agg.create_state().unwrap();
        agg.update(&mut state, &chunk).await.unwrap();
        assert_eq!(
            agg.get_result(&state).await.unwrap(),
            Some("ccc_bbb_ddd_aaa".into())
        );
    }
}
