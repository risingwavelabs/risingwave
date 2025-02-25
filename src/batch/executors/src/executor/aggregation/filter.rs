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
use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::Result;
use risingwave_expr::aggregate::{AggregateFunction, AggregateState, BoxedAggregateFunction};
use risingwave_expr::expr::Expression;

/// A special aggregator that filters out rows that do not satisfy the given _condition_
/// and feeds the rows that satisfy to the _inner_ aggregator.
pub struct Filter {
    condition: Arc<dyn Expression>,
    inner: BoxedAggregateFunction,
}

impl Filter {
    pub fn new(condition: Arc<dyn Expression>, inner: BoxedAggregateFunction) -> Self {
        assert_eq!(condition.return_type(), DataType::Boolean);
        Self { condition, inner }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Filter {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        self.inner.create_state()
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
        let bitmap = self
            .condition
            .eval(input.data_chunk())
            .await?
            .as_bool()
            .to_bitmap();
        let mut input1 = input.clone();
        input1.set_visibility(input.visibility() & &bitmap);
        self.inner.update_range(state, &input1, range).await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        self.inner.get_result(state).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::aggregate::{AggCall, build_append_only};
    use risingwave_expr::expr::{ExpressionBoxExt, LiteralExpression, build_from_pretty};

    use super::*;

    #[tokio::test]
    async fn test_selective_agg_always_true() -> Result<()> {
        let condition = LiteralExpression::new(DataType::Boolean, Some(true.into())).boxed();
        let agg = Filter::new(
            condition.into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state()?;

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&mut state, &chunk, 0..1).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 1);

        agg.update_range(&mut state, &chunk, 2..4).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 3);

        agg.update(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg() -> Result<()> {
        let expr = build_from_pretty("(greater_than:boolean $0:int8 5:int8)");
        let agg = Filter::new(
            expr.into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state()?;

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&mut state, &chunk, 0..1).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 1);

        agg.update_range(&mut state, &chunk, 1..2).await?; // should be filtered out
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 1);

        agg.update_range(&mut state, &chunk, 2..4).await?; // only 6 should be applied
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 2);

        agg.update(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg_null_condition() -> Result<()> {
        let expr = build_from_pretty("(equal:boolean $0:int8 null:int8)");
        let agg = Filter::new(
            expr.into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state()?;

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&mut state, &chunk, 0..1).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 0);

        agg.update_range(&mut state, &chunk, 2..4).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 0);

        agg.update(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 0);

        Ok(())
    }
}
