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

use risingwave_common::array::ArrayBuilderImpl;
use risingwave_common::types::{Datum, ListValue, ScalarRefImpl};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate;
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::expr::Context;

#[aggregate("array_agg(any) -> anyarray")]
fn array_agg(state: &mut ArrayAggState, value: Option<ScalarRefImpl<'_>>, ctx: &Context) {
    state
        .0
        .get_or_insert_with(|| ctx.arg_types[0].create_array_builder(1))
        .append(value);
}

#[derive(Debug, Clone, Default)]
struct ArrayAggState(Option<ArrayBuilderImpl>);

impl EstimateSize for ArrayAggState {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl AggStateDyn for ArrayAggState {}

/// Finishes aggregation and returns the result.
impl From<&ArrayAggState> for Datum {
    fn from(state: &ArrayAggState) -> Self {
        state
            .0
            .as_ref()
            .map(|b| ListValue::new(b.clone().finish()).into())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StreamChunk};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::Result;
    use risingwave_expr::aggregate::{AggCall, build_append_only, build_list_state};

    #[tokio::test]
    async fn test_array_agg_basic() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " i
            + 123
            + 456
            + 789",
        );
        let array_agg = build_append_only(&AggCall::from_pretty("(array_agg:int4[] $0:int4)"))?;
        let mut state = array_agg.create_state()?;
        array_agg.update(&mut state, &chunk).await?;
        let actual = array_agg.get_result(&state).await?;
        assert_eq!(actual, Some(ListValue::from_iter([123, 456, 789]).into()));
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_empty() -> Result<()> {
        let array_agg = build_append_only(&AggCall::from_pretty("(array_agg:int4[] $0:int4)"))?;
        let mut state = array_agg.create_state()?;

        assert_eq!(array_agg.get_result(&state).await?, None);

        let chunk = StreamChunk::from_pretty(
            " i
            + .",
        );
        array_agg.update(&mut state, &chunk).await?;
        assert_eq!(
            array_agg.get_result(&state).await?,
            Some(ListValue::from_iter([None::<i32>]).into())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_list_state_unordered() -> Result<()> {
        let agg = build_list_state(&AggCall::from_pretty("(array_agg:int4[] $0:int4)"))?;
        let mut state = agg.create_state()?;
        agg.update(
            &mut state,
            &StreamChunk::from_pretty(
                " i
                + 1
                + .
                + 2
                + 1",
            ),
        )
        .await?;
        assert_eq!(
            agg.get_result(&state).await?,
            Some(ListValue::from_iter([Some(1), None, Some(2), Some(1)]).into())
        );
        // retract removes the first match; a never-inserted row is ignored
        agg.update(
            &mut state,
            &StreamChunk::from_pretty(
                " i
                - 1
                - .
                - 9",
            ),
        )
        .await?;
        assert_eq!(
            agg.get_result(&state).await?,
            Some(ListValue::from_iter([2, 1]).into())
        );
        agg.update(
            &mut state,
            &StreamChunk::from_pretty(
                " i
                - 2
                - 1",
            ),
        )
        .await?;
        assert_eq!(agg.get_result(&state).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_list_state_ordered() -> Result<()> {
        // array_agg(value order by k1 asc, k2 desc) where value=$0, k1=$1, k2=$2
        let agg = build_list_state(&AggCall::from_pretty(
            "(array_agg:int4[] $0:int4 orderby $1:asc $2:desc)",
        ))?;
        let mut state = agg.create_state()?;
        agg.update(
            &mut state,
            &StreamChunk::from_pretty(
                " i i i
                + 1 2 1
                + 2 1 1
                + 3 2 5
                + 4 . 1
                + 5 1 1",
            ),
        )
        .await?;
        // k1 asc with nulls last by default, ties keep arrival order
        assert_eq!(
            agg.get_result(&state).await?,
            Some(ListValue::from_iter([2, 5, 3, 1, 4]).into())
        );

        // encode/decode roundtrip keeps the sorted state
        let encoded = agg.encode_state(&state)?;
        let mut state = agg.decode_state(encoded)?;
        assert_eq!(
            agg.get_result(&state).await?,
            Some(ListValue::from_iter([2, 5, 3, 1, 4]).into())
        );

        agg.update(
            &mut state,
            &StreamChunk::from_pretty(
                " i i i
                - 3 2 5
                U- 2 1 1
                U+ 2 3 1",
            ),
        )
        .await?;
        assert_eq!(
            agg.get_result(&state).await?,
            Some(ListValue::from_iter([5, 1, 2, 4]).into())
        );
        Ok(())
    }
}
