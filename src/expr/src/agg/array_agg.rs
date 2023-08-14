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

use risingwave_common::array::ListValue;
use risingwave_common::types::{Datum, ScalarImpl, ScalarRef};
use risingwave_expr_macro::aggregate;

#[aggregate("array_agg(*) -> list", state = "State")]
fn array_agg<'a, T: ScalarRef<'a>>(state: Option<State>, value: Option<T>) -> State {
    let mut state = state.unwrap_or_default();
    state.0.push(value.map(|v| v.to_owned_scalar().into()));
    state
}

#[derive(Default, Clone)]
struct State(Vec<Datum>);

impl From<State> for ListValue {
    fn from(state: State) -> Self {
        ListValue::new(state.0)
    }
}

impl TryFrom<ScalarImpl> for State {
    type Error = ();

    fn try_from(state: ScalarImpl) -> Result<Self, Self::Error> {
        state.try_into().map_err(|_| ())
    }
}

impl From<State> for ScalarImpl {
    fn from(state: State) -> Self {
        ListValue::new(state.0).into()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StreamChunk};
    use risingwave_common::test_prelude::StreamChunkTestExt;

    use crate::agg::AggCall;
    use crate::Result;

    #[tokio::test]
    async fn test_array_agg_basic() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " i
            + 123
            + 456
            + 789",
        );
        let array_agg = crate::agg::build(&AggCall::from_pretty("(array_agg:int4[] $0:int4)"))?;
        let mut state = array_agg.init_state();
        array_agg.update(&mut state, &chunk).await?;
        let actual = array_agg.get_result(&state).await?;
        assert_eq!(
            actual,
            Some(ListValue::new(vec![Some(123.into()), Some(456.into()), Some(789.into())]).into())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_empty() -> Result<()> {
        let array_agg = crate::agg::build(&AggCall::from_pretty("(array_agg:int4[] $0:int4)"))?;
        let mut state = array_agg.init_state();

        assert_eq!(array_agg.get_result(&state).await?, None);

        let chunk = StreamChunk::from_pretty(
            " i
            + .",
        );
        array_agg.update(&mut state, &chunk).await?;
        assert_eq!(
            array_agg.get_result(&state).await?,
            Some(ListValue::new(vec![None]).into())
        );
        Ok(())
    }
}
