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

use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::*;
use risingwave_expr_macro::build_aggregate;

use super::Aggregator;
use crate::agg::AggCall;
use crate::Result;

#[build_aggregate("count() -> int64")]
fn build_count_star(_: AggCall) -> Result<Box<dyn Aggregator>> {
    Ok(Box::<CountStar>::default())
}

#[derive(Clone, Default, EstimateSize)]
pub struct CountStar {
    result: i64,
}

#[async_trait::async_trait]
impl Aggregator for CountStar {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    async fn update_multi(
        &mut self,
        input: &StreamChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        for row_id in start_row_id..end_row_id {
            if input.vis().is_set(row_id) {
                match input.ops()[row_id] {
                    Op::Insert | Op::UpdateInsert => self.result += 1,
                    Op::Delete | Op::UpdateDelete => self.result -= 1,
                }
            }
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        let res = std::mem::replace(&mut self.result, 0);
        let ArrayBuilderImpl::Int64(b) = builder else {
            bail!("Unexpected builder for count(*).");
        };
        b.append(Some(res));
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        EstimateSize::estimated_size(self)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;

    use super::*;

    #[test]
    fn test_countable_agg() {
        let mut state = CountStar::default();

        // when there is no element, output should be `0`.
        assert_eq!(state.result, 0);

        // insert one element to state
        state
            .update_multi(&StreamChunk::from_pretty("i\n+ 0"), 0, 1)
            .now_or_never()
            .unwrap()
            .unwrap();

        // should be one row
        assert_eq!(state.result, 1);

        // delete one element from state
        state
            .update_multi(&StreamChunk::from_pretty("i\n- 0"), 0, 1)
            .now_or_never()
            .unwrap()
            .unwrap();

        // should be 0 rows.
        assert_eq!(state.result, 0);

        // one more deletion, so we are having `-1` elements inside.
        state
            .update_multi(&StreamChunk::from_pretty("i\n- 0"), 0, 1)
            .now_or_never()
            .unwrap()
            .unwrap();

        // should be the same as `TestState`'s output
        assert_eq!(state.result, -1);

        // one more insert, so we are having `0` elements inside.
        state
            .update_multi(&StreamChunk::from_pretty("i\n- 0 D\n+ 1"), 0, 2)
            .now_or_never()
            .unwrap()
            .unwrap();

        // should be `0`
        assert_eq!(state.result, 0);

        // one more deletion, so we are having `-1` elements inside.
        state
            .update_multi(&StreamChunk::from_pretty("i\n- 1\n+ 0 D"), 0, 2)
            .now_or_never()
            .unwrap()
            .unwrap();

        // should be `-1`
        assert_eq!(state.result, -1);
    }
}
