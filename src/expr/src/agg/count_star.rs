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
use risingwave_common::types::*;
use risingwave_expr_macro::build_aggregate;

use super::Aggregator;
use crate::function::aggregate::AggCall;
use crate::Result;

#[build_aggregate("count() -> int64")]
fn build_count_star(_: AggCall) -> Result<Box<dyn Aggregator>> {
    Ok(Box::new(CountStar::default()))
}

#[derive(Clone, Default)]
pub struct CountStar {
    result: usize,
}

#[async_trait::async_trait]
impl Aggregator for CountStar {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        if let Some(visibility) = input.visibility() {
            for row_id in start_row_id..end_row_id {
                if visibility.is_set(row_id) {
                    self.result += 1;
                }
            }
        } else {
            self.result += end_row_id - start_row_id;
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        let res = std::mem::replace(&mut self.result, 0) as i64;
        let ArrayBuilderImpl::Int64(b) = builder else {
            bail!("Unexpected builder for count(*).");
        };
        b.append(Some(res));
        Ok(())
    }
}
