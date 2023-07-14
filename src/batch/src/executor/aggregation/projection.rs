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

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::agg::{Aggregator, BoxedAggState};
use risingwave_expr::Result;

#[derive(Clone)]
pub struct Projection {
    inner: BoxedAggState,
    indices: Vec<usize>,
}

impl Projection {
    pub fn new(indices: Vec<usize>, inner: BoxedAggState) -> Self {
        Self { inner, indices }
    }
}

#[async_trait::async_trait]
impl Aggregator for Projection {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update_multi(
        &mut self,
        input: &StreamChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.inner
            .update_multi(
                &input.clone().project(&self.indices),
                start_row_id,
                end_row_id,
            )
            .await
    }

    fn output(&mut self) -> Result<Datum> {
        self.inner.output()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn get_state(&self) -> Datum {
        self.inner.get_state()
    }

    fn set_state(&mut self, state: Datum) {
        self.inner.set_state(state);
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.estimated_size()
    }
}
