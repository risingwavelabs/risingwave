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

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::Result;
use risingwave_expr::aggregate::{AggregateFunction, AggregateState, BoxedAggregateFunction};

pub struct Projection {
    inner: BoxedAggregateFunction,
    indices: Vec<usize>,
}

impl Projection {
    pub fn new(indices: Vec<usize>, inner: BoxedAggregateFunction) -> Self {
        Self { inner, indices }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Projection {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        self.inner.create_state()
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        self.inner
            .update(state, &input.project(&self.indices))
            .await
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        self.inner
            .update_range(state, &input.project(&self.indices), range)
            .await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        self.inner.get_result(state).await
    }
}
