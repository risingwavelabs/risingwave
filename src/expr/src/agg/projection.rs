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

use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::DataType;

use super::{Aggregator, BoxedAggState};
use crate::Result;

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
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.inner
            .update_multi(
                &input.clone().reorder_columns(&self.indices),
                start_row_id,
                end_row_id,
            )
            .await
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        self.inner.output(builder)
    }
}

impl EstimateSize for Projection {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}
