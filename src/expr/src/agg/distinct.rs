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

use std::collections::HashSet;

use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DataType;

use super::{Aggregator, BoxedAggState};
use crate::Result;

/// `Distinct` is a wrapper of `Aggregator` that only keeps distinct rows.
#[derive(Clone)]
pub struct Distinct {
    inner: BoxedAggState,
    exists: HashSet<OwnedRow>, // TODO: optimize for small rows
    exists_estimated_size: usize,
}

impl Distinct {
    pub fn new(inner: BoxedAggState) -> Self {
        Self {
            inner,
            exists: Default::default(),
            exists_estimated_size: 0,
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for Distinct {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        let mut bitmap_builder = BitmapBuilder::with_capacity(input.capacity());
        bitmap_builder.append_bitmap(&input.vis().to_bitmap());
        for row_id in start_row_id..end_row_id {
            let (row_ref, vis) = input.row_at(row_id);
            let row = row_ref.to_owned_row();
            let row_size = row.estimated_size();
            let b = vis && self.exists.insert(row);
            if b {
                self.exists_estimated_size += row_size;
            }
            bitmap_builder.set(row_id, b);
        }
        let mut input = input.clone();
        input.set_visibility(bitmap_builder.finish());

        self.inner
            .update_multi(&input, start_row_id, end_row_id)
            .await
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        self.inner.output(builder)
    }
}

impl EstimateSize for Distinct {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size() + self.exists_estimated_size
    }
}
