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

use anyhow::anyhow;
use futures_util::FutureExt;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, RowRef};
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::{Aggregator, BoxedAggState};
use crate::{ExprError, Result};

/// `ProjectionOrderBy` is a wrapper of `Aggregator` that sorts rows by given columns and then
/// projects columns.
#[derive(Clone)]
pub struct ProjectionOrderBy {
    inner: BoxedAggState,
    arg_types: Vec<DataType>,
    arg_indices: Vec<usize>,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
    unordered_values: Vec<(OrderKey, OwnedRow)>,
    unordered_values_estimated_size: usize,
}

type OrderKey = Vec<u8>;

impl ProjectionOrderBy {
    pub fn new(
        arg_types: Vec<DataType>,
        arg_indices: Vec<usize>,
        column_orders: Vec<ColumnOrder>,
        inner: BoxedAggState,
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
            unordered_values: vec![],
            unordered_values_estimated_size: 0,
        }
    }

    fn push_row(&mut self, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .map_err(|e| ExprError::Internal(anyhow!("failed to encode row, error: {}", e)))?;
        let projected_row = row.project(&self.arg_indices).to_owned_row();

        self.unordered_values_estimated_size += key.len() + projected_row.estimated_size();
        self.unordered_values.push((key, projected_row));
        Ok(())
    }
}

#[async_trait::async_trait]
impl Aggregator for ProjectionOrderBy {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.unordered_values.reserve(end_row_id - start_row_id);
        for row_id in start_row_id..end_row_id {
            let (row, vis) = input.row_at(row_id);
            if vis {
                self.push_row(row)?;
            }
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        // sort
        self.unordered_values_estimated_size = 0;
        let mut rows = std::mem::take(&mut self.unordered_values);
        rows.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        // build chunk
        let mut chunk_builder = DataChunkBuilder::new(self.arg_types.clone(), 1024);
        for (_, row) in rows {
            if let Some(chunk) = chunk_builder.append_one_row(row) {
                self.inner
                    .update_multi(&chunk, 0, chunk.capacity())
                    .now_or_never()
                    .expect("todo: support async aggregation with orderby")?;
            }
        }
        if let Some(chunk) = chunk_builder.consume_all() {
            self.inner
                .update_multi(&chunk, 0, chunk.capacity())
                .now_or_never()
                .expect("todo: support async aggregation with orderby")?;
        }
        self.inner.output(builder)
    }
}

impl EstimateSize for ProjectionOrderBy {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size() + self.unordered_values_estimated_size
    }
}
