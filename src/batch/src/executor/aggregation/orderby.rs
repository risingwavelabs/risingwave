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

use std::ops::Range;

use anyhow::anyhow;
use futures_util::FutureExt;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::agg::{Aggregator, BoxedAggState};
use risingwave_expr::{ExprError, Result};

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
    unordered_values_estimated_heap_size: usize,
}

type OrderKey = Box<[u8]>;

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
            unordered_values_estimated_heap_size: 0,
        }
    }

    fn push_row(&mut self, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .map_err(|e| ExprError::Internal(anyhow!("failed to encode row, error: {}", e)))?;
        let projected_row = row.project(&self.arg_indices).to_owned_row();

        self.unordered_values_estimated_heap_size +=
            key.len() + projected_row.estimated_heap_size();
        self.unordered_values.push((key.into(), projected_row));
        Ok(())
    }
}

#[async_trait::async_trait]
impl Aggregator for ProjectionOrderBy {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update(&mut self, input: &StreamChunk) -> Result<()> {
        self.unordered_values.reserve(input.cardinality());
        for (op, row) in input.rows() {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(row)?;
        }
        Ok(())
    }

    async fn update_range(&mut self, input: &StreamChunk, range: Range<usize>) -> Result<()> {
        self.unordered_values.reserve(range.len());
        for (op, row) in input.rows_in(range) {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(row)?;
        }
        Ok(())
    }

    fn get_output(&self) -> Result<Datum> {
        unimplemented!("get_output is not supported for orderby");
    }

    fn output(&mut self) -> Result<Datum> {
        // sort
        self.unordered_values_estimated_heap_size = 0;
        let mut rows = std::mem::take(&mut self.unordered_values);
        rows.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        // build chunk
        let mut chunk_builder = DataChunkBuilder::new(self.arg_types.clone(), 1024);
        for (_, row) in rows {
            if let Some(data_chunk) = chunk_builder.append_one_row(row) {
                let chunk = StreamChunk::from(data_chunk);
                self.inner
                    .update(&chunk)
                    .now_or_never()
                    .expect("todo: support async aggregation with orderby")?;
            }
        }
        if let Some(data_chunk) = chunk_builder.consume_all() {
            let chunk = StreamChunk::from(data_chunk);
            self.inner
                .update(&chunk)
                .now_or_never()
                .expect("todo: support async aggregation with orderby")?;
        }
        self.inner.output()
    }

    fn reset(&mut self) {
        unimplemented!("reset is not supported for orderby");
    }

    fn get_state(&self) -> Datum {
        unimplemented!("get is not supported for orderby");
    }

    fn set_state(&mut self, _: Datum) {
        unimplemented!("set is not supported for orderby");
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.inner.estimated_size()
            + self.unordered_values.capacity() * std::mem::size_of::<(OrderKey, OwnedRow)>()
            + self.unordered_values_estimated_heap_size
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StreamChunk};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::agg::AggCall;

    use super::super::build;

    #[tokio::test]
    async fn array_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " i    i
            + 123  3
            + 456  2
            + 789  2
            + 321  9",
        );
        let mut agg = build(&AggCall::from_pretty(
            "(array_agg:int4[] $0:int4 orderby $1:asc $0:desc)",
        ))
        .unwrap();
        agg.update(&chunk).await.unwrap();
        assert_eq!(
            agg.output().unwrap(),
            Some(
                ListValue::new(vec![
                    Some(789.into()),
                    Some(456.into()),
                    Some(123.into()),
                    Some(321.into()),
                ])
                .into()
            )
        );
    }

    #[tokio::test]
    async fn string_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " T   T i i
            + aaa _ 1 3
            + bbb _ 0 4
            + ccc _ 0 8
            + ddd _ 1 3",
        );
        let mut agg = build(&AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar orderby $2:asc $3:desc $0:desc)",
        ))
        .unwrap();
        agg.update(&chunk).await.unwrap();
        assert_eq!(agg.output().unwrap(), Some("ccc_bbb_ddd_aaa".into()));
    }
}
