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
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarImpl};

use super::{Aggregator, BoxedAggState};
use crate::expr::ExpressionRef;
use crate::Result;

/// A special aggregator that filters out rows that do not satisfy the given _condition_
/// and feeds the rows that satisfy to the _inner_ aggregator.
#[derive(Clone)]
pub struct Filter {
    condition: ExpressionRef,
    inner: BoxedAggState,
}

impl Filter {
    pub fn new(condition: ExpressionRef, inner: BoxedAggState) -> Self {
        assert_eq!(condition.return_type(), DataType::Boolean);
        Self { condition, inner }
    }
}

#[async_trait::async_trait]
impl Aggregator for Filter {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        let bitmap = if start_row_id == 0 && end_row_id == input.capacity() {
            // if the input if the whole chunk, use `eval` to speed up
            self.condition.eval(input).await?.as_bool().to_bitmap()
        } else {
            let mut bitmap_builder = BitmapBuilder::default();
            // otherwise, run `eval_row` on each row
            for row_id in start_row_id..end_row_id {
                let (row_ref, vis) = input.row_at(row_id);
                assert!(vis); // cuz the input chunk is supposed to be compacted
                let b = self
                    .condition
                    .eval_row(&row_ref.into_owned_row())
                    .await?
                    .map(ScalarImpl::into_bool)
                    .unwrap_or(false);
                bitmap_builder.append(b);
            }
            bitmap_builder.finish()
        };
        if bitmap.all() {
            // if the bitmap is all set, meaning all rows satisfy the filter,
            // call `update_multi` for potential optimization
            self.inner
                .update_multi(input, start_row_id, end_row_id)
                .await
        } else {
            // TODO(yuchao): we might want to pass visibility bitmap to the
            // inner aggregator, or re-compact the input chunk after filtering.
            // https://github.com/risingwavelabs/risingwave/pull/4972#discussion_r958013816
            for (_, row_id) in (start_row_id..end_row_id)
                .enumerate()
                .filter(|(i, _)| bitmap.is_set(*i))
            {
                self.inner.update_single(input, row_id).await?;
            }
            Ok(())
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        self.inner.output(builder)
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.estimated_size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;
    use crate::expr::{build_from_pretty, Expression, LiteralExpression};

    #[derive(Clone)]
    struct MockAgg {
        count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Aggregator for MockAgg {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn update_multi(
            &mut self,
            _input: &DataChunk,
            start_row_id: usize,
            end_row_id: usize,
        ) -> Result<()> {
            self.count
                .fetch_add(end_row_id - start_row_id, Ordering::Relaxed);
            Ok(())
        }

        fn output(&mut self, _builder: &mut ArrayBuilderImpl) -> Result<()> {
            unimplemented!()
        }

        fn estimated_size(&self) -> usize {
            std::mem::size_of::<AtomicUsize>()
        }
    }

    #[tokio::test]
    async fn test_selective_agg_always_true() -> Result<()> {
        let condition =
            Arc::from(LiteralExpression::new(DataType::Boolean, Some(true.into())).boxed());
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            condition,
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = DataChunk::from_pretty(
            "I
             9
             5
             6
             1",
        );

        agg.update_single(&chunk, 0).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_multi(&chunk, 2, 4).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 3);

        agg.update_multi(&chunk, 0, chunk.capacity()).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg() -> Result<()> {
        let expr = build_from_pretty("(greater_than:boolean $0:int8 5:int8)");
        let condition = Arc::from(expr);
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            condition,
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = DataChunk::from_pretty(
            "I
             9
             5
             6
             1",
        );

        agg.update_single(&chunk, 0).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_single(&chunk, 1).await?; // should be filtered out
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_multi(&chunk, 2, 4).await?; // only 6 should be applied
        assert_eq!(agg_count.load(Ordering::Relaxed), 2);

        agg.update_multi(&chunk, 0, chunk.capacity()).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg_null_condition() -> Result<()> {
        let expr = build_from_pretty("(equal:boolean $0:int8 null:int8)");
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            Arc::from(expr),
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = DataChunk::from_pretty(
            "I
             9
             5
             6
             1",
        );

        agg.update_single(&chunk, 0).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        agg.update_multi(&chunk, 2, 4).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        agg.update_multi(&chunk, 0, chunk.capacity()).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
