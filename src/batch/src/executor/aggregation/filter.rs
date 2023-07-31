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
use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::agg::{Aggregator, BoxedAggState};
use risingwave_expr::expr::Expression;
use risingwave_expr::Result;

/// A special aggregator that filters out rows that do not satisfy the given _condition_
/// and feeds the rows that satisfy to the _inner_ aggregator.
#[derive(Clone)]
pub struct Filter {
    condition: Arc<dyn Expression>,
    inner: BoxedAggState,
}

impl Filter {
    pub fn new(condition: Arc<dyn Expression>, inner: BoxedAggState) -> Self {
        assert_eq!(condition.return_type(), DataType::Boolean);
        Self { condition, inner }
    }
}

#[async_trait::async_trait]
impl Aggregator for Filter {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn update(&mut self, input: &StreamChunk) -> Result<()> {
        self.update_range(input, 0..input.capacity()).await
    }

    async fn update_range(&mut self, input: &StreamChunk, range: Range<usize>) -> Result<()> {
        let bitmap = self
            .condition
            .eval(input.data_chunk())
            .await?
            .as_bool()
            .to_bitmap();
        let mut input1 = input.clone();
        input1.set_vis(input.vis() & &bitmap);
        self.inner.update_range(&input1, range).await
    }

    fn get_output(&self) -> Result<Datum> {
        self.inner.get_output()
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::expr::{build_from_pretty, Expression, LiteralExpression};

    use super::*;

    #[derive(Clone)]
    struct MockAgg {
        count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Aggregator for MockAgg {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn update(&mut self, input: &StreamChunk) -> Result<()> {
            self.count.fetch_add(input.cardinality(), Ordering::Relaxed);
            Ok(())
        }

        async fn update_range(&mut self, input: &StreamChunk, range: Range<usize>) -> Result<()> {
            let count = range.filter(|i| input.vis().is_set(*i)).count();
            self.count.fetch_add(count, Ordering::Relaxed);
            Ok(())
        }

        fn get_output(&self) -> Result<Datum> {
            unimplemented!()
        }

        fn output(&mut self) -> Result<Datum> {
            unimplemented!()
        }

        fn reset(&mut self) {
            unimplemented!()
        }

        fn get_state(&self) -> Datum {
            unimplemented!()
        }

        fn set_state(&mut self, _: Datum) {
            unimplemented!()
        }

        fn estimated_size(&self) -> usize {
            std::mem::size_of::<AtomicUsize>()
        }
    }

    #[tokio::test]
    async fn test_selective_agg_always_true() -> Result<()> {
        let condition = LiteralExpression::new(DataType::Boolean, Some(true.into())).boxed();
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            condition.into(),
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&chunk, 0..1).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_range(&chunk, 2..4).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 3);

        agg.update(&chunk).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg() -> Result<()> {
        let expr = build_from_pretty("(greater_than:boolean $0:int8 5:int8)");
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            expr.into(),
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&chunk, 0..1).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_range(&chunk, 1..2).await?; // should be filtered out
        assert_eq!(agg_count.load(Ordering::Relaxed), 1);

        agg.update_range(&chunk, 2..4).await?; // only 6 should be applied
        assert_eq!(agg_count.load(Ordering::Relaxed), 2);

        agg.update(&chunk).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg_null_condition() -> Result<()> {
        let expr = build_from_pretty("(equal:boolean $0:int8 null:int8)");
        let agg_count = Arc::new(AtomicUsize::new(0));
        let mut agg = Filter::new(
            expr.into(),
            Box::new(MockAgg {
                count: agg_count.clone(),
            }),
        );

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.update_range(&chunk, 0..1).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        agg.update_range(&chunk, 2..4).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        agg.update(&chunk).await?;
        assert_eq!(agg_count.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
