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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::ShutdownToken;

/// `SortAggExecutor` implements the sort aggregate algorithm, which assumes
/// that the input chunks has already been sorted by group columns.
/// The aggregation will be applied to tuples within the same group.
/// And the output schema is `[group columns, agg result]`.
///
/// As a special case, simple aggregate without groups satisfies the requirement
/// automatically because all tuples should be aggregated together.
pub struct SortAggExecutor {
    aggs: Vec<BoxedAggregateFunction>,
    group_key: Vec<BoxedExpression>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
    output_size_limit: usize, // make unit test easy
    shutdown_rx: ShutdownToken,
}

impl BoxedExecutorBuilder for SortAggExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let sort_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::SortAgg
        )?;

        let aggs: Vec<_> = sort_agg_node
            .get_agg_calls()
            .iter()
            .map(|agg| AggCall::from_protobuf(agg).and_then(|agg| build_agg(&agg)))
            .try_collect()?;

        let group_key: Vec<_> = sort_agg_node
            .get_group_key()
            .iter()
            .map(build_from_prost)
            .try_collect()?;

        let fields = group_key
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            aggs,
            group_key,
            child,
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
            output_size_limit: source.context().get_config().developer.chunk_size,
            shutdown_rx: source.shutdown_rx().clone(),
        }))
    }
}

impl Executor for SortAggExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl SortAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut left_capacity = self.output_size_limit;
        let mut agg_states: Vec<_> = self
            .aggs
            .iter()
            .map(|agg| agg.create_state())
            .try_collect()?;
        let (mut group_builders, mut agg_builders) =
            Self::create_builders(&self.group_key, &self.aggs);
        let mut curr_group = if self.group_key.is_empty() {
            Some(Vec::new())
        } else {
            None
        };

        #[for_await]
        for child_chunk in self.child.execute() {
            let child_chunk = StreamChunk::from(child_chunk?.compact());
            let mut group_columns = Vec::with_capacity(self.group_key.len());
            for expr in &mut self.group_key {
                self.shutdown_rx.check()?;
                let result = expr.eval(&child_chunk).await?;
                group_columns.push(result);
            }

            let groups = if group_columns.is_empty() {
                EqGroups::single_with_len(child_chunk.cardinality())
            } else {
                let groups: Vec<_> = group_columns
                    .iter()
                    .map(|col| EqGroups::detect(col))
                    .try_collect()?;
                EqGroups::intersect(&groups)
            };

            for range in groups.ranges() {
                self.shutdown_rx.check()?;
                let group: Vec<_> = group_columns
                    .iter()
                    .map(|col| col.datum_at(range.start))
                    .collect();

                if curr_group.as_ref() != Some(&group)
                    && let Some(group) = curr_group.replace(group)
                {
                    group_builders
                        .iter_mut()
                        .zip_eq_fast(group.into_iter())
                        .for_each(|(builder, datum)| {
                            builder.append(datum);
                        });
                    Self::output_agg_states(&self.aggs, &mut agg_states, &mut agg_builders).await?;
                    left_capacity -= 1;

                    if left_capacity == 0 {
                        let output = DataChunk::new(
                            group_builders
                                .into_iter()
                                .chain(agg_builders)
                                .map(|b| b.finish().into())
                                .collect(),
                            self.output_size_limit,
                        );
                        yield output;

                        (group_builders, agg_builders) =
                            Self::create_builders(&self.group_key, &self.aggs);
                        left_capacity = self.output_size_limit;
                    }
                }

                Self::update_agg_states(&self.aggs, &mut agg_states, &child_chunk, range).await?;
            }
        }

        if let Some(group) = curr_group.take() {
            group_builders
                .iter_mut()
                .zip_eq_fast(group.into_iter())
                .for_each(|(builder, datum)| {
                    builder.append(datum);
                });
            Self::output_agg_states(&self.aggs, &mut agg_states, &mut agg_builders).await?;
            left_capacity -= 1;

            let output = DataChunk::new(
                group_builders
                    .into_iter()
                    .chain(agg_builders)
                    .map(|b| b.finish().into())
                    .collect(),
                self.output_size_limit - left_capacity,
            );
            yield output;
        }
    }

    async fn update_agg_states(
        aggs: &[BoxedAggregateFunction],
        agg_states: &mut [AggregateState],
        child_chunk: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        for (agg, state) in aggs.iter().zip_eq_fast(agg_states.iter_mut()) {
            agg.update_range(state, child_chunk, range.clone()).await?;
        }
        Ok(())
    }

    async fn output_agg_states(
        aggs: &[BoxedAggregateFunction],
        agg_states: &mut [AggregateState],
        agg_builders: &mut [ArrayBuilderImpl],
    ) -> Result<()> {
        for ((agg, state), builder) in aggs
            .iter()
            .zip_eq_fast(agg_states.iter_mut())
            .zip_eq_fast(agg_builders)
        {
            let result = agg.get_result(state).await?;
            builder.append(result);
            *state = agg.create_state()?;
        }
        Ok(())
    }

    fn create_builders(
        group_key: &[BoxedExpression],
        aggs: &[BoxedAggregateFunction],
    ) -> (Vec<ArrayBuilderImpl>, Vec<ArrayBuilderImpl>) {
        let group_builders = group_key
            .iter()
            .map(|e| e.return_type().create_array_builder(1))
            .collect();

        let agg_builders = aggs
            .iter()
            .map(|e| e.return_type().create_array_builder(1))
            .collect();

        (group_builders, agg_builders)
    }
}

#[derive(Default, Debug)]
struct EqGroups {
    /// `[0, I1, ..., In, Len]` -> `[[0, I1), [I1, I2), ..., [In, Len)]`
    /// `[0]` -> `[]`
    indices: Vec<usize>,
}

impl EqGroups {
    fn new(indices: Vec<usize>) -> Self {
        EqGroups { indices }
    }

    fn single_with_len(len: usize) -> Self {
        EqGroups {
            indices: vec![0, len],
        }
    }

    fn ranges(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        EqGroupsIter {
            indices: &self.indices,
            curr: 0,
        }
    }

    /// Detect the equality groups in the given array.
    fn detect(array: &ArrayImpl) -> Result<EqGroups> {
        dispatch_array_variants!(array, array, { Ok(Self::detect_inner(array)) })
    }

    fn detect_inner<T>(array: &T) -> EqGroups
    where
        T: Array,
        for<'a> T::RefItem<'a>: Eq,
    {
        let mut indices = vec![0];
        if array.is_empty() {
            return EqGroups { indices };
        }
        let mut curr_group = array.value_at(0);
        for i in 1..array.len() {
            let v = array.value_at(i);
            if v == curr_group {
                continue;
            }
            curr_group = v;
            indices.push(i);
        }
        indices.push(array.len());
        EqGroups::new(indices)
    }

    /// `intersect` combines the grouping information from each column into a single one.
    /// This is required so that we know `group by c1, c2` with `c1 = [a, a, c, c, d, d]`
    /// and `c2 = [g, h, h, h, h, h]` actually forms 4 groups: `[(a, g), (a, h), (c, h), (d, h)]`.
    ///
    /// Since the internal encoding is a sequence of sorted indices, this is effectively
    /// merging all sequences into a single one with deduplication. In the example above,
    /// the `EqGroups` of `c1` is `[2, 4]` and that of `c2` is `[1]`, so the output of
    /// `intersect` would be `[1, 2, 4]` identifying the new groups starting at these indices.
    fn intersect(columns: &[EqGroups]) -> EqGroups {
        let mut indices = Vec::new();
        // Use of BinaryHeap here is not to get a performant implementation but a
        // concise one. The number of group columns would not be huge.
        // Storing iterator rather than (ci, idx) in heap actually makes the implementation
        // more verbose:
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=1e3b098ee3ef352d5a0cac03b3193799
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap = BinaryHeap::new();
        for (ci, column) in columns.iter().enumerate() {
            if let Some(ri) = column.indices.first() {
                heap.push(Reverse((ri, ci, 0)));
            }
        }
        while let Some(Reverse((ri, ci, idx))) = heap.pop() {
            if let Some(ri_next) = columns[ci].indices.get(idx + 1) {
                heap.push(Reverse((ri_next, ci, idx + 1)));
            }
            if indices.last() == Some(ri) {
                continue;
            }
            indices.push(*ri);
        }
        EqGroups::new(indices)
    }
}

struct EqGroupsIter<'a> {
    indices: &'a [usize],
    curr: usize,
}

impl Iterator for EqGroupsIter<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr + 1 >= self.indices.len() {
            return None;
        }
        let ret = self.indices[self.curr]..self.indices[self.curr + 1];
        self.curr += 1;
        Some(ret)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use futures_async_stream::for_await;
    use risingwave_common::array::{Array as _, I64Array};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_pretty;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn execute_count_star_int32() -> Result<()> {
        // mock a child executor
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut child = MockExecutor::new(schema);
        child.add(DataChunk::from_pretty(
            "i i i
             1 1 7
             2 1 8
             3 3 8
             4 3 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 3 9
             2 4 9
             3 4 9
             4 5 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 5 9
             2 5 9
             3 5 9
             4 5 9",
        ));

        let count_star = build_agg(&AggCall::from_pretty("(count:int8)"))?;
        let group_exprs: Vec<BoxedExpression> = vec![];
        let aggs = vec![count_star];

        // chain group key fields and agg state schema to get output schema for sort agg
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: group_exprs,
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit: 3,
            shutdown_rx: ShutdownToken::empty(),
        });

        let fields = &executor.schema().fields;
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type, DataType::Int64);

        let mut stream = executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));
        assert_matches!(stream.next().await, None);

        let chunk = res?;
        assert_eq!(chunk.cardinality(), 1);
        let actual = chunk.column_at(0);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();

        // check the result
        assert_eq!(v, vec![Some(12)]);
        Ok(())
    }

    #[tokio::test]
    async fn execute_count_star_int32_grouped() -> Result<()> {
        // mock a child executor
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut child = MockExecutor::new(schema);
        child.add(DataChunk::from_pretty(
            "i i i
             1 1 7
             2 1 8
             3 3 8
             4 3 9
             5 4 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 4 9
             2 4 9
             3 4 9
             4 5 9
             5 6 9
             6 7 9
             7 7 9
             8 8 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 8 9
             2 8 9
             3 8 9
             4 8 9
             5 8 9",
        ));

        let count_star = build_agg(&AggCall::from_pretty("(count:int8)"))?;
        let group_exprs: Vec<_> = (1..=2)
            .map(|idx| build_from_pretty(format!("${idx}:int4")))
            .collect();

        let aggs = vec![count_star];

        // chain group key fields and agg state schema to get output schema for sort agg
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: group_exprs,
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit: 3,
            shutdown_rx: ShutdownToken::empty(),
        });

        let fields = &executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        assert_eq!(fields[2].data_type, DataType::Int64);

        let mut stream = executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        assert_eq!(chunk.cardinality(), 3);
        let actual = chunk.column_at(2);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();

        // check the result
        assert_eq!(v, vec![Some(1), Some(1), Some(1)]);
        check_group_key_column(&chunk, 0, vec![Some(1), Some(1), Some(3)]);
        check_group_key_column(&chunk, 1, vec![Some(7), Some(8), Some(8)]);

        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        assert_eq!(chunk.cardinality(), 3);
        let actual = chunk.column_at(2);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();

        assert_eq!(v, vec![Some(1), Some(4), Some(1)]);
        check_group_key_column(&chunk, 0, vec![Some(3), Some(4), Some(5)]);
        check_group_key_column(&chunk, 1, vec![Some(9), Some(9), Some(9)]);

        // check the result
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        assert_eq!(chunk.cardinality(), 3);
        let actual = chunk.column_at(2);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();

        // check the result
        assert_eq!(v, vec![Some(1), Some(2), Some(6)]);
        check_group_key_column(&chunk, 0, vec![Some(6), Some(7), Some(8)]);
        check_group_key_column(&chunk, 1, vec![Some(9), Some(9), Some(9)]);

        assert_matches!(stream.next().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn execute_sum_int32() -> Result<()> {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let mut child = MockExecutor::new(schema);
        child.add(DataChunk::from_pretty(
            " i
              1
              2
              3
              4
              5
              6
              7
              8
              9
             10",
        ));

        let sum_agg = build_agg(&AggCall::from_pretty("(sum:int8 $0:int4)"))?;

        let group_exprs: Vec<BoxedExpression> = vec![];
        let aggs = vec![sum_agg];
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();
        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: vec![],
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit: 4,
            shutdown_rx: ShutdownToken::empty(),
        });

        let mut stream = executor.execute();
        let chunk = stream.next().await.unwrap()?;
        assert_matches!(stream.next().await, None);

        let actual = chunk.column_at(0);
        let actual: &I64Array = actual.as_ref().into();
        let v = actual.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(55)]);

        assert_matches!(stream.next().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn execute_sum_int32_grouped() -> Result<()> {
        // mock a child executor
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut child = MockExecutor::new(schema);
        child.add(DataChunk::from_pretty(
            "i i i
             1 1 7
             2 1 8
             3 3 8
             4 3 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 3 9
             2 4 9
             3 4 9
             4 5 9",
        ));
        child.add(DataChunk::from_pretty(
            "i i i
             1 5 9
             2 5 9
             3 5 9
             4 5 9",
        ));

        let sum_agg = build_agg(&AggCall::from_pretty("(sum:int8 $0:int4)"))?;
        let group_exprs: Vec<_> = (1..=2)
            .map(|idx| build_from_pretty(format!("${idx}:int4")))
            .collect();

        let aggs = vec![sum_agg];

        // chain group key fields and agg state schema to get output schema for sort agg
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let output_size_limit = 4;
        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: group_exprs,
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit,
            shutdown_rx: ShutdownToken::empty(),
        });

        let fields = &executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        assert_eq!(fields[2].data_type, DataType::Int64);

        let mut stream = executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        let actual = chunk.column_at(2);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();

        // check the result
        assert_eq!(v, vec![Some(1), Some(2), Some(3), Some(5)]);
        check_group_key_column(&chunk, 0, vec![Some(1), Some(1), Some(3), Some(3)]);
        check_group_key_column(&chunk, 1, vec![Some(7), Some(8), Some(8), Some(9)]);

        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        let actual2 = chunk.column_at(2);
        let actual_agg2: &I64Array = actual2.as_ref().into();
        let v = actual_agg2.iter().collect::<Vec<Option<i64>>>();

        // check the result
        assert_eq!(v, vec![Some(5), Some(14)]);
        check_group_key_column(&chunk, 0, vec![Some(4), Some(5)]);
        check_group_key_column(&chunk, 1, vec![Some(9), Some(9)]);

        assert_matches!(stream.next().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn execute_sum_int32_grouped_exceed_limit() -> Result<()> {
        // mock a child executor
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut child = MockExecutor::new(schema);
        child.add(DataChunk::from_pretty(
            " i  i  i
              1  1  7
              2  1  8
              3  3  8
              4  3  8
              5  4  9
              6  4  9
              7  5  9
              8  5  9
              9  6 10
             10  6 10",
        ));
        child.add(DataChunk::from_pretty(
            " i  i  i
              1  6 10
              2  7 12",
        ));

        let sum_agg = build_agg(&AggCall::from_pretty("(sum:int8 $0:int4)"))?;
        let group_exprs: Vec<_> = (1..=2)
            .map(|idx| build_from_pretty(format!("${idx}:int4")))
            .collect();

        let aggs = vec![sum_agg];

        // chain group key fields and agg state schema to get output schema for sort agg
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: group_exprs,
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit: 3,
            shutdown_rx: ShutdownToken::empty(),
        });

        let fields = &executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        assert_eq!(fields[2].data_type, DataType::Int64);

        // check first chunk
        let mut stream = executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        let actual = chunk.column_at(2);
        let actual_agg: &I64Array = actual.as_ref().into();
        let v = actual_agg.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(1), Some(2), Some(7)]);
        check_group_key_column(&chunk, 0, vec![Some(1), Some(1), Some(3)]);
        check_group_key_column(&chunk, 1, vec![Some(7), Some(8), Some(8)]);

        // check second chunk
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        let actual2 = chunk.column_at(2);
        let actual_agg2: &I64Array = actual2.as_ref().into();
        let v = actual_agg2.iter().collect::<Vec<Option<i64>>>();
        assert_eq!(v, vec![Some(11), Some(15), Some(20)]);
        check_group_key_column(&chunk, 0, vec![Some(4), Some(5), Some(6)]);
        check_group_key_column(&chunk, 1, vec![Some(9), Some(9), Some(10)]);

        // check third chunk
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));

        let chunk = res?;
        let actual2 = chunk.column_at(2);
        let actual_agg2: &I64Array = actual2.as_ref().into();
        let v = actual_agg2.iter().collect::<Vec<Option<i64>>>();

        assert_eq!(v, vec![Some(2)]);
        check_group_key_column(&chunk, 0, vec![Some(7)]);
        check_group_key_column(&chunk, 1, vec![Some(12)]);

        assert_matches!(stream.next().await, None);
        Ok(())
    }

    fn check_group_key_column(actual: &DataChunk, col_idx: usize, expect: Vec<Option<i32>>) {
        assert_eq!(
            actual
                .column_at(col_idx)
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            expect
        );
    }

    #[tokio::test]
    async fn test_shutdown_rx() -> Result<()> {
        let child = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i
                 4",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32)]),
        );

        let sum_agg = build_agg(&AggCall::from_pretty("(sum:int8 $0:int4)"))?;
        let group_exprs: Vec<_> = (1..=2)
            .map(|idx| build_from_pretty(format!("${idx}:int4")))
            .collect();

        let aggs = vec![sum_agg];

        // chain group key fields and agg state schema to get output schema for sort agg
        let fields = group_exprs
            .iter()
            .map(|e| e.return_type())
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let output_size_limit = 4;
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let executor = Box::new(SortAggExecutor {
            aggs,
            group_key: group_exprs,
            child: Box::new(child),
            schema: Schema { fields },
            identity: "SortAggExecutor".to_owned(),
            output_size_limit,
            shutdown_rx,
        });
        shutdown_tx.cancel();
        #[for_await]
        for data in executor.execute() {
            assert!(data.is_err());
            break;
        }

        Ok(())
    }
}
