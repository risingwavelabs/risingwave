// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::{repeat_n, Itertools};
use risingwave_common::array::column::Column;
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{Array, DataChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_expr::expr::{
    build_from_prost as expr_build_from_prost, BoxedExpression, Expression,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::join::JoinType;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Nested loop join executor.
///
///
/// High Level Idea:
/// 1. Iterate tuple from left child.
/// 2. Concatenated with right chunk, eval expression and get visibility bitmap
/// 3. Create new chunk with visibility bitmap and yield to upper.
pub struct NestedLoopJoinExecutor {
    /// Expression to eval join condition
    join_expr: BoxedExpression,
    /// Executor should handle different join type.
    join_type: JoinType,
    /// Original output schema
    original_schema: Schema,
    /// Actual output schema
    schema: Schema,
    /// We may only need certain columns.
    /// output_indices are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Left child executor
    left_child: BoxedExecutor,
    /// Right child executor
    right_child: BoxedExecutor,
    /// Identity string of the executor
    identity: String,
}

impl Executor for NestedLoopJoinExecutor {
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

impl NestedLoopJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let left_data_types = self.left_child.schema().data_types();
        let data_types = self.original_schema.data_types();

        let mut chunk_builder = DataChunkBuilder::with_default_size(data_types);

        // Cache the outputs of left child
        let left = self.left_child.execute().try_collect().await?;

        // Get the joined stream
        let stream = match self.join_type {
            JoinType::Inner => Self::do_inner_join,
            JoinType::LeftOuter => Self::do_left_outer_join,
            JoinType::LeftSemi => Self::do_left_semi_anti_join::<false>,
            JoinType::LeftAnti => Self::do_left_semi_anti_join::<true>,
            JoinType::RightOuter => Self::do_right_outer_join,
            JoinType::RightSemi => Self::do_right_semi_anti_join::<false>,
            JoinType::RightAnti => Self::do_right_semi_anti_join::<true>,
            JoinType::FullOuter => todo!(),
        };

        #[for_await]
        for chunk in stream(
            &mut chunk_builder,
            left_data_types,
            self.join_expr,
            left,
            self.right_child,
        ) {
            yield chunk?.reorder_columns(&self.output_indices)
        }

        // Handle remaining chunk
        if let Some(chunk) = chunk_builder.consume_all()? {
            yield chunk.reorder_columns(&self.output_indices)
        }
    }
}

impl NestedLoopJoinExecutor {
    /// Create constant data chunk (one tuple repeat `num_tuples` times).
    fn convert_datum_refs_to_chunk(
        datum_refs: &[DatumRef<'_>],
        num_tuples: usize,
        data_types: &[DataType],
    ) -> Result<DataChunk> {
        let mut output_array_builders: Vec<_> = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(num_tuples))
            .collect();
        for _i in 0..num_tuples {
            for (builder, datum_ref) in output_array_builders.iter_mut().zip_eq(datum_refs) {
                builder.append_datum_ref(*datum_ref)?;
            }
        }

        // Finish each array builder and get Column.
        let result_columns = output_array_builders
            .into_iter()
            .map(|builder| builder.finish().map(|arr| Column::new(Arc::new(arr))))
            .try_collect()?;

        Ok(DataChunk::new(result_columns, num_tuples))
    }

    /// Create constant data chunk (one tuple repeat `num_tuples` times).
    fn convert_row_to_chunk(
        row_ref: &RowRef<'_>,
        num_tuples: usize,
        data_types: &[DataType],
    ) -> Result<DataChunk> {
        let datum_refs = row_ref.values().collect_vec();
        Self::convert_datum_refs_to_chunk(&datum_refs, num_tuples, data_types)
    }

    /// The layout be like:
    ///
    /// [ `left` chunk     |  `right` chunk     ]
    ///
    /// # Arguments
    ///
    /// * `left` Data chunk padded to the left half of result data chunk..
    /// * `right` Data chunk padded to the right half of result data chunk.
    ///
    /// Note: Use this function with careful: It is not designed to be a general concatenate of two
    /// chunk: Usually one side should be const row chunk and the other side is normal chunk.
    /// Currently only feasible to use in join executor.
    /// If two normal chunk, the result is undefined.
    fn concatenate(left: &DataChunk, right: &DataChunk) -> DataChunk {
        assert_eq!(left.capacity(), right.capacity());

        let mut concated_columns = Vec::with_capacity(left.columns().len() + right.columns().len());
        concated_columns.extend_from_slice(left.columns());
        concated_columns.extend_from_slice(right.columns());

        DataChunk::new(concated_columns, left.capacity())
    }

    /// Create a chunk by concatenating a row with a chunk and set its visibility according to the
    /// evaluation result of the expression.
    fn concatenate_and_eval(
        expr: &dyn Expression,
        left_row_types: &[DataType],
        left_row: RowRef,
        right_chunk: &DataChunk,
    ) -> Result<DataChunk> {
        let left_chunk =
            Self::convert_row_to_chunk(&left_row, right_chunk.capacity(), left_row_types)?;
        let mut chunk = Self::concatenate(&left_chunk, right_chunk);
        chunk.set_visibility(expr.eval(&chunk)?.as_bool().iter().collect());
        Ok(chunk)
    }

    /// Append a chunk to the chunk builder and get a stream of the spilled chunks.
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn append_chunk(chunk_builder: &mut DataChunkBuilder, chunk: DataChunk) {
        let (mut remain, mut output) =
            chunk_builder.append_chunk(SlicedDataChunk::new_checked(chunk)?)?;
        if let Some(output_chunk) = output {
            yield output_chunk
        }
        while let Some(remain_chunk) = remain {
            (remain, output) = chunk_builder.append_chunk(remain_chunk)?;
            if let Some(output_chunk) = output {
                yield output_chunk
            }
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 2,
            "NestedLoopJoinExecutor should have 2 children!"
        );

        let nested_loop_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::NestedLoopJoin
        )?;

        let join_type = JoinType::from_prost(nested_loop_join_node.get_join_type()?);
        let join_expr = expr_build_from_prost(nested_loop_join_node.get_join_cond()?)?;

        let left_child = inputs.remove(0);
        let right_child = inputs.remove(0);

        let output_indices = nested_loop_join_node
            .output_indices
            .iter()
            .map(|&v| v as usize)
            .collect();

        Ok(Box::new(NestedLoopJoinExecutor::new(
            join_expr,
            join_type,
            output_indices,
            left_child,
            right_child,
            "NestedLoopExecutor".into(),
        )))
    }
}

impl NestedLoopJoinExecutor {
    pub fn new(
        join_expr: BoxedExpression,
        join_type: JoinType,
        output_indices: Vec<usize>,
        left_child: BoxedExecutor,
        right_child: BoxedExecutor,
        identity: String,
    ) -> Self {
        // TODO(Bowen): Merge this with derive schema in Logical Join (#790).
        let original_schema = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => left_child.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => right_child.schema().clone(),
            _ => Schema::from_iter(
                left_child
                    .schema()
                    .fields()
                    .iter()
                    .chain(right_child.schema().fields().iter())
                    .cloned(),
            ),
        };
        let schema = Schema::from_iter(
            output_indices
                .iter()
                .map(|&idx| original_schema[idx].clone()),
        );
        Self {
            join_expr,
            join_type,
            original_schema,
            schema,
            output_indices,
            left_child,
            right_child,
            identity,
        }
    }
}

impl NestedLoopJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_inner_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    #[for_await]
                    for spilled in Self::append_chunk(chunk_builder, chunk) {
                        yield spilled?
                    }
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        let mut matched = BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        let right_data_types = right.schema().data_types();
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    matched.set(left_row_idx, true);
                    #[for_await]
                    for spilled in Self::append_chunk(chunk_builder, chunk) {
                        yield spilled?
                    }
                }
            }
        }
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq(matched.finish().iter())
            .filter(|(_, matched)| !*matched)
        {
            let datum_refs = left_row
                .values()
                .chain(repeat_n(None, right_data_types.len()));
            if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs)? {
                yield chunk
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_semi_anti_join<const ANTI_JOIN: bool>(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        let mut matched = BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    matched.set(left_row_idx, true)
                }
            }
        }
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq(matched.finish().iter())
            .filter(|(_, matched)| if ANTI_JOIN { !*matched } else { *matched })
        {
            if let Some(chunk) = chunk_builder.append_one_row_ref(left_row)? {
                yield chunk
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            let mut matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    // chunk.visibility() must be Some(_)
                    matched = &matched | chunk.visibility().unwrap();
                    #[for_await]
                    for spilled in Self::append_chunk(chunk_builder, chunk) {
                        yield spilled?
                    }
                }
            }
            for (right_row, _) in right_chunk
                .rows()
                .zip_eq(matched.iter())
                .filter(|(_, matched)| !*matched)
            {
                let datum_refs = repeat_n(None, left_data_types.len()).chain(right_row.values());
                if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs)? {
                    yield chunk
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_semi_anti_join<const ANTI_JOIN: bool>(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        #[for_await]
        for right_chunk in right.execute() {
            let mut right_chunk = right_chunk?;
            let mut matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    // chunk.visibility() must be Some(_)
                    matched = &matched | chunk.visibility().unwrap();
                }
            }
            if ANTI_JOIN {
                matched = !&matched;
            }
            right_chunk.set_visibility(matched);
            if right_chunk.cardinality() > 0 {
                #[for_await]
                for spilled in Self::append_chunk(chunk_builder, right_chunk) {
                    yield spilled?
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarRefImpl};
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::InputRefExpression;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;

    /// Test combine two chunk into one.
    #[test]
    fn test_concatenate() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length);
            for _ in 0..length {
                builder.append(Some(i as i32)).unwrap();
            }
            let arr = builder.finish().unwrap();
            columns.push(Column::new(Arc::new(arr.into())))
        }
        let chunk1: DataChunk = DataChunk::new(columns.clone(), length);
        let chunk2: DataChunk = DataChunk::new(columns.clone(), length);
        let chunk = NestedLoopJoinExecutor::concatenate(&chunk1, &chunk2);
        assert_eq!(chunk.capacity(), chunk1.capacity());
        assert_eq!(chunk.capacity(), chunk2.capacity());
        assert_eq!(chunk.columns().len(), chunk1.columns().len() * 2);
        assert_eq!(chunk.cardinality(), length);
        assert_eq!(chunk.visibility(), None);
    }

    /// Test the function of convert row into constant row chunk (one row repeat multiple times).
    #[test]
    fn test_convert_row_to_chunk() {
        let row = vec![Some(ScalarRefImpl::Int32(3))];
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let const_row_chunk =
            NestedLoopJoinExecutor::convert_datum_refs_to_chunk(&row, 5, &schema.data_types())
                .unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).unwrap().0.value_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }

    struct TestFixture {
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, 8.4::FLOAT), (3, 3.9::FLOAT), (3, 6.6::FLOAT), (4, 0.7::FLOAT),
    /// (6, 5.5::FLOAT), (6, 5.6::FLOAT), (8, 7.0::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (2, 6.1::REAL), (3, 8.9::REAL), (6, 3.4::REAL), (8, 3.5::REAL), (9, 7.5::REAL),
    /// (10, null), (11, 8::REAL), (12, null), (20, 5.7::REAL), (30, 9.6::REAL),
    /// (100, null), (200, 8.18::REAL);
    /// ```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![DataType::Int32, DataType::Float32],
                right_types: vec![DataType::Int32, DataType::Float64],
                join_type,
            }
        }

        fn create_left_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float32),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i f
                 1 6.1
                 2 8.4
                 3 3.9",
            ));

            executor.add(DataChunk::from_pretty(
                "i f
                 3 6.6
                 4 0.7
                 6 5.5
                 6 5.6
                 8 7.0",
            ));

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float64),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i F
                 2 6.1
                 3 8.9
                 6 3.4
                 8 3.5",
            ));

            executor.add(DataChunk::from_pretty(
                " i F
                  9 7.5
                 10 .
                 11 8
                 12 .",
            ));

            executor.add(DataChunk::from_pretty(
                "  i F
                  20 5.7
                  30 9.6
                 100 .
                 200 8.18",
            ));

            Box::new(executor)
        }

        fn create_join_executor(&self) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let output_indices = match self.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => vec![0, 1],
                JoinType::RightSemi | JoinType::RightAnti => vec![0, 1],
                _ => vec![0, 1, 2, 3],
            };

            Box::new(NestedLoopJoinExecutor::new(
                new_binary_expr(
                    Type::Equal,
                    DataType::Boolean,
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(InputRefExpression::new(DataType::Int32, 2)),
                ),
                join_type,
                output_indices,
                left_child,
                right_child,
                "NestedLoopJoinExecutor".into(),
            ))
        }

        async fn do_test(&self, expected: DataChunk) {
            let join_executor = self.create_join_executor();
            let mut expected_mock_exec = MockExecutor::new(join_executor.schema().clone());
            expected_mock_exec.add(expected);
            diff_executor_output(join_executor, Box::new(expected_mock_exec)).await;
        }
    }

    /// sql: select * from t1, t2 where t1.v1 = t2.v1
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i f   i F
             2 8.4 2 6.1
             3 3.9 3 8.9
             3 6.6 3 8.9
             6 5.5 6 3.4
             6 5.6 6 3.4
             8 7.0 8 3.5",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    /// sql: select * from t1 left outer join t2 on t1.v1 = t2.v1
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i f   i F
             2 8.4 2 6.1
             3 3.9 3 8.9
             3 6.6 3 8.9
             6 5.5 6 3.4
             6 5.6 6 3.4
             8 7.0 8 3.5
             1 6.1 . .
             4 0.7 . .",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i f
             2 8.4
             3 3.9
             3 6.6
             6 5.5
             6 5.6
             8 7.0",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i f
             1 6.1
             4 0.7",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i f   i F
             2 8.4 2 6.1
             3 3.9 3 8.9
             3 6.6 3 8.9
             6 5.5 6 3.4
             6 5.6 6 3.4
             8 7.0 8 3.5
             . .   9 7.5
             . .   10 .
             . .   11 8
             . .   12 .
             . .   20 5.7
             . .   30 9.6
             . .   100 .
             . .   200 8.18",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i F
             2 6.1
             3 8.9
             6 3.4
             8 3.5",
        );

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "  i F
               9 7.5
              10 .
              11 8
              12 .
              20 5.7
              30 9.6
             100 .
             200 8.18",
        );

        test_fixture.do_test(expected_chunk).await;
    }
}
