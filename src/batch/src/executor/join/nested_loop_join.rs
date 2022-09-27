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

use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::{repeat_n, Itertools};
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{Array, DataChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::expr::{
    build_from_prost as expr_build_from_prost, BoxedExpression, Expression,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::join::{concatenate, convert_row_to_chunk, JoinType};
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
            JoinType::FullOuter => Self::do_full_outer_join,
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
        if let Some(chunk) = chunk_builder.consume_all() {
            yield chunk.reorder_columns(&self.output_indices)
        }
    }
}

impl NestedLoopJoinExecutor {
    /// Create a chunk by concatenating a row with a chunk and set its visibility according to the
    /// evaluation result of the expression.
    fn concatenate_and_eval(
        expr: &dyn Expression,
        left_row_types: &[DataType],
        left_row: RowRef<'_>,
        right_chunk: &DataChunk,
    ) -> Result<DataChunk> {
        let left_chunk = convert_row_to_chunk(&left_row, right_chunk.capacity(), left_row_types)?;
        let mut chunk = concatenate(&left_chunk, right_chunk)?;
        chunk.set_visibility(expr.eval(&chunk)?.as_bool().iter().collect());
        Ok(chunk)
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [left_child, right_child]: [_; 2] = inputs.try_into().unwrap();

        let nested_loop_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::NestedLoopJoin
        )?;

        let join_type = JoinType::from_prost(nested_loop_join_node.get_join_type()?);
        let join_expr = expr_build_from_prost(nested_loop_join_node.get_join_cond()?)?;

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
            source.plan_node().get_identity().clone(),
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
        // 1. Iterate over the right table by chunks.
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            // 2. Iterator over the left table by rows.
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                // 3. Concatenate the left row and right chunk into a single chunk and evaluate the
                // expression on it.
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                // 4. Yield the concatenated chunk.
                if chunk.cardinality() > 0 {
                    #[for_await]
                    for spilled in chunk_builder.trunc_data_chunk(chunk) {
                        yield spilled
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
        // Same as inner join except that a bitmap is used to track which row of the left table is
        // matched.
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
                    for spilled in chunk_builder.trunc_data_chunk(chunk) {
                        yield spilled
                    }
                }
            }
        }
        // Yield unmatched rows in the left table.
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq(matched.finish().iter())
            .filter(|(_, matched)| !*matched)
        {
            let datum_refs = left_row
                .values()
                .chain(repeat_n(None, right_data_types.len()));
            if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs) {
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
                if matched.is_set(left_row_idx) {
                    continue;
                }
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
            if let Some(chunk) = chunk_builder.append_one_row_ref(left_row) {
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
            // Use a bitmap to track which row of the current right chunk is matched.
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
                    for spilled in chunk_builder.trunc_data_chunk(chunk) {
                        yield spilled
                    }
                }
            }
            for (right_row, _) in right_chunk
                .rows()
                .zip_eq(matched.iter())
                .filter(|(_, matched)| !*matched)
            {
                let datum_refs = repeat_n(None, left_data_types.len()).chain(right_row.values());
                if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs) {
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
                for spilled in chunk_builder.trunc_data_chunk(right_chunk) {
                    yield spilled
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_full_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
    ) {
        let mut left_matched =
            BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        let right_data_types = right.schema().data_types();
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            let mut right_matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )?;
                if chunk.cardinality() > 0 {
                    left_matched.set(left_row_idx, true);
                    right_matched = &right_matched | chunk.visibility().unwrap();
                    #[for_await]
                    for spilled in chunk_builder.trunc_data_chunk(chunk) {
                        yield spilled
                    }
                }
            }
            // Yield unmatched rows in the right table
            for (right_row, _) in right_chunk
                .rows()
                .zip_eq(right_matched.iter())
                .filter(|(_, matched)| !*matched)
            {
                let datum_refs = repeat_n(None, left_data_types.len()).chain(right_row.values());
                if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs) {
                    yield chunk
                }
            }
        }
        // Yield unmatched rows in the left table.
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq(left_matched.finish().iter())
            .filter(|(_, matched)| !*matched)
        {
            let datum_refs = left_row
                .values()
                .chain(repeat_n(None, right_data_types.len()));
            if let Some(chunk) = chunk_builder.append_one_row_from_datum_refs(datum_refs) {
                yield chunk
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::InputRefExpression;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;

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

    #[tokio::test]
    async fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

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
             . .   200 8.18
             1 6.1 . .
             4 0.7 . .",
        );

        test_fixture.do_test(expected_chunk).await;
    }
}
