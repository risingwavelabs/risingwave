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

use futures_async_stream::try_stream;
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{Array, DataChunk};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::{repeat_n, RowExt};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::expr::{
    build_from_prost as expr_build_from_prost, BoxedExpression, Expression,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::join::{concatenate, convert_row_to_chunk, JoinType};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::ShutdownToken;

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
    /// `output_indices` are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Left child executor
    left_child: BoxedExecutor,
    /// Right child executor
    right_child: BoxedExecutor,
    /// Identity string of the executor
    identity: String,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// Memory context used for recording memory usage of executor.
    mem_context: MemoryContext,

    shutdown_rx: ShutdownToken,
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
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let left_data_types = self.left_child.schema().data_types();
        let data_types = self.original_schema.data_types();

        let mut chunk_builder = DataChunkBuilder::new(data_types, self.chunk_size);

        // Cache the outputs of left child
        let left: Vec<DataChunk> = {
            let mut ret = Vec::with_capacity(1024);
            #[for_await]
            for chunk in self.left_child.execute() {
                let c = chunk?;
                trace!("Estimated chunk size is {:?}", c.estimated_heap_size());
                if !self.mem_context.add(c.estimated_heap_size() as i64) {
                    Err(BatchError::OutOfMemory(self.mem_context.mem_limit()))?;
                }
                ret.push(c);
            }
            ret
        };

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
            JoinType::AsOfInner | JoinType::AsOfLeftOuter => {
                unimplemented!("AsOf join is not supported in NestedLoopJoinExecutor")
            }
        };

        #[for_await]
        for chunk in stream(
            &mut chunk_builder,
            left_data_types,
            self.join_expr,
            left,
            self.right_child,
            self.shutdown_rx.clone(),
        ) {
            yield chunk?.project(&self.output_indices)
        }

        // Handle remaining chunk
        if let Some(chunk) = chunk_builder.consume_all() {
            yield chunk.project(&self.output_indices)
        }
    }
}

impl NestedLoopJoinExecutor {
    /// Create a chunk by concatenating a row with a chunk and set its visibility according to the
    /// evaluation result of the expression.
    async fn concatenate_and_eval(
        expr: &dyn Expression,
        left_row_types: &[DataType],
        left_row: RowRef<'_>,
        right_chunk: &DataChunk,
    ) -> Result<DataChunk> {
        let left_chunk = convert_row_to_chunk(&left_row, right_chunk.capacity(), left_row_types)?;
        let mut chunk = concatenate(&left_chunk, right_chunk)?;
        chunk.set_visibility(expr.eval(&chunk).await?.as_bool().iter().collect());
        Ok(chunk)
    }
}

impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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

        let identity = source.plan_node().get_identity().clone();
        let mem_context = source.context().create_executor_mem_context(&identity);

        Ok(Box::new(NestedLoopJoinExecutor::new(
            join_expr,
            join_type,
            output_indices,
            left_child,
            right_child,
            identity,
            source.context().get_config().developer.chunk_size,
            mem_context,
            source.shutdown_rx().clone(),
        )))
    }
}

impl NestedLoopJoinExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        join_expr: BoxedExpression,
        join_type: JoinType,
        output_indices: Vec<usize>,
        left_child: BoxedExecutor,
        right_child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        shutdown_rx: ShutdownToken,
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
            chunk_size,
            mem_context,
            shutdown_rx,
        }
    }
}

impl NestedLoopJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_inner_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        // 1. Iterate over the right table by chunks.
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            // 2. Iterator over the left table by rows.
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                shutdown_rx.check()?;
                // 3. Concatenate the left row and right chunk into a single chunk and evaluate the
                // expression on it.
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                // 4. Yield the concatenated chunk.
                if chunk.cardinality() > 0 {
                    for spilled in chunk_builder.append_chunk(chunk) {
                        yield spilled
                    }
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_left_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        let mut matched = BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        let right_data_types = right.schema().data_types();
        // Same as inner join except that a bitmap is used to track which row of the left table is
        // matched.
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                shutdown_rx.check()?;
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                if chunk.cardinality() > 0 {
                    matched.set(left_row_idx, true);
                    for spilled in chunk_builder.append_chunk(chunk) {
                        yield spilled
                    }
                }
            }
        }
        // Yield unmatched rows in the left table.
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq_debug(matched.finish().iter())
            .filter(|(_, matched)| !*matched)
        {
            shutdown_rx.check()?;
            let row = left_row.chain(repeat_n(Datum::None, right_data_types.len()));
            if let Some(chunk) = chunk_builder.append_one_row(row) {
                yield chunk
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_left_semi_anti_join<const ANTI_JOIN: bool>(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        let mut matched = BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                shutdown_rx.check()?;
                if matched.is_set(left_row_idx) {
                    continue;
                }
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                if chunk.cardinality() > 0 {
                    matched.set(left_row_idx, true)
                }
            }
        }
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq_debug(matched.finish().iter())
            .filter(|(_, matched)| if ANTI_JOIN { !*matched } else { *matched })
        {
            shutdown_rx.check()?;
            if let Some(chunk) = chunk_builder.append_one_row(left_row) {
                yield chunk
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_right_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            // Use a bitmap to track which row of the current right chunk is matched.
            let mut matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                shutdown_rx.check()?;
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                if chunk.cardinality() > 0 {
                    // chunk.visibility() must be Some(_)
                    matched = &matched | chunk.visibility();
                    for spilled in chunk_builder.append_chunk(chunk) {
                        yield spilled
                    }
                }
            }
            for (right_row, _) in right_chunk
                .rows()
                .zip_eq_debug(matched.iter())
                .filter(|(_, matched)| !*matched)
            {
                shutdown_rx.check()?;
                let row = repeat_n(Datum::None, left_data_types.len()).chain(right_row);
                if let Some(chunk) = chunk_builder.append_one_row(row) {
                    yield chunk
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_right_semi_anti_join<const ANTI_JOIN: bool>(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        #[for_await]
        for right_chunk in right.execute() {
            let mut right_chunk = right_chunk?;
            let mut matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for left_row in left.iter().flat_map(|chunk| chunk.rows()) {
                shutdown_rx.check()?;
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                if chunk.cardinality() > 0 {
                    // chunk.visibility() must be Some(_)
                    matched = &matched | chunk.visibility();
                }
            }
            if ANTI_JOIN {
                matched = !&matched;
            }
            right_chunk.set_visibility(matched);
            if right_chunk.cardinality() > 0 {
                for spilled in chunk_builder.append_chunk(right_chunk) {
                    yield spilled
                }
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_full_outer_join(
        chunk_builder: &mut DataChunkBuilder,
        left_data_types: Vec<DataType>,
        join_expr: BoxedExpression,
        left: Vec<DataChunk>,
        right: BoxedExecutor,
        shutdown_rx: ShutdownToken,
    ) {
        let mut left_matched =
            BitmapBuilder::zeroed(left.iter().map(|chunk| chunk.capacity()).sum());
        let right_data_types = right.schema().data_types();
        #[for_await]
        for right_chunk in right.execute() {
            let right_chunk = right_chunk?;
            let mut right_matched = BitmapBuilder::zeroed(right_chunk.capacity()).finish();
            for (left_row_idx, left_row) in left.iter().flat_map(|chunk| chunk.rows()).enumerate() {
                shutdown_rx.check()?;
                let chunk = Self::concatenate_and_eval(
                    join_expr.as_ref(),
                    &left_data_types,
                    left_row,
                    &right_chunk,
                )
                .await?;
                if chunk.cardinality() > 0 {
                    left_matched.set(left_row_idx, true);
                    right_matched = &right_matched | chunk.visibility();
                    for spilled in chunk_builder.append_chunk(chunk) {
                        yield spilled
                    }
                }
            }
            // Yield unmatched rows in the right table
            for (right_row, _) in right_chunk
                .rows()
                .zip_eq_debug(right_matched.iter())
                .filter(|(_, matched)| !*matched)
            {
                shutdown_rx.check()?;
                let row = repeat_n(Datum::None, left_data_types.len()).chain(right_row);
                if let Some(chunk) = chunk_builder.append_one_row(row) {
                    yield chunk
                }
            }
        }
        // Yield unmatched rows in the left table.
        for (left_row, _) in left
            .iter()
            .flat_map(|chunk| chunk.rows())
            .zip_eq_debug(left_matched.finish().iter())
            .filter(|(_, matched)| !*matched)
        {
            shutdown_rx.check()?;
            let row = left_row.chain(repeat_n(Datum::None, right_data_types.len()));
            if let Some(chunk) = chunk_builder.append_one_row(row) {
                yield chunk
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::memory::MemoryContext;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_pretty;

    use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;
    use crate::task::ShutdownToken;

    const CHUNK_SIZE: usize = 1024;

    struct TestFixture {
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
            Self { join_type }
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

        fn create_join_executor(&self, shutdown_rx: ShutdownToken) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let output_indices = match self.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => vec![0, 1],
                JoinType::RightSemi | JoinType::RightAnti => vec![0, 1],
                _ => vec![0, 1, 2, 3],
            };

            Box::new(NestedLoopJoinExecutor::new(
                build_from_pretty("(equal:boolean $0:int4 $2:int4)"),
                join_type,
                output_indices,
                left_child,
                right_child,
                "NestedLoopJoinExecutor".into(),
                CHUNK_SIZE,
                MemoryContext::none(),
                shutdown_rx,
            ))
        }

        async fn do_test(&self, expected: DataChunk) {
            let join_executor = self.create_join_executor(ShutdownToken::empty());
            let mut expected_mock_exec = MockExecutor::new(join_executor.schema().clone());
            expected_mock_exec.add(expected);
            diff_executor_output(join_executor, Box::new(expected_mock_exec)).await;
        }

        async fn do_test_shutdown(&self) {
            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            let join_executor = self.create_join_executor(shutdown_rx);
            shutdown_tx.cancel();
            #[for_await]
            for chunk in join_executor.execute() {
                assert!(chunk.is_err());
                break;
            }

            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            let join_executor = self.create_join_executor(shutdown_rx);
            shutdown_tx.abort("test");
            #[for_await]
            for chunk in join_executor.execute() {
                assert!(chunk.is_err());
                break;
            }
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

    #[tokio::test]
    async fn test_shutdown_rx() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);
        test_fixture.do_test_shutdown().await;
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);
        test_fixture.do_test_shutdown().await;
    }
}
