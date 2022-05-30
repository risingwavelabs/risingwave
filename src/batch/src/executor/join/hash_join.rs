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

use std::convert::TryInto;
use std::marker::PhantomData;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::join::hash_join_state::{BuildTable, ProbeTable};
use crate::executor::join::JoinType;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::{BatchTaskContext, TaskId};

/// Parameters of equi-join.
///
/// We use following sql as an example in comments:
/// ```sql
/// select a.a1, a.a2, b.b1, b.b2 from a inner join b where a.a3 = b.b3 and a.a1 = b.b1
/// ```
#[derive(Default)]
pub(super) struct EquiJoinParams {
    join_type: JoinType,
    /// Column indexes of left keys in equi join, e.g., the column indexes of `b1` and `b3` in `b`.
    left_key_columns: Vec<usize>,
    /// Data types of left keys in equi join, e.g., the column types of `b1` and `b3` in `b`.
    left_key_types: Vec<DataType>,
    /// Data types of left columns in equi join, e.g., the column types of `b1` `b2` `b3` in `b`.
    left_col_len: usize,
    /// Column indexes of right keys in equi join, e.g., the column indexes of `a1` and `a3` in
    /// `a`.
    right_key_columns: Vec<usize>,
    /// Data types of right keys in equi join, e.g., the column types of `a1` and `a3` in `a`.
    right_key_types: Vec<DataType>,
    /// Data types of right columns in equi join, e.g., the column types of `a1` `a2` `a3` in `a`.
    right_col_len: usize,
    /// Column types of the concatenation of two input side, e.g. the column types of
    /// `a1`, `a2`, `a3`, `b1`, `b2`, `b3`.
    full_data_types: Vec<DataType>,
    /// Data chunk buffer size
    batch_size: usize,
    /// Non-equi condition
    pub cond: Option<BoxedExpression>,
}

pub(super) struct HashJoinExecutor<K> {
    /// Probe side
    left_child: Option<BoxedExecutor>,
    /// Build side
    right_child: Option<BoxedExecutor>,
    params: EquiJoinParams,
    schema: Schema,
    identity: String,
    _phantom: PhantomData<K>,
}

impl EquiJoinParams {
    #[inline(always)]
    pub(super) fn probe_key_columns(&self) -> &[usize] {
        &self.left_key_columns
    }

    #[inline(always)]
    pub(super) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[inline(always)]
    pub(super) fn full_data_types(&self) -> &[DataType] {
        &self.full_data_types
    }

    #[inline(always)]
    pub(super) fn left_len(&self) -> usize {
        self.left_col_len
    }

    #[inline(always)]
    pub(super) fn right_len(&self) -> usize {
        self.right_col_len
    }

    #[inline(always)]
    pub(super) fn build_key_columns(&self) -> &[usize] {
        &self.right_key_columns
    }

    #[inline(always)]
    pub(super) fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[inline(always)]
    pub(super) fn has_non_equi_cond(&self) -> bool {
        self.cond.is_some()
    }
}

impl<K: HashKey + Send + Sync> Executor for HashJoinExecutor<K> {
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

impl<K: HashKey + Send + Sync> HashJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut right_child_stream = self.right_child.take().unwrap().execute();
        let mut build_table = BuildTable::with_params(self.params);

        while let Some(chunk) = right_child_stream.next().await {
            let chunk = chunk?;
            build_table.append_build_chunk(chunk)?;
        }
        let mut probe_table: ProbeTable<K> = build_table.try_into()?;

        let mut state = HashJoinState::Probe;
        let mut left_child_stream = self.left_child.take().unwrap().execute();

        // first probe
        match left_child_stream.next().await {
            Some(data_chunk) => {
                let data_chunk = data_chunk?;
                probe_table.set_probe_data(data_chunk)?;
            }
            None => {
                state = HashJoinState::Done;
            }
        }
        // probe
        while state == HashJoinState::Probe {
            if let Some(ret_data_chunk) = probe_table.join()? {
                let data_chunk = if probe_table.has_non_equi_cond() {
                    probe_table.process_non_equi_condition(ret_data_chunk)?
                } else {
                    Some(ret_data_chunk)
                };

                // TODO(yuhao): Current we handle cut null columns in semi/anti join just
                // before returning chunks. We can furthur optimize this by cut columns earlier.
                let output_data_chunk =
                    data_chunk.map(|chunk| probe_table.remove_null_columns_for_semi_anti(chunk));

                probe_table.reset_result_index();

                if let Some(data_chunk) = output_data_chunk && data_chunk.cardinality() > 0 {
                    yield data_chunk;
                }
            } else {
                match left_child_stream.next().await {
                    Some(data_chunk) => {
                        let data_chunk = data_chunk?;
                        probe_table.set_probe_data(data_chunk)?;
                    }
                    None => {
                        // Consume the rest when when probe side end.
                        let ret_data_chunk = probe_table.consume_left()?;
                        let data_chunk = if probe_table.has_non_equi_cond() {
                            probe_table.process_non_equi_condition(ret_data_chunk)?
                        } else {
                            Some(ret_data_chunk)
                        };

                        let output_data_chunk = data_chunk
                            .map(|chunk| probe_table.remove_null_columns_for_semi_anti(chunk));

                        probe_table.reset_result_index();

                        if probe_table.join_type().need_join_remaining() {
                            state = HashJoinState::ProbeRemaining;
                        } else {
                            state = HashJoinState::Done;
                        }
                        if let Some(data_chunk) = output_data_chunk && data_chunk.cardinality() > 0 {
                            yield data_chunk;
                        }
                    }
                }
            }
        }
        // probe_remaining
        if !probe_table.build_data_empty() {
            while state == HashJoinState::ProbeRemaining {
                let output_data_chunk =
                    if let Some(ret_data_chunk) = probe_table.join_remaining()? {
                        let output_data_chunk =
                            probe_table.remove_null_columns_for_semi_anti(ret_data_chunk);

                        probe_table.reset_result_index();
                        output_data_chunk
                    } else {
                        let ret_data_chunk = probe_table.consume_left()?;
                        let output_data_chunk =
                            probe_table.remove_null_columns_for_semi_anti(ret_data_chunk);

                        state = HashJoinState::Done;
                        output_data_chunk
                    };
                yield output_data_chunk
            }
        }
    }
}

#[derive(PartialEq)]
pub enum HashJoinState {
    Probe,
    ProbeRemaining,
    Done,
}

impl<K> HashJoinExecutor<K> {
    fn new(
        left_child: BoxedExecutor,
        right_child: BoxedExecutor,
        params: EquiJoinParams,
        schema: Schema,
        identity: String,
    ) -> Self {
        HashJoinExecutor {
            left_child: Some(left_child),
            right_child: Some(right_child),
            params,
            schema,
            identity,
            _phantom: PhantomData,
        }
    }
}

pub struct HashJoinExecutorBuilder {
    params: EquiJoinParams,
    left_child: BoxedExecutor,
    right_child: BoxedExecutor,
    schema: Schema,
    task_id: TaskId,
}

struct HashJoinExecutorBuilderDispatcher;

/// A dispatcher to help create specialized hash join executor.
impl HashKeyDispatcher for HashJoinExecutorBuilderDispatcher {
    type Input = HashJoinExecutorBuilder;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: HashJoinExecutorBuilder) -> Self::Output {
        Box::new(HashJoinExecutor::<K>::new(
            input.left_child,
            input.right_child,
            input.params,
            input.schema,
            format!("HashJoinExecutor{:?}", input.task_id),
        ))
    }
}

/// Hash join executor builder.
#[async_trait::async_trait]
impl BoxedExecutorBuilder for HashJoinExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        context: &ExecutorBuilder<C>,
    ) -> Result<BoxedExecutor> {
        ensure!(context.plan_node().get_children().len() == 2);

        let left_child = context
            .clone_for_plan(&context.plan_node.get_children()[0])
            .build()
            .await?;
        let right_child = context
            .clone_for_plan(&context.plan_node.get_children()[1])
            .build()
            .await?;

        let hash_join_node = try_match_expand!(
            context.plan_node().get_node_body().unwrap(),
            NodeBody::HashJoin
        )?;

        let join_type = JoinType::from_prost(hash_join_node.get_join_type()?);

        let cond = match hash_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let full_schema_fields = [
            left_child.schema().fields.clone(),
            right_child.schema().fields.clone(),
        ]
        .concat();

        let schema_fields = if join_type.keep_all() {
            full_schema_fields.clone()
        } else if join_type.keep_left() {
            left_child.schema().fields.clone()
        } else if join_type.keep_right() {
            right_child.schema().fields.clone()
        } else {
            unreachable!()
        };

        let full_data_types = full_schema_fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect();

        let mut params = EquiJoinParams {
            join_type,
            left_col_len: left_child.schema().len(),
            right_col_len: right_child.schema().len(),
            full_data_types,
            batch_size: DEFAULT_CHUNK_BUFFER_SIZE,
            cond,
            ..Default::default()
        };

        for left_key in hash_join_node.get_left_key() {
            let left_key = *left_key as usize;
            params.left_key_columns.push(left_key);
            params
                .left_key_types
                .push(left_child.schema()[left_key].data_type());
        }

        for right_key in hash_join_node.get_right_key() {
            let right_key = *right_key as usize;
            params.right_key_columns.push(right_key);
            params
                .right_key_types
                .push(right_child.schema()[right_key].data_type());
        }

        ensure!(params.left_key_columns.len() == params.right_key_columns.len());

        let hash_key_kind = calc_hash_key_kind(&params.right_key_types);

        let builder = HashJoinExecutorBuilder {
            params,
            left_child,
            right_child,
            schema: Schema {
                fields: schema_fields,
            },
            task_id: context.task_id.clone(),
        };

        Ok(HashJoinExecutorBuilderDispatcher::dispatch_by_kind(
            hash_key_kind,
            builder,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::error::Result;
    use risingwave_common::hash::Key32;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression};
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::hash_join::{EquiJoinParams, HashJoinExecutor};
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::BoxedExecutor;
    struct DataChunkMerger {
        data_types: Vec<DataType>,
        array_builders: Vec<ArrayBuilderImpl>,
    }

    impl DataChunkMerger {
        fn new(data_types: Vec<DataType>) -> Result<Self> {
            let array_builders = data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(1024))
                .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

            Ok(Self {
                data_types,
                array_builders,
            })
        }

        fn append(&mut self, data_chunk: &DataChunk) -> Result<()> {
            ensure!(self.array_builders.len() == data_chunk.dimension());
            for idx in 0..self.array_builders.len() {
                self.array_builders[idx].append_array(data_chunk.column_at(idx).array_ref())?;
            }

            Ok(())
        }

        fn finish(self) -> Result<DataChunk> {
            let columns = self
                .array_builders
                .into_iter()
                .map(|array_builder| array_builder.finish().map(|arr| Column::new(Arc::new(arr))))
                .collect::<Result<Vec<Column>>>()?;

            Ok(DataChunk::zooja(columns))
        }
    }

    fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) -> bool {
        assert!(left.visibility().is_none());
        assert!(right.visibility().is_none());

        if left.cardinality() != right.cardinality() {
            return false;
        }

        left.rows()
            .zip_eq(right.rows())
            .all(|(row1, row2)| row1 == row2)
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
    /// (1, 6.1::FLOAT), (2, null), (null, 8.4::FLOAT), (3, 3.9::FLOAT), (null, null),
    /// (4, 6.6::FLOAT), (3, null), (null, 0.7::FLOAT), (5, null), (null, 5.5::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (8, 6.1::REAL), (2, null), (null, 8.9::REAL), (3, null), (null, 3.5::REAL),
    /// (6, null), (4, 7.5::REAL), (6, null), (null, 8::REAL), (7, null),
    /// (null, 9.1::REAL), (9, null), (3, 3.7::REAL), (9, null), (null, 9.6::REAL),
    /// (100, null), (null, 8.18::REAL), (200, null);
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
                 2 .
                 . 8.4
                 3 3.9
                 . .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i f
                 4 6.6
                 3 .
                 . 0.7
                 5 .
                 . 5.5",
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
                 8 6.1
                 2 .
                 . 8.9
                 3 .
                 . 3.5
                 6 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i F
                 4 7.5
                 6 .
                 . 8
                 7 .
                 . 9.1
                 9 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "  i F
                   3 3.7
                   9 .
                   . 9.6
                 100 .
                   . 8.18
                 200 .   ",
            ));

            Box::new(executor)
        }

        fn full_data_types(&self) -> Vec<DataType> {
            [self.left_types.clone(), self.right_types.clone()].concat()
        }

        fn output_data_types(&self) -> Vec<DataType> {
            let join_type = self.join_type;
            if join_type.keep_all() {
                [self.left_types.clone(), self.right_types.clone()].concat()
            } else if join_type.keep_left() {
                self.left_types.clone()
            } else if join_type.keep_right() {
                self.right_types.clone()
            } else {
                unreachable!()
            }
        }

        fn create_cond() -> BoxedExpression {
            let left_expr = InputRefExpression::new(DataType::Float32, 1);
            let right_expr = InputRefExpression::new(DataType::Float64, 3);
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(left_expr),
                Box::new(right_expr),
            )
        }

        fn create_join_executor(&self, has_non_equi_cond: bool) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let schema_fields = if join_type.keep_all() {
                [
                    left_child.schema().fields.clone(),
                    right_child.schema().fields.clone(),
                ]
                .concat()
            } else if join_type.keep_left() {
                left_child.schema().fields.clone()
            } else if join_type.keep_right() {
                right_child.schema().fields.clone()
            } else {
                unreachable!()
            };

            let full_data_types = self.full_data_types();

            let cond = if has_non_equi_cond {
                Some(Self::create_cond())
            } else {
                None
            };

            let left_col_len = left_child.schema().len();
            let right_col_len = right_child.schema().len();

            let params = EquiJoinParams {
                join_type,
                left_key_columns: vec![0],
                left_key_types: vec![self.left_types[0].clone()],
                left_col_len,
                right_key_columns: vec![0],
                right_key_types: vec![self.right_types[0].clone()],
                right_col_len,
                full_data_types,
                batch_size: 2,
                cond,
            };

            let schema = Schema {
                fields: schema_fields,
            };

            Box::new(HashJoinExecutor::<Key32>::new(
                left_child,
                right_child,
                params,
                schema,
                "HashJoinExecutor2".to_string(),
            )) as BoxedExecutor
        }

        fn select_from_chunk(&self, data_chunk: DataChunk) -> DataChunk {
            let join_type = self.join_type;
            let (columns, vis) = data_chunk.into_parts();

            let keep_columns = if join_type.keep_all() {
                vec![columns[1].clone(), columns[3].clone()]
            } else if join_type.keep_left() || join_type.keep_right() {
                vec![columns[1].clone()]
            } else {
                unreachable!()
            };

            DataChunk::new(keep_columns, vis)
        }

        async fn do_test(&self, expected: DataChunk, has_non_equi_cond: bool) {
            let join_executor = self.create_join_executor(has_non_equi_cond);

            let mut data_chunk_merger = DataChunkMerger::new(self.output_data_types()).unwrap();

            let fields = &join_executor.schema().fields;

            if self.join_type.keep_all() {
                assert_eq!(fields[1].data_type, DataType::Float32);
                assert_eq!(fields[3].data_type, DataType::Float64);
            } else if self.join_type.keep_left() {
                assert_eq!(fields[1].data_type, DataType::Float32);
            } else if self.join_type.keep_right() {
                assert_eq!(fields[1].data_type, DataType::Float64)
            } else {
                unreachable!()
            }

            let mut stream = join_executor.execute();

            while let Some(data_chunk) = stream.next().await {
                let data_chunk = data_chunk.unwrap();
                let data_chunk = data_chunk.compact().unwrap();
                data_chunk_merger.append(&data_chunk).unwrap();
            }

            let result_chunk = data_chunk_merger.finish().unwrap();

            // Take (t1.v2, t2.v2) in inner and left/right/full outer
            // or v2 decided by side of anti/semi.
            let output_chunk = self.select_from_chunk(result_chunk);

            // TODO: Replace this with unsorted comparison
            // assert_eq!(expected, result_chunk);
            assert!(is_data_chunk_eq(&expected, &output_chunk));
        }
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             .   .
             3.9 3.7
             3.9 .
             6.6 7.5
             .   3.7
             .   .  ",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_inner_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             6.6 7.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             6.1 .
             .   .
             8.4 .
             3.9 3.7
             3.9 .
             .   .
             6.6 7.5
             .   3.7
             .   .
             0.7 .
             .   .
             5.5 .  ",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             6.1 .
             .   .
             8.4 .
             3.9 .
             .   .
             6.6 7.5
             .   .
             0.7 .
             .   .
             5.5 .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 right outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             .   .
             3.9 3.7
             3.9 .
             6.6 7.5
             .   3.7
             .   .
             .   6.1
             .   8.9
             .   3.5
             .   .
             .   .
             .   8.0
             .   .
             .   9.1
             .   .
             .   .
             .   9.6
             .   .
             .   8.18
             .   .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             6.6 7.5
             .   6.1
             .   .
             .   8.9
             .   .
             .   3.5
             .   .
             .   .
             .   8.0
             .   .
             .   9.1
             .   .
             .   3.7
             .   .
             .   9.6
             .   .
             .   8.18
             .   .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 full outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let expected_chunk = DataChunk::from_pretty(
            "f   F
             6.1 .
             .   .
             8.4 .
             3.9 3.7
             3.9 .
             .   .
             6.6 7.5
             .   3.7
             .   .
             0.7 .
             .   .
             5.5 .
             .   6.1
             .   8.9
             .   3.5
             .   .
             .   .
             .   8.0
             .   .
             .   9.1
             .   .
             .   .
             .   9.6
             .   .
             .   8.18
             .   .   ",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "f
             6.1
             8.4
             .
             0.7
             .
             5.5",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_left_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "f
             6.1
             .
             8.4
             3.9
             .
             .
             0.7
             .
             5.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "f
             .
             3.9
             6.6
             .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_left_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "f
             6.6",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "F
             6.1
             8.9
             3.5
             .
             .
             8.0
             .
             9.1
             .
             .
             9.6
             .
             8.18
             .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_right_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "F
             6.1
             .
             8.9
             .
             3.5
             .
             .
             8.0
             .
             9.1
             .
             3.7
             .
             9.6
             .
             8.18
             .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "F
             .
             3.7
             .
             7.5",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_right_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "F
             7.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }
}
