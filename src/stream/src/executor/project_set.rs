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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use either::{for_both, Either};
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayRef, DataChunk, I64ArrayBuilder, Op, StreamChunk,
};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::table_function::ProjectSetSelectItem;

use super::error::StreamExecutorError;
use super::{BoxedExecutor, Executor, ExecutorInfo, Message, PkIndices, PkIndicesRef};

impl ProjectSetExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        select_list: Vec<ProjectSetSelectItem>,
        executor_id: u64,
    ) -> Self {
        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(
            select_list
                .iter()
                .map(|expr| Field::unnamed(expr.return_type())),
        );

        let info = ExecutorInfo {
            schema: Schema { fields },
            pk_indices,
            identity: format!("ProjectSet {:X}", executor_id),
        };
        Self {
            input,
            info,
            select_list,
        }
    }
}

/// `ProjectSetExecutor` projects data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectSetExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectSetExecutor {
    input: BoxedExecutor,
    info: ExecutorInfo,
    /// Expressions of the current project_section.
    select_list: Vec<ProjectSetSelectItem>,
}

impl Debug for ProjectSetExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectSetExecutor")
            .field("exprs", &self.select_list)
            .finish()
    }
}

impl Executor for ProjectSetExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl ProjectSetExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let data_types = self
            .select_list
            .iter()
            .map(|i| i.return_type())
            .collect_vec();
        let input = self.input.execute();

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact()?;

                    let (data_chunk, ops) = chunk.into_parts();

                    // First column will be `projected_row_id`, which represents the index in the
                    // output table
                    let mut projected_row_id_builder =
                        I64ArrayBuilder::new(DEFAULT_CHUNK_BUFFER_SIZE);
                    let mut builders = data_types
                        .iter()
                        .map(|ty| ty.create_array_builder(DEFAULT_CHUNK_BUFFER_SIZE))
                        .collect_vec();
                    let mut ret_ops = vec![];

                    let results: Vec<_> = self
                        .select_list
                        .iter()
                        .map(|select_item| select_item.eval(&data_chunk))
                        .try_collect()?;
                    assert!(
                        results
                            .iter()
                            .all(|result| ops.len() == for_both!(result, r=>r.len())),
                        "ProjectSet has mismatched output cardinalities among select list."
                    );

                    // each iteration corresponds to the outputs of one input row
                    for (row_idx, op) in ops.into_iter().enumerate() {
                        let items = results
                            .iter()
                            .map(|result| match result {
                                Either::Left(arrays) => Either::Left(arrays[row_idx].clone()),
                                Either::Right(array) => Either::Right(array.value_at(row_idx)),
                            })
                            .collect_vec();

                        // The maximum length of the results of table functions will be the output
                        // length.
                        let max_tf_len = items
                            .iter()
                            .map(|i| i.as_ref().map_left(|arr| arr.len()).left_or(0))
                            .max()
                            .unwrap();

                        // ProjectSet cannot preserve that U- is followed by U+,
                        // so we rewrite update to insert/delete.
                        let op = match op {
                            Op::Delete | Op::UpdateDelete => Op::Delete,
                            Op::Insert | Op::UpdateInsert => Op::Insert,
                        };
                        ret_ops.extend(vec![op; max_tf_len]);
                        for i in 0..max_tf_len {
                            projected_row_id_builder.append(Some(i as i64))?;
                        }

                        for (item, builder) in items.into_iter().zip_eq(builders.iter_mut()) {
                            match item {
                                Either::Left(array_ref) => {
                                    builder.append_array(&array_ref)?;
                                    for _ in 0..(max_tf_len - array_ref.len()) {
                                        builder.append_null()?;
                                    }
                                }
                                Either::Right(datum_ref) => {
                                    for _ in 0..max_tf_len {
                                        builder.append_datum_ref(datum_ref)?;
                                    }
                                }
                            }
                        }
                    }

                    let mut columns = Vec::with_capacity(self.select_list.len() + 1);
                    let projected_row_id: ArrayRef =
                        Arc::new(projected_row_id_builder.finish().into());
                    let cardinality = projected_row_id.len();
                    columns.push(Column::new(projected_row_id));
                    for builder in builders {
                        columns.push(Column::new(Arc::new(builder.finish())))
                    }

                    let chunk = DataChunk::new(columns, cardinality);

                    yield Message::Chunk(StreamChunk::from_parts(ret_ops, chunk));
                }
                m => yield m,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{Expression, InputRefExpression, LiteralExpression};
    use risingwave_expr::table_function::repeat_tf;
    use risingwave_pb::expr::expr_node::Type;

    use super::super::test_utils::MockSource;
    use super::super::*;
    use super::*;

    #[tokio::test]
    async fn test_project_set() {
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 7 8
            - 3 6",
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let source = MockSource::with_chunks(schema, PkIndices::new(), vec![chunk1, chunk2]);

        let left_expr = InputRefExpression::new(DataType::Int64, 0);
        let right_expr = InputRefExpression::new(DataType::Int64, 1);
        let test_expr = new_binary_expr(
            Type::Add,
            DataType::Int64,
            Box::new(left_expr),
            Box::new(right_expr),
        );
        let tf1 = repeat_tf(
            LiteralExpression::new(DataType::Int32, Some(1_i32.into())).boxed(),
            1,
        );
        let tf2 = repeat_tf(
            LiteralExpression::new(DataType::Int32, Some(2_i32.into())).boxed(),
            2,
        );

        let project_set = Box::new(ProjectSetExecutor::new(
            Box::new(source),
            vec![],
            vec![test_expr.into(), tf1.into(), tf2.into()],
            1,
        ));

        let expected = vec![
            StreamChunk::from_pretty(
                " I I i i
                + 0 5 1 2
                + 1 5 . 2
                + 0 7 1 2
                + 1 7 . 2
                + 0 9 1 2
                + 1 9 . 2",
            ),
            StreamChunk::from_pretty(
                " I I  i i
                + 0 15 1 2
                + 1 15 . 2
                - 0 9  1 2
                - 1 9  . 2",
            ),
        ];

        let mut project_set = project_set.execute();

        for expected in expected {
            let msg = project_set.next().await.unwrap().unwrap();
            let chunk = msg.as_chunk().unwrap();
            assert_eq!(*chunk, expected);
        }
        assert!(project_set.next().await.unwrap().unwrap().is_stop());
    }
}
