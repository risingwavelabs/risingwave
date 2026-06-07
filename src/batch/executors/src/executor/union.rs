// Copyright 2024 RisingWave Labs
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

use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_expr::expr_context::{capture_expr_context, expr_context_scope};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use rw_futures_util::select_all;
use tokio::sync::mpsc;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    PushContext, PushSink, PushStatus,
};

pub struct UnionExecutor {
    inputs: Vec<BoxedExecutor>,
    identity: String,
}

impl Executor for UnionExecutor {
    fn schema(&self) -> &Schema {
        self.inputs[0].schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let (sender, mut receiver) = mpsc::channel(context.morsel_queue_capacity());
            let mut tasks = Vec::with_capacity(self.inputs.len());
            for input in self.inputs {
                let context = context.clone();
                let sender = sender.clone();
                let task_scope = context.task_scope();
                let expr_context = task_scope
                    .is_none()
                    .then(|| capture_expr_context().ok())
                    .flatten();
                tasks.push(tokio::spawn(async move {
                    let exec = async move {
                        let mut child_sink = UnionChildSink::from_sender(sender.clone());
                        if let Err(error) = input.execute_push(context, &mut child_sink).await {
                            let _ = sender.send(Err(error)).await;
                        }
                    }
                    .boxed();
                    if let Some(task_scope) = task_scope {
                        task_scope.scope(exec).await;
                    } else if let Some(expr_context) = expr_context {
                        expr_context_scope(expr_context, exec).await;
                    } else {
                        exec.await;
                    }
                }));
            }
            drop(sender);

            while let Some(chunk) = receiver.recv().await {
                let chunk = chunk?;
                if sink.push(chunk).await?.is_finished() {
                    abort_union_tasks(&tasks);
                    return sink.finish().await;
                }
            }

            for task in tasks {
                match task.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        return Err(BatchError::Internal(anyhow::anyhow!(error.to_string())));
                    }
                }
            }

            sink.finish().await
        }
        .boxed()
    }
}

struct UnionChildSink {
    sender: mpsc::Sender<Result<DataChunk>>,
}

impl UnionChildSink {
    fn from_sender(sender: mpsc::Sender<Result<DataChunk>>) -> Self {
        Self { sender }
    }
}

impl PushSink for UnionChildSink {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            if self.sender.send(Ok(chunk)).await.is_err() {
                return Ok(PushStatus::Finished);
            }
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }

    fn requires_input_order(&self) -> bool {
        false
    }
}

fn abort_union_tasks(tasks: &[tokio::task::JoinHandle<()>]) {
    for task in tasks {
        task.abort();
    }
}

impl UnionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let mut stream = select_all(
            self.inputs
                .into_iter()
                .map(|input| input.execute())
                .collect_vec(),
        )
        .boxed();

        while let Some(data_chunk) = stream.next().await {
            let data_chunk = data_chunk?;
            yield data_chunk
        }
    }
}

impl BoxedExecutorBuilder for UnionExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let _union_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::Union)?;

        Ok(Box::new(Self::new(
            inputs,
            source.plan_node().get_identity().clone(),
        )))
    }
}

impl UnionExecutor {
    pub fn new(inputs: Vec<BoxedExecutor>, identity: String) -> Self {
        Self { inputs, identity }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::stream::StreamExt;
    use risingwave_common::array::{Array, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;

    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{Executor, PushContext, UnionExecutor, collect_push_input};
    use crate::task::ShutdownToken;

    #[tokio::test]
    async fn test_union_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor1 = MockExecutor::new(schema.clone());
        mock_executor1.add(DataChunk::from_pretty(
            "i i
             1 10
             2 20
             3 30
             4 40",
        ));

        let mut mock_executor2 = MockExecutor::new(schema);
        mock_executor2.add(DataChunk::from_pretty(
            "i i
             5 50
             6 60
             7 70
             8 80",
        ));

        let union_executor = Box::new(UnionExecutor {
            inputs: vec![Box::new(mock_executor1), Box::new(mock_executor2)],
            identity: "UnionExecutor".to_owned(),
        });
        let fields = &union_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        let mut stream = union_executor.execute();
        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));
        if let Ok(res) = res {
            let col1 = res.column_at(0);
            let array = col1;
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 4);
            assert_eq!(col1.value_at(0), Some(1));
            assert_eq!(col1.value_at(1), Some(2));
            assert_eq!(col1.value_at(2), Some(3));
            assert_eq!(col1.value_at(3), Some(4));

            let col2 = res.column_at(1);
            let array = col2;
            let col2 = array.as_int32();
            assert_eq!(col2.len(), 4);
            assert_eq!(col2.value_at(0), Some(10));
            assert_eq!(col2.value_at(1), Some(20));
            assert_eq!(col2.value_at(2), Some(30));
            assert_eq!(col2.value_at(3), Some(40));
        }

        let res = stream.next().await.unwrap();
        assert_matches!(res, Ok(_));
        if let Ok(res) = res {
            let col1 = res.column_at(0);
            let array = col1;
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 4);
            assert_eq!(col1.value_at(0), Some(5));
            assert_eq!(col1.value_at(1), Some(6));
            assert_eq!(col1.value_at(2), Some(7));
            assert_eq!(col1.value_at(3), Some(8));

            let col2 = res.column_at(1);
            let array = col2;
            let col2 = array.as_int32();
            assert_eq!(col2.len(), 4);
            assert_eq!(col2.value_at(0), Some(50));
            assert_eq!(col2.value_at(1), Some(60));
            assert_eq!(col2.value_at(2), Some(70));
            assert_eq!(col2.value_at(3), Some(80));
        }

        let res = stream.next().await;
        assert_matches!(res, None);
    }

    #[tokio::test]
    async fn test_union_push_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor1 = MockExecutor::new(schema.clone());
        mock_executor1.add(DataChunk::from_pretty(
            "i i
             1 10
             2 20
             3 30
             4 40",
        ));

        let mut mock_executor2 = MockExecutor::new(schema);
        mock_executor2.add(DataChunk::from_pretty(
            "i i
             5 50
             6 60
             7 70
             8 80",
        ));

        let union_executor = Box::new(UnionExecutor {
            inputs: vec![Box::new(mock_executor1), Box::new(mock_executor2)],
            identity: "UnionExecutor".to_owned(),
        });
        let context = PushContext::new(ShutdownToken::empty()).with_morsel_parallelism(2);
        let (_schema, chunks) = collect_push_input(union_executor, context).await.unwrap();
        let mut rows = vec![];
        for chunk in chunks {
            let col1 = chunk.column_at(0).as_int32();
            let col2 = chunk.column_at(1).as_int32();
            for row_idx in 0..chunk.cardinality() {
                rows.push((
                    col1.value_at(row_idx).unwrap(),
                    col2.value_at(row_idx).unwrap(),
                ));
            }
        }
        rows.sort();

        assert_eq!(
            rows,
            vec![
                (1, 10),
                (2, 20),
                (3, 30),
                (4, 40),
                (5, 50),
                (6, 60),
                (7, 70),
                (8, 80)
            ]
        );
    }
}
