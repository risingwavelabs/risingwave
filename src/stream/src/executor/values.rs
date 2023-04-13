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

use std::vec;

use await_tree::InstrumentAwait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::ensure;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::BoxedExpression;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    ActorContextRef, Barrier, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef,
    StreamExecutorError,
};
use crate::task::CreateMviewProgress;

const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Execute `values` in stream. As is a leaf, current workaround holds a `barrier_executor`.
/// May refractor with `BarrierRecvExecutor` in the near future.
pub struct ValuesExecutor {
    ctx: ActorContextRef,
    // Receiver of barrier channel.
    barrier_receiver: UnboundedReceiver<Barrier>,
    progress: CreateMviewProgress,

    rows: vec::IntoIter<Vec<BoxedExpression>>,
    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
}

impl ValuesExecutor {
    /// Currently hard-code the `pk_indices` as the last column.
    pub fn new(
        ctx: ActorContextRef,
        progress: CreateMviewProgress,
        rows: Vec<Vec<BoxedExpression>>,
        schema: Schema,
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
    ) -> Self {
        Self {
            ctx,
            progress,
            barrier_receiver,
            rows: rows.into_iter(),
            pk_indices: vec![schema.len()],
            identity: format!("ValuesExecutor {:X}", executor_id),
            schema,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(self) {
        let Self {
            mut progress,
            mut barrier_receiver,
            schema,
            mut rows,
            ..
        } = self;
        let barrier = barrier_receiver
            .recv()
            .instrument_await("values_executor_recv_first_barrier")
            .await
            .unwrap();

        let emit = barrier.is_newly_added(self.ctx.id);

        yield Message::Barrier(barrier);
        // If it's failover, do not evaluate rows (assume they have been yielded)
        if emit {
            let cardinality = schema.len();
            ensure!(cardinality > 0);
            while !rows.is_empty() {
                // We need a one row chunk rather than an empty chunk because constant
                // expression's eval result is same size as input chunk
                // cardinality.
                let one_row_chunk = DataChunk::new_dummy(1);

                let chunk_size = DEFAULT_CHUNK_SIZE.min(rows.len());
                let mut array_builders = schema.create_array_builders(chunk_size);
                for row in rows.by_ref().take(chunk_size) {
                    for (expr, builder) in row.into_iter().zip_eq_fast(&mut array_builders) {
                        let out = expr
                            .eval_infallible(&one_row_chunk, |err| {
                                self.ctx.on_compute_error(err, self.identity.as_str())
                            })
                            .await;
                        builder.append_array(&out);
                    }
                }

                let columns: Vec<_> = array_builders
                    .into_iter()
                    .map(|b| b.finish().into())
                    .collect();

                let chunk = DataChunk::new(columns, chunk_size);
                let ops = vec![Op::Insert; chunk_size];

                let stream_chunk = StreamChunk::from_parts(ops, chunk);
                yield Message::Chunk(stream_chunk);
            }
        }

        while let Some(barrier) = barrier_receiver.recv().await {
            if emit {
                progress.finish(barrier.epoch.curr);
            }
            yield Message::Barrier(barrier);
        }
    }
}

impl Executor for ValuesExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array;
    use risingwave_common::array::{
        ArrayImpl, I16Array, I32Array, I64Array, StructArray, StructValue,
    };
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::expr::{BoxedExpression, LiteralExpression};
    use tokio::sync::mpsc::unbounded_channel;

    use super::ValuesExecutor;
    use crate::executor::test_utils::StreamExecutorTestExt;
    use crate::executor::{ActorContext, Barrier, Executor, Mutation};
    use crate::task::{CreateMviewProgress, LocalBarrierManager};

    #[tokio::test]
    async fn test_values() {
        let barrier_manager = LocalBarrierManager::for_test();
        let progress =
            CreateMviewProgress::for_test(Arc::new(parking_lot::Mutex::new(barrier_manager)));
        let actor_id = progress.actor_id();
        let (tx, barrier_receiver) = unbounded_channel();
        let value = StructValue::new(vec![Some(1.into()), Some(2.into()), Some(3.into())]);
        let exprs = vec![
            Box::new(LiteralExpression::new(
                DataType::Int16,
                Some(ScalarImpl::Int16(1)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(2)),
            )),
            Box::new(LiteralExpression::new(
                DataType::Int64,
                Some(ScalarImpl::Int64(3)),
            )),
            Box::new(LiteralExpression::new(
                DataType::new_struct(
                    vec![DataType::Int32, DataType::Int32, DataType::Int32],
                    vec![],
                ),
                Some(ScalarImpl::Struct(value)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int64,
                Some(ScalarImpl::Int64(0)),
            )) as BoxedExpression,
        ];
        let fields = exprs
            .iter() // for each column
            .map(|col| Field::unnamed(col.return_type()))
            .collect::<Vec<Field>>();
        let values_executor_struct = ValuesExecutor::new(
            ActorContext::create(actor_id),
            progress,
            vec![exprs],
            Schema { fields },
            barrier_receiver,
            10005,
        );
        let mut values_executor = Box::new(values_executor_struct).execute();

        // Init barrier
        let first_message = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: Default::default(),
            added_actors: maplit::hashset! {actor_id},
            splits: Default::default(),
        });
        tx.send(first_message).unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));

        // Consume the barrier
        let values_msg = values_executor.next().await.unwrap().unwrap();

        let result = values_msg
            .into_chunk()
            .unwrap()
            .compact()
            .data_chunk()
            .to_owned();

        let array: ArrayImpl = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { I32Array, [Some(2)] }.into(),
                array! { I32Array, [Some(3)] }.into(),
            ],
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
        )
        .into();

        assert_eq!(
            *result.column_at(0).array(),
            array! {I16Array, [Some(1_i16)]}.into()
        );
        assert_eq!(
            *result.column_at(1).array(),
            array! {I32Array, [Some(2)]}.into()
        );
        assert_eq!(
            *result.column_at(2).array(),
            array! {I64Array, [Some(3)]}.into()
        );
        assert_eq!(*result.column_at(3).array(), array);
        assert_eq!(
            *result.column_at(4).array(),
            array! {I64Array, [Some(0)]}.into()
        );

        // ValueExecutor should simply forward following barriers
        tx.send(Barrier::new_test_barrier(2)).unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));

        tx.send(Barrier::new_test_barrier(3)).unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));
    }
}
