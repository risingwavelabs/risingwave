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

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{BoxedExecutor, Executor, Message, PkIndicesRef, StreamExecutorResult};
use crate::executor::{
    AlignedMessage, BarrierAligner, Executor as ExecutorV1, ExecutorBuilder, PkIndices,
};
use crate::executor_v2::error::TracedStreamExecutorError;
use crate::executor_v2::{BoxedMessageStream, ExecutorInfo};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

/// `UnionExecutor` merges data from multiple inputs. Currently this is done by using
/// [`BarrierAligner`]. In the future we could have more efficient implementation.
pub struct UnionExecutor {
    inputs: Vec<BoxedExecutor>,
    info: ExecutorInfo,
}

impl std::fmt::Debug for UnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl UnionExecutor {
    pub fn new(pk_indices: PkIndices, inputs: Vec<BoxedExecutor>) -> Self {
        Self {
            info: ExecutorInfo {
                schema: inputs[0].schema().clone(),
                pk_indices,
                identity: "UnionExecutor".to_string(),
            },
            inputs,
        }
    }
}

#[async_trait]
impl Executor for UnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
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

struct BarrierAlignerWrapper(BarrierAligner, ExecutorInfo);

impl Executor for BarrierAlignerWrapper {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.1.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.1.pk_indices
    }

    fn identity(&self) -> &str {
        &self.1.identity
    }
}

impl BarrierAlignerWrapper {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(mut self) {
        loop {
            match self.0.next().await {
                AlignedMessage::Left(x) => {
                    yield Message::Chunk(x.map_err(StreamExecutorError::InputError)?)
                }
                AlignedMessage::Right(x) => {
                    yield Message::Chunk(x.map_err(StreamExecutorError::InputError)?)
                }
                AlignedMessage::Barrier(x) => yield Message::Barrier(x),
            }
        }
    }
}

fn build_align_executor(mut inputs: Vec<BoxedExecutor>) -> StreamExecutorResult<BoxedExecutor> {
    match inputs.len() {
        0 => unreachable!(),
        1 => Ok(inputs.remove(0)),
        _ => {
            let right = build_align_executor(inputs.split_off(inputs.len() / 2))?;
            let left = build_align_executor(inputs)?;
            let schema = left.schema().clone();
            let pk_indices = right.pk_indices().to_vec();
            assert_eq!(&schema, right.schema());
            assert_eq!(pk_indices, right.pk_indices());
            let aligner = BarrierAligner::new(Box::new(left.v1()), Box::new(right.v1()));
            Ok(BarrierAlignerWrapper(
                aligner,
                ExecutorInfo {
                    schema,
                    pk_indices,
                    identity: "BarrierAlignerWrapper".to_string(),
                },
            )
            .boxed())
        }
    }
}

impl UnionExecutor {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let executor = build_align_executor(self.inputs)?;
        let stream = executor.execute();

        #[for_await]
        for item in stream {
            yield item?;
        }
    }
}

pub struct UnionExecutorBuilder {}

impl ExecutorBuilder for UnionExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> risingwave_common::error::Result<BoxedExecutor> {
        try_match_expand!(node.get_node().unwrap(), Node::UnionNode)?;
        Ok(UnionExecutor::new(params.pk_indices, params.input).boxed())
    }
}
