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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::exchange::input::BoxedInput;
use super::ActorContextRef;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    BoxedMessageStream, Executor, ExecutorInfo, Message, PkIndices, PkIndicesRef,
};
use crate::task::FragmentId;
/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    /// Input from upstream.
    input: BoxedInput,

    /// Logical Operator Info
    info: ExecutorInfo,

    ctx: ActorContextRef,

    /// Upstream fragment id.
    upstream_fragment_id: FragmentId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl ReceiverExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        input: BoxedInput,
        ctx: ActorContextRef,
        _receiver_id: u64,
        upstream_fragment_id: FragmentId,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            input,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "ReceiverExecutor".to_string(),
            },
            ctx,
            upstream_fragment_id,
            metrics,
        }
    }
}

impl Executor for ReceiverExecutor {
    fn execute(mut self: Box<Self>) -> BoxedMessageStream {
        let actor_id_str = self.ctx.id.to_string();
        let upstream_fragment_id_str = self.upstream_fragment_id.to_string();

        let stream = #[try_stream]
        async move {
            let mut start_time = minstant::Instant::now();
            while let Some(msg) = self.input.next().await {
                self.metrics
                    .actor_input_buffer_blocking_duration_ns
                    .with_label_values(&[&actor_id_str, &upstream_fragment_id_str])
                    .inc_by(start_time.elapsed().as_nanos() as u64);
                let msg: Message = msg?;

                match &msg {
                    Message::Chunk(chunk) => {
                        self.metrics
                            .actor_in_record_cnt
                            .with_label_values(&[&actor_id_str])
                            .inc_by(chunk.cardinality() as _);
                    }
                    Message::Barrier(_) => {}
                };

                yield msg;
                start_time = minstant::Instant::now();
            }
        };

        stream.boxed()
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
