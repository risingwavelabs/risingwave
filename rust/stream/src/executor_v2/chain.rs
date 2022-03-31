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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use super::error::TracedStreamExecutorError;
use super::{BoxedExecutor, Executor, ExecutorInfo, Message, Mutation};
use crate::task::{ActorId, FinishCreateMviewNotifier};

/// [`ChainExecutor`] is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, [`ChainExecutor`] is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
pub struct ChainExecutor {
    snapshot: BoxedExecutor,

    upstream: BoxedExecutor,

    upstream_indices: Vec<usize>,

    notifier: FinishCreateMviewNotifier,

    actor_id: ActorId,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], msg: Message) -> Message {
    match msg {
        Message::Chunk(chunk) => {
            let columns = upstream_indices
                .iter()
                .map(|i| chunk.columns()[*i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(
                chunk.ops().to_vec(),
                columns,
                chunk.visibility().clone(),
            ))
        }
        _ => msg,
    }
}

impl ChainExecutor {
    pub fn new(
        snapshot: BoxedExecutor,
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        notifier: FinishCreateMviewNotifier,
        actor_id: ActorId,
        info: ExecutorInfo,
    ) -> Self {
        Self {
            snapshot,
            upstream,
            upstream_indices,
            notifier,
            actor_id,
            info,
        }
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let mut upstream = self.upstream.execute();

        // 1. Consume the upstream to get the first barrier.
        //
        let first_msg = upstream.next().await.unwrap()?;
        let barrier = first_msg
            .as_barrier()
            .expect("the first message received by chain must be a barrier");
        let epoch = barrier.epoch;

        let to_consume_snapshot = match barrier.mutation.as_ref().cloned().as_deref() {
            // If the barrier is a conf change of creating this mview, init snapshot from its epoch
            // and begin to consume the snapshot.
            Some(Mutation::AddOutput(map)) => map
                .values()
                .flatten()
                .any(|info| info.actor_id == self.actor_id),

            // If the barrier is not a conf change, it means we've recovered and the snapshot is
            // already consumed.
            _ => false,
        };

        // The first barrier message should be propagated.
        yield first_msg;

        // 2. Consume the snapshot if needed. Note that the snapshot is alreay projected, so there's
        // no mapping required.
        //
        if to_consume_snapshot {
            // Init the snapshot with reading epoch.
            let snapshot = self.snapshot.execute_with_epoch(epoch.prev);

            #[for_await]
            for msg in snapshot {
                let msg = msg?;
                yield msg;
            }
        }

        // 3. Report that we've finished the creation (for a workaround).
        //
        self.notifier.notify(epoch.curr);

        // 4. Continuously consume the upstream.
        //
        #[for_await]
        for msg in upstream {
            let msg = msg?;
            yield mapping(&self.upstream_indices, msg);
        }
    }
}

impl Executor for ChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

// TODO: restore the unit tests
