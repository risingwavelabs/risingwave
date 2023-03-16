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

use futures::StreamExt;
use risingwave_common::catalog::Schema;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    ActorContext, ActorContextRef, Barrier, BoxedMessageStream, Executor, Message, PkIndicesRef,
    StreamExecutorError,
};

pub struct BarrierRecvExecutor {
    _ctx: ActorContextRef,

    identity: String,

    barrier_receiver: UnboundedReceiver<Barrier>,
}

impl BarrierRecvExecutor {
    pub fn new(
        ctx: ActorContextRef,
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
    ) -> Self {
        Self {
            _ctx: ctx,
            identity: format!("BarrierRecvExecutor {:X}", executor_id),
            barrier_receiver,
        }
    }

    pub fn for_test(barrier_receiver: UnboundedReceiver<Barrier>) -> Self {
        Self::new(ActorContext::create(0), barrier_receiver, 0)
    }
}

impl Executor for BarrierRecvExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        UnboundedReceiverStream::new(self.barrier_receiver)
            .map(|barrier| Ok(Message::Barrier(barrier)))
            .chain(futures::stream::once(async {
                Err(StreamExecutorError::channel_closed("barrier receiver"))
            }))
            .boxed()
    }

    fn schema(&self) -> &Schema {
        Schema::empty()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &[]
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use futures::pin_mut;
    use tokio::sync::mpsc;

    use super::*;
    use crate::executor::test_utils::StreamExecutorTestExt;

    #[tokio::test]
    async fn test_barrier_recv() {
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let barrier_recv = BarrierRecvExecutor::for_test(barrier_rx).boxed();
        let stream = barrier_recv.execute();
        pin_mut!(stream);

        barrier_tx.send(Barrier::new_test_barrier(114)).unwrap();
        barrier_tx.send(Barrier::new_test_barrier(514)).unwrap();

        let barrier_1 = stream.next_unwrap_ready_barrier().unwrap();
        assert_eq!(barrier_1.epoch.curr, 114);
        let barrier_2 = stream.next_unwrap_ready_barrier().unwrap();
        assert_eq!(barrier_2.epoch.curr, 514);

        stream.next_unwrap_pending();

        drop(barrier_tx);
        assert!(stream.next_unwrap_ready().is_err());
    }
}
