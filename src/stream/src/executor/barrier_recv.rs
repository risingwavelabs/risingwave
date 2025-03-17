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

use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::executor::prelude::*;

/// The executor only for receiving barrier from the meta service. It always resides in the leaves
/// of the streaming graph.
pub struct BarrierRecvExecutor {
    _ctx: ActorContextRef,

    /// The barrier receiver registered in the local barrier manager.
    barrier_receiver: UnboundedReceiver<Barrier>,
}

impl BarrierRecvExecutor {
    pub fn new(ctx: ActorContextRef, barrier_receiver: UnboundedReceiver<Barrier>) -> Self {
        Self {
            _ctx: ctx,
            barrier_receiver,
        }
    }

    pub fn for_test(barrier_receiver: UnboundedReceiver<Barrier>) -> Self {
        Self::new(ActorContext::for_test(0), barrier_receiver)
    }
}

impl Execute for BarrierRecvExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        UnboundedReceiverStream::new(self.barrier_receiver)
            .map(|barrier| Ok(Message::Barrier(barrier)))
            .chain(futures::stream::once(async {
                // We do not use the stream termination as the control message, and this line should
                // never be reached in normal cases. So we just return an error here.
                Err(StreamExecutorError::channel_closed("barrier receiver"))
            }))
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use futures::pin_mut;
    use risingwave_common::util::epoch::test_epoch;
    use tokio::sync::mpsc;

    use super::*;
    use crate::executor::test_utils::StreamExecutorTestExt;

    #[tokio::test]
    async fn test_barrier_recv() {
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let barrier_recv = BarrierRecvExecutor::for_test(barrier_rx).boxed();
        let stream = barrier_recv.execute();
        pin_mut!(stream);

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(1)))
            .unwrap();
        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(2)))
            .unwrap();

        let barrier_1 = stream.next_unwrap_ready_barrier().unwrap();
        assert_eq!(barrier_1.epoch.curr, test_epoch(1));
        let barrier_2 = stream.next_unwrap_ready_barrier().unwrap();
        assert_eq!(barrier_2.epoch.curr, test_epoch(2));

        stream.next_unwrap_pending();

        drop(barrier_tx);
        assert!(stream.next_unwrap_ready().is_err());
    }
}
