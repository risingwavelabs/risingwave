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

use std::fmt::{Debug, Formatter};

use anyhow::anyhow;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;

/// A dummy source executor that only receives barrier messages and sends them to
/// the downstream executor. Used when there is no external stream source.
pub struct DummySourceExecutor {
    actor_ctx: ActorContextRef,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
}

impl DummySourceExecutor {
    pub fn new(actor_ctx: ActorContextRef, barrier_receiver: UnboundedReceiver<Barrier>) -> Self {
        Self {
            actor_ctx,
            barrier_receiver: Some(barrier_receiver),
        }
    }

    /// A dummy source executor only receives barrier messages and sends them to
    /// the downstream executor.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?} with no stream source",
                    self.actor_ctx.id
                )
            })?;
        yield Message::Barrier(barrier);

        while let Some(barrier) = barrier_receiver.recv().await {
            yield Message::Barrier(barrier);
        }
    }
}

impl Execute for DummySourceExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl Debug for DummySourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DummySourceExecutor")
            .field("actor_id", &self.actor_ctx.id)
            .finish()
    }
}
