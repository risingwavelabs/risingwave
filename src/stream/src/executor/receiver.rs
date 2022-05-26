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

use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use risingwave_common::catalog::Schema;

use super::{ActorContextRef, OperatorInfo};
use crate::executor::{
    BoxedMessageStream, Executor, ExecutorInfo, Message, PkIndices, PkIndicesRef,
};

/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    receiver: Receiver<Message>,
    /// Logical Operator Info
    info: ExecutorInfo,
    /// Actor context
    actor_context: ActorContextRef,
    actor_context_position: usize,
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
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        receiver: Receiver<Message>,
        actor_context: ActorContextRef,
        receiver_id: u64,
    ) -> Self {
        let actor_context_position = {
            let mut ctx = actor_context.lock();
            let actor_context_position = ctx.info.len();
            ctx.info.push(OperatorInfo::new(receiver_id));
            actor_context_position
        };

        Self {
            receiver,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "ReceiverExecutor".to_string(),
            },
            actor_context,
            actor_context_position,
        }
    }
}

impl Executor for ReceiverExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let actor_context = self.actor_context.clone();
        let actor_context_position = self.actor_context_position;
        self.receiver
            .map(move |msg| {
                actor_context.lock().info[actor_context_position].next_message(&msg);
                Ok(msg)
            })
            .boxed()
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
