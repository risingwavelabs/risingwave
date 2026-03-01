// Copyright 2022 RisingWave Labs
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

use std::fmt;

use await_tree::InstrumentAwait;

use super::error::ExchangeChannelClosed;
use super::multiplexed::MultiplexedActorOutput;
use super::permit::Sender;
use crate::error::StreamResult;
use crate::executor::DispatcherMessageBatch as Message;
use crate::task::ActorId;

/// Output channel for a dispatcher to send messages to a downstream actor.
///
/// - `Direct`: a plain `permit::Sender` channel (used for both local and non-multiplexed remote).
/// - `Multiplexed`: a `MultiplexedActorOutput` handle that shares a single physical channel
///   with other upstream actors on the same node, performing sender-side barrier coalescing.
pub enum Output {
    Direct {
        actor_id: ActorId,
        span: await_tree::Span,
        ch: Sender,
    },
    Multiplexed {
        /// The downstream actor ID (used for hash-based routing and output management).
        actor_id: ActorId,
        inner: MultiplexedActorOutput,
        span: await_tree::Span,
    },
}

impl fmt::Debug for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Output::Direct { actor_id, .. } => f
                .debug_struct("Output::Direct")
                .field("actor_id", actor_id)
                .finish(),
            Output::Multiplexed {
                actor_id, inner, ..
            } => f
                .debug_struct("Output::Multiplexed")
                .field("downstream_actor_id", actor_id)
                .field("upstream_actor_id", &inner.actor_id())
                .finish(),
        }
    }
}

impl Output {
    pub fn new(actor_id: ActorId, ch: Sender) -> Self {
        Output::Direct {
            span: await_tree::span!("Output (actor {:?})", actor_id).verbose(),
            actor_id,
            ch,
        }
    }

    pub fn new_multiplexed(actor_id: ActorId, inner: MultiplexedActorOutput) -> Self {
        Output::Multiplexed {
            span: await_tree::span!("Output (actor {:?}, multiplexed)", actor_id).verbose(),
            actor_id,
            inner,
        }
    }

    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        match self {
            Output::Direct { actor_id, span, ch } => ch
                .send(message)
                .instrument_await(span.clone())
                .await
                .map_err(|_| ExchangeChannelClosed::output(*actor_id).into()),
            Output::Multiplexed { inner, span, .. } => {
                inner.send(message).instrument_await(span.clone()).await
            }
        }
    }

    pub fn actor_id(&self) -> ActorId {
        match self {
            Output::Direct { actor_id, .. } | Output::Multiplexed { actor_id, .. } => *actor_id,
        }
    }
}
