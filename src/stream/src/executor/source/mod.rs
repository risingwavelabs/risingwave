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

pub mod executor_core;
use async_stack_trace::StackTrace;
pub use executor_core::StreamSourceCore;
mod fs_source_executor;
pub use fs_source_executor::*;
use risingwave_common::bail;
pub use state_table_handler::*;

pub mod source_executor;

pub mod state_table_handler;

use futures_async_stream::try_stream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Barrier, Message};

/// Receive barriers from barrier manager with the channel, error on channel close.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn barrier_to_message_stream(mut rx: UnboundedReceiver<Barrier>) {
    while let Some(barrier) = rx.recv().stack_trace("receive_barrier").await {
        yield Message::Barrier(barrier);
    }
    bail!("barrier reader closed unexpectedly");
}
