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

use std::sync::Arc;

use await_tree::InstrumentAwait;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use tracing::{event, Instrument};

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ExecutorInfo, Message, MessageStream};
use crate::task::ActorId;

/// Streams wrapped by `trace` will print data passing in the stream graph to stdout.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn trace(
    enable_executor_row_count: bool,
    info: Arc<ExecutorInfo>,
    _input_pos: usize,
    actor_id: ActorId,
    _executor_id: u64,
    metrics: Arc<StreamingMetrics>,
    input: impl MessageStream,
) {
    let actor_id_string = actor_id.to_string();

    let mut span = tracing::span!(
        target: "epoch_trace",
        tracing::Level::INFO,
        "executor_next",
        executor = info.identity
    );

    pin_mut!(input);

    while let Some(message) = input.next().instrument(span.clone()).await.transpose()? {
        if let Message::Chunk(chunk) = &message {
            if chunk.cardinality() > 0 {
                if enable_executor_row_count {
                    metrics
                        .executor_row_count
                        .with_label_values(&[&actor_id_string, &info.identity])
                        .inc_by(chunk.cardinality() as u64);
                }
                event!(tracing::Level::TRACE, prev = %info.identity, msg = "chunk", "input = \n{:#?}", chunk);
            }
        }

        if let Message::Barrier(barrier) = &message {
            span = barrier.tracing_context().attach(span);
        }

        yield message;
    }
}

/// Streams wrapped by `metrics` will update actor metrics.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn metrics(
    enable_executor_row_count: bool,
    actor_id: ActorId,
    executor_id: u64,
    metrics: Arc<StreamingMetrics>,
    input: impl MessageStream,
) {
    let actor_id_string = actor_id.to_string();
    let executor_id_string = executor_id.to_string();
    pin_mut!(input);

    while let Some(message) = input.next().await.transpose()? {
        if enable_executor_row_count {
            if let Message::Chunk(chunk) = &message {
                if chunk.cardinality() > 0 {
                    metrics
                        .executor_row_count
                        .with_label_values(&[&actor_id_string, &executor_id_string])
                        .inc_by(chunk.cardinality() as u64);
                }
            }
        }

        yield message;
    }
}

fn pretty_identity(identity: &str, actor_id: ActorId, executor_id: u64) -> String {
    format!(
        "{} (actor {}, executor {})",
        identity,
        actor_id,
        executor_id as u32 // Use the lower 32 bit to match the dashboard.
    )
}

/// Streams wrapped by `instrument_await_tree` will be able to print the spans of the
/// executors in the stack trace through `await-tree`.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn instrument_await_tree(
    info: Arc<ExecutorInfo>,
    actor_id: ActorId,
    executor_id: u64,
    input: impl MessageStream,
) {
    pin_mut!(input);

    let span: await_tree::Span = pretty_identity(&info.identity, actor_id, executor_id).into();

    while let Some(message) = input
        .next()
        .instrument_await(span.clone())
        .await
        .transpose()?
    {
        yield message;
    }
}
