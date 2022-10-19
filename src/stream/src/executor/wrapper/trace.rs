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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use async_stack_trace::{SpanValue, StackTrace};
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use minitrace::prelude::*;
use tracing::event;

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ExecutorInfo, Message, MessageStream};
use crate::task::ActorId;

/// Streams wrapped by `trace` will print data passing in the stream graph to stdout.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn trace(
    enable_executor_row_count: bool,
    info: Arc<ExecutorInfo>,
    input_pos: usize,
    actor_id: ActorId,
    executor_id: u64,
    metrics: Arc<StreamingMetrics>,
    input: impl MessageStream,
) {
    let span_name = format!("{}_{}_next", info.identity, input_pos);
    let actor_id_string = actor_id.to_string();
    let executor_id_string = executor_id.to_string();

    let span = || {
        let mut span = Span::enter_with_local_parent("next");
        span.add_property(|| ("otel.name", span_name.to_string()));
        span.add_property(|| ("next", info.identity.to_string()));
        span.add_property(|| ("input_pos", input_pos.to_string()));
        span
    };

    pin_mut!(input);

    while let Some(message) = input.next().in_span(span()).await.transpose()? {
        if let Message::Chunk(chunk) = &message {
            if chunk.cardinality() > 0 {
                if enable_executor_row_count {
                    metrics
                        .executor_row_count
                        .with_label_values(&[&actor_id_string, &executor_id_string])
                        .inc_by(chunk.cardinality() as u64);
                }
                event!(tracing::Level::TRACE, prev = %info.identity, msg = "chunk", "input = \n{:#?}", chunk);
            }
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

/// Streams wrapped by `stack_trace` will print the async stack trace of the executors.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn stack_trace(
    info: Arc<ExecutorInfo>,
    actor_id: ActorId,
    executor_id: u64,
    input: impl MessageStream,
) {
    pin_mut!(input);

    let span: SpanValue = pretty_identity(&info.identity, actor_id, executor_id).into();

    while let Some(message) = input.next().stack_trace(span.clone()).await.transpose()? {
        yield message;
    }
}

/// Streams wrapped by `unwind_trace` will print the identity of the executors when panicking.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn unwind_trace(
    info: Arc<ExecutorInfo>,
    actor_id: ActorId,
    executor_id: u64,
    input: impl MessageStream,
) {
    let identity = pretty_identity(&info.identity, actor_id, executor_id);

    let input = AssertUnwindSafe(input).catch_unwind().map(|r| {
        r.unwrap_or_else(|e| {
            println!("*** unwinding panic {e:p} *** {identity}");
            std::panic::resume_unwind(e)
        })
    });
    pin_mut!(input);

    while let Some(message) = input.next().await.transpose()? {
        yield message;
    }
}
