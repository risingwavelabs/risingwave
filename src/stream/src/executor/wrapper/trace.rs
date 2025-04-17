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

use std::sync::Arc;

use await_tree::InstrumentAwait;
use futures::{StreamExt, pin_mut};
use futures_async_stream::try_stream;
use tracing::{Instrument, Span};

use crate::executor::error::StreamExecutorError;
use crate::executor::{ActorContextRef, ExecutorInfo, Message, MessageStream};

/// Streams wrapped by `trace` will be traced with `tracing` spans and reported to `opentelemetry`.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn trace(
    enable_executor_row_count: bool,
    info: Arc<ExecutorInfo>,
    actor_ctx: ActorContextRef,
    input: impl MessageStream,
) {
    let actor_id_str = actor_ctx.id.to_string();
    let fragment_id_str = actor_ctx.fragment_id.to_string();

    let executor_row_count = if enable_executor_row_count {
        let count = actor_ctx
            .streaming_metrics
            .executor_row_count
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str, &info.identity]);
        Some(count)
    } else {
        None
    };

    let new_span = || {
        tracing::info_span!(
            "executor",
            "otel.name" = info.identity,
            "message" = tracing::field::Empty,    // record later
            "chunk_size" = tracing::field::Empty, // record later
        )
    };
    let mut span = new_span();

    pin_mut!(input);

    while let Some(message) = input.next().instrument(span.clone()).await.transpose()? {
        // Emit a debug event and record the message type.
        match &message {
            Message::Chunk(chunk) => {
                if let Some(count) = &executor_row_count {
                    count.inc_by(chunk.cardinality() as u64);
                }
                tracing::debug!(
                    target: "events::stream::message::chunk",
                    parent: &span,
                    cardinality = chunk.cardinality(),
                    capacity = chunk.capacity(),
                    "\n{}\n", chunk.to_pretty_with_schema(&info.schema),
                );
                span.record("message", "chunk");
                span.record("chunk_size", chunk.cardinality());
            }
            Message::Watermark(watermark) => {
                tracing::debug!(
                    target: "events::stream::message::watermark",
                    parent: &span,
                    value = ?watermark.val,
                    col_idx = watermark.col_idx,
                );
                span.record("message", "watermark");
            }
            Message::Barrier(barrier) => {
                tracing::debug!(
                    target: "events::stream::message::barrier",
                    parent: &span,
                    prev_epoch = barrier.epoch.prev,
                    curr_epoch = barrier.epoch.curr,
                    kind = ?barrier.kind,
                );
                span.record("message", "barrier");
            }
        };

        // Drop the span as the inner executor has yielded a new message.
        //
        // This is essentially similar to `.instrument(new_span())`, but it allows us to
        // emit the debug event and record the message type.
        let _ = std::mem::replace(&mut span, Span::none());

        yield message;

        // Create a new span after we're called again. The parent span may also have been
        // updated.
        span = new_span();
    }
}

/// Streams wrapped by `instrument_await_tree` will be able to print the spans of the
/// executors in the stack trace through `await-tree`.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn instrument_await_tree(info: Arc<ExecutorInfo>, input: impl MessageStream) {
    pin_mut!(input);

    let span: await_tree::Span = info.identity.clone().into();

    while let Some(message) = input
        .next()
        .instrument_await(span.clone())
        .await
        .transpose()?
    {
        yield message;
    }
}
