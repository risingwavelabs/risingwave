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

use std::sync::Arc;

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use tracing::event;
use tracing_futures::Instrument;

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ExecutorInfo, Message, MessageStream};
use crate::task::ActorId;

/// Streams wrapped by `trace` will print data passing in the stream graph to stdout.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn trace(
    info: Arc<ExecutorInfo>,
    input_pos: usize,
    actor_id: ActorId,
    metrics: Arc<StreamingMetrics>,
    input: impl MessageStream,
) {
    let span_name = format!("{}_{}_next", info.identity, input_pos);
    let actor_id_string = actor_id.to_string();

    let span = || {
        tracing::trace_span!(
            "next",
            otel.name = span_name.as_str(),
            next = info.identity.as_str(), // For the upstream trace pipe, its output is our input.
            input_pos = input_pos,
        )
    };

    pin_mut!(input);

    while let Some(message) = input.next().instrument(span()).await.transpose()? {
        if let Message::Chunk(chunk) = &message {
            if chunk.cardinality() > 0 {
                metrics
                    .actor_row_count
                    .with_label_values(&[&actor_id_string])
                    .inc_by(chunk.cardinality() as u64);
                event!(tracing::Level::TRACE, prev = %info.identity, msg = "chunk", "input = \n{:#?}", chunk);
            }
        }

        yield message;
    }
}

/// Streams wrapped by `metrics` will update actor metrics.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn metrics(actor_id: ActorId, metrics: Arc<StreamingMetrics>, input: impl MessageStream) {
    let actor_id_string = actor_id.to_string();

    pin_mut!(input);

    while let Some(message) = input.next().await.transpose()? {
        if let Message::Chunk(chunk) = &message {
            if chunk.cardinality() > 0 {
                metrics
                    .actor_row_count
                    .with_label_values(&[&actor_id_string])
                    .inc_by(chunk.cardinality() as u64);
            }
        }

        yield message;
    }
}
