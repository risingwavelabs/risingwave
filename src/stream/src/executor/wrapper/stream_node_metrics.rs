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

use std::sync::atomic::Ordering;

use futures_async_stream::try_stream;
use tokio::time::Instant;

use crate::executor::prelude::*;

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn stream_node_metrics(
    operator_id: u64,
    input: impl MessageStream,
    actor_ctx: ActorContextRef,
) {
    let stats = actor_ctx.streaming_metrics.new_profile_metrics(operator_id);

    let blocking_duration = Instant::now();

    #[for_await]
    for message in input {
        stats.stream_node_output_blocking_duration_ns.fetch_add(
            blocking_duration.elapsed().as_nanos() as u32,
            Ordering::Relaxed,
        );
        let message = message?;
        if let Message::Chunk(ref c) = message {
            stats
                .stream_node_output_row_count
                .fetch_add(c.cardinality() as u32, Ordering::Relaxed);
        }
        yield message;
    }
}
