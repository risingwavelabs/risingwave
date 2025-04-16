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

use futures_async_stream::try_stream;
use tokio::time::Instant;

use crate::executor::monitor::profiling_stats::{ProfileMetricsExt, ProfileMetricsImpl};
use crate::executor::prelude::*;

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn stream_node_metrics(
    info: Arc<ExecutorInfo>,
    enable_explain_analyze_stats: bool,
    input: impl MessageStream,
    actor_ctx: ActorContextRef,
) {
    let stats = ProfileMetricsImpl::new(
        info.id,
        &actor_ctx.streaming_metrics,
        enable_explain_analyze_stats,
    );

    #[for_await]
    for message in input {
        let message = message?;
        if let Message::Chunk(ref c) = message {
            stats.inc_row_count(c.cardinality() as u64);
        }
        let blocking_duration = Instant::now();
        yield message;
        stats.inc_blocking_duration_ns(blocking_duration.elapsed().as_nanos() as u64);
    }
}
