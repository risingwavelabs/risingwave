// Copyright 2024 RisingWave Labs
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

use futures_async_stream::stream;
use risingwave_common::array::StreamChunk;
use risingwave_common_rate_limit::{RateLimit, RateLimiter};

/// Get the rate-limited max chunk size.
pub(crate) fn limited_chunk_size(rate_limit_burst: Option<u32>) -> usize {
    let config_chunk_size = crate::config::chunk_size();
    rate_limit_burst
        .map(|burst| config_chunk_size.min(burst as usize))
        .unwrap_or(config_chunk_size)
}

/// Yield `chunk` under the limiter's current policy, split into pieces no larger than the rate
/// so that a policy update takes effect within about a second.
#[stream(item = StreamChunk)]
pub(crate) async fn rate_limited_pieces(limiter: &RateLimiter, chunk: StreamChunk) {
    if chunk.capacity() == 0 {
        yield chunk;
        return;
    }
    let rate_limit = loop {
        match limiter.rate_limit() {
            RateLimit::Pause => limiter.wait(0).await,
            limit => break limit,
        }
    };
    match rate_limit {
        RateLimit::Pause => unreachable!(),
        RateLimit::Disabled => yield chunk,
        RateLimit::Fixed(limit) => {
            let max_permits = limit.get();
            if chunk.rate_limit_permits() <= max_permits {
                limiter.wait(chunk.rate_limit_permits()).await;
                yield chunk;
            } else {
                for piece in chunk.split(max_permits as _) {
                    limiter.wait_chunk(&piece).await;
                    yield piece;
                }
            }
        }
    }
}
