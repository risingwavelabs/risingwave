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

use crate::executor::prelude::*;

#[derive(Default)]
pub struct DummyExecutor;

impl Execute for DummyExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        futures::stream::pending().boxed()
    }
}

pub fn compute_rate_limit_chunk_permits(chunk: &StreamChunk, burst: usize) -> usize {
    let chunk_size = chunk.capacity();
    let ends_with_update = if chunk_size >= 2 {
        // Note we have to check if the 2nd last is `U-` to be consistenct with `StreamChunkBuilder`.
        // If something inconsistent happens in the stream, we may not have `U+` after this `U-`.
        chunk.ops()[chunk_size - 2].is_update_delete()
    } else {
        false
    };
    if chunk_size == burst + 1 && ends_with_update {
        // If the chunk size exceed limit because of the last `Update` operation,
        // we should minus 1 to make sure the permits consumed is within the limit (max burst).
        chunk_size - 1
    } else {
        chunk_size
    }
}
