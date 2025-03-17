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

use async_trait::async_trait;

mod cache;
mod sides;
use self::cache::LookupCache;
use self::sides::*;
mod impl_;

pub use impl_::LookupExecutorParams;

use crate::executor::prelude::*;

#[cfg(test)]
mod tests;

/// `LookupExecutor` takes one input stream and one arrangement. It joins the input stream with the
/// arrangement. Currently, it only supports inner join. See `LookupExecutorParams` for more
/// information.
///
/// The output schema is `| stream columns | arrangement columns |`.
/// The input is required to be first stream and then arrangement.
pub struct LookupExecutor<S: StateStore> {
    ctx: ActorContextRef,

    /// the data types of the produced data chunk inside lookup (before reordering)
    chunk_data_types: Vec<DataType>,

    /// The join side of the arrangement
    arrangement: ArrangeJoinSide<S>,

    /// The join side of the stream
    stream: StreamJoinSide,

    /// The executor for arrangement.
    arrangement_executor: Option<Executor>,

    /// The executor for stream.
    stream_executor: Option<Executor>,

    /// The last received barrier.
    last_barrier: Option<Barrier>,

    /// Information of column reordering
    column_mapping: Vec<usize>,

    /// When we receive a row from the stream side, we will first convert it to join key, and then
    /// map it to arrange side. For example, if we receive `[a, b, c]` from the stream side, where:
    /// `stream_join_key = [1, 2]`, `arrange_join_key = [3, 2]` with order rules [2 ascending, 3
    /// ascending].
    ///
    /// * We will first extract join key `[b, c]`,
    /// * then map it to the order of arrangement join key `[c, b]`.
    ///
    /// This vector records such mapping.
    key_indices_mapping: Vec<usize>,

    /// The cache for arrangement side.
    lookup_cache: LookupCache,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

#[async_trait]
impl<S: StateStore> Execute for LookupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
