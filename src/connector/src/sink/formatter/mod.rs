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

use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::Row;

use crate::sink::Result;

mod append_only;
mod upsert;

pub use append_only::AppendOnlyFormatter;
pub use upsert::UpsertFormatter;

/// Transforms a `StreamChunk` into a sequence of key-value pairs according a specific format,
/// for example append-only, upsert or debezium.
///
/// Each row in the `StreamChunk` may emit 0, 1, or more output pairs, which would be concatenated
/// (flattened) by [`format_chunk`]. This auxiliary function is not included as a provided method
/// due to some typing issue.
///
/// Generator syntax, once available, may make this simpler. Note that we only need an iterator,
/// and async generator would be an overkill.
pub trait SinkFormatter {
    type K;
    type V;
    type I: IntoIterator<Item = (Option<Self::K>, Option<Self::V>)>;

    fn format_row(&mut self, op: Op, row: impl Row + Copy) -> Result<Self::I>;
}

pub fn format_chunk<'a, F: SinkFormatter + 'a>(
    mut formatter: F,
    chunk: &'a StreamChunk,
) -> impl Iterator<Item = Result<<F::I as IntoIterator>::Item>> + 'a {
    chunk
        .rows()
        .map(move |(op, row)| formatter.format_row(op, row))
        .flatten_ok()
}
