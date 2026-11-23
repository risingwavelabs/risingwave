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

mod buffer_table_cache;
mod eowc_gap_fill;
mod sort;
mod sort_buffer;

pub use buffer_table_cache::BufferTableCache;
pub use eowc_gap_fill::{EowcGapFillExecutor, EowcGapFillExecutorArgs};
use risingwave_common::array::StreamChunk;
pub use sort::{SortExecutor, SortExecutorArgs};
pub use sort_buffer::SortBuffer;

use crate::executor::StreamExecutorResult;
use crate::executor::filter::FilterExecutorInner;

/// Skips the rows that are NULL in the watermark column `col_idx` of an EOWC executor. A watermark
/// only bounds non-NULL values, so no watermark would ever close such a row.
pub fn skip_null_rows(
    chunk: StreamChunk,
    col_idx: usize,
) -> StreamExecutorResult<Option<StreamChunk>> {
    if chunk.column_at(col_idx).null_bitmap().all() {
        return Ok(Some(chunk));
    }
    FilterExecutorInner::<false>::filter_not_null(chunk, col_idx)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;

    use super::*;

    #[test]
    fn test_skip_null_rows() {
        let chunk = StreamChunk::from_pretty(
            "  I I
             +  1 .
             +  2 2
             U- 3 .
             U+ 3 3
             U- 4 4
             U+ 4 .
             U- 5 .
             U+ 5 .
             U- 6 6
             U+ 6 7
             -  8 .
             +  9 . D",
        );
        let expected = StreamChunk::from_pretty(
            "  I I
             +  2 2
             +  3 3
             -  4 4
             U- 6 6
             U+ 6 7",
        );
        let output = skip_null_rows(chunk, 1).unwrap().unwrap();
        assert_eq!(output.compact_vis(), expected);

        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 .
             - 2 .",
        );
        assert!(skip_null_rows(chunk, 1).unwrap().is_none());
    }
}
