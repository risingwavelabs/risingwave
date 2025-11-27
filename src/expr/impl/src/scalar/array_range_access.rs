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

use risingwave_common::array::ListRef;
use risingwave_expr::function;

/// If the case is `array[1,2,3][:2]`, then start will be 0 set by the frontend
/// If the case is `array[1,2,3][1:]`, then end will be `i32::MAX` set by the frontend
#[function("array_range_access(anyarray, int4, int4) -> anyarray")]
pub fn array_range_access(
    list: ListRef<'_>,
    start: i32,
    end: i32,
    writer: &mut impl risingwave_common::array::ListWrite,
) {
    let start = std::cmp::max(start, 1) as usize;
    let end = std::cmp::min(std::cmp::max(0, end), list.len() as i32) as usize;
    if start <= end {
        writer.write_iter(list.iter().take(end).skip(start - 1));
    }
}
