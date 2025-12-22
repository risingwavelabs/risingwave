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

use risingwave_common::config::meta::default::compaction_config;

/// Determines whether the key count is large enough to warrant using a block-based filter.
///
/// # Arguments
/// * `kv_count` - The total number of keys
/// * `max_kv_count` - Optional configured threshold. If None, uses `DEFAULT_MAX_KV_COUNT_FOR_XOR16`
///
/// # Returns
/// `true` if `kv_count` exceeds the threshold, indicating block-based filter should be used
pub fn is_kv_count_too_large_for_xor16(kv_count: u64, max_kv_count: Option<u64>) -> bool {
    let threshold = max_kv_count.unwrap_or(compaction_config::DEFAULT_MAX_KV_COUNT_FOR_XOR16);
    kv_count > threshold
}
