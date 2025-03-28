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

/// Generate a globally unique operator id.
pub fn unique_operator_id(fragment_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((fragment_id as u64) << 32) + operator_id
}

/// Generate a globally unique executor id.
pub fn unique_executor_id(actor_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((actor_id as u64) << 32) + operator_id
}

pub fn unique_executor_id_from_unique_operator_id(actor_id: u32, unique_operator_id: u64) -> u64 {
    // keep only low-32 bits
    let unique_operator_id = unique_operator_id & 0xFFFFFFFF;
    ((actor_id as u64) << 32) + unique_operator_id
}
