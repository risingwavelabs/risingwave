// Copyright 2026 RisingWave Labs
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

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;

use crate::key::{FullKey, TableKey};

pub fn build_vnode_partition_boundary_keys(
    table_id: TableId,
    vnode_count: usize,
    partition_count: usize,
) -> Vec<Bytes> {
    debug_assert!(partition_count > 0);
    debug_assert!(partition_count <= vnode_count);

    if partition_count <= 1 {
        return vec![];
    }

    // Keep the same partitioning rule with `MultiBuilder`:
    // - First `partition_count - remainder` partitions have `basic` vnodes.
    // - Last `remainder` partitions have `basic + 1` vnodes.
    let basic = vnode_count / partition_count;
    let remainder = vnode_count % partition_count;
    let small_partitions = partition_count - remainder;

    let mut boundary_keys = Vec::with_capacity(partition_count.saturating_sub(1));
    let mut start = 0usize;
    for idx in 0..partition_count {
        let size = if idx < small_partitions {
            basic
        } else {
            basic + 1
        };
        if idx > 0 {
            boundary_keys.push(vnode_boundary_full_key(table_id, start));
        }
        start += size;
    }
    debug_assert_eq!(start, vnode_count);
    boundary_keys
}

pub fn vnode_boundary_full_key(table_id: TableId, vnode: usize) -> Bytes {
    FullKey::new(
        table_id,
        TableKey(VirtualNode::from_index(vnode).to_be_bytes().to_vec()),
        u64::MAX,
    )
    .encode()
    .into()
}
