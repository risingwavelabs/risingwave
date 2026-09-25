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

use std::ops::Bound;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};

use super::Sstable;

pub(crate) fn vnode_range_overlaps_bitmap(vnode_range: (usize, usize), bitmap: &Bitmap) -> bool {
    assert!(vnode_range.0 <= vnode_range.1);
    let start = vnode_range.0.min(bitmap.len());
    let end = vnode_range.1.min(bitmap.len());
    if start == end || !bitmap.any() {
        return false;
    }
    if bitmap.all() {
        return true;
    }
    (start..end).any(|vnode| bitmap.is_set(vnode))
}

pub(crate) fn block_vnode_range(sstable: &Sstable, block_index: usize) -> (usize, usize) {
    let block_meta = &sstable.meta.block_metas[block_index];
    let block_smallest_key = FullKey::decode(&block_meta.smallest_key);
    let table_key_end = match sstable.meta.block_metas.get(block_index + 1) {
        // A table switch always starts a new block. The next table's smallest key has an
        // unrelated vnode, so use the current table's terminal range instead.
        Some(next_block_meta) if next_block_meta.table_id() != block_meta.table_id() => {
            Bound::Unbounded
        }
        // Full-key versions of the same table key may span adjacent blocks. After projecting
        // away the epoch, the boundary vnode therefore remains part of the current block.
        Some(next_block_meta) => Bound::Included(
            FullKey::decode(&next_block_meta.smallest_key)
                .user_key
                .table_key,
        ),
        // `SstableMeta::largest_key` is the actual last key, unlike the next block's smallest
        // key above. Keep it inclusive, especially for singleton tables whose key contains only
        // the vnode prefix.
        None => Bound::Included(
            FullKey::decode(&sstable.meta.largest_key)
                .user_key
                .table_key,
        ),
    };

    let table_key_range = (
        Bound::Included(block_smallest_key.user_key.table_key),
        table_key_end,
    );
    // Block-meta separators may shorten the table key below the vnode prefix. They are valid
    // full-key search boundaries but cannot identify a vnode, so fail open instead of panicking
    // or dropping a block that may belong to this worker.
    if match &table_key_range.0 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } || match &table_key_range.1 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } {
        return (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
    }
    vnode_range(&table_key_range)
}
