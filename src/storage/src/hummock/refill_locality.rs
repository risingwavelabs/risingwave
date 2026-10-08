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

use std::collections::HashMap;
use std::ops::Bound;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};
use risingwave_pb::id::TableId;

use super::Sstable;

/// Borrows the single runtime ownership source in `CacheRefiller`, projected by worker role.
/// Keep both lanes: Foyer's policies distinguish them, while Pin uses their union.
#[derive(Clone, Copy, Default)]
pub(crate) struct RefillOwnership<'a> {
    pub streaming: Option<&'a HashMap<TableId, Bitmap>>,
    pub serving: Option<&'a HashMap<TableId, Bitmap>>,
}

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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{UserKey, prefix_slice_with_vnode};
    use risingwave_pb::id::TableId;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_table_ids,
    };
    use crate::hummock::value::HummockValue;

    #[tokio::test]
    async fn test_block_vnode_range_handles_vnode_only_block_boundaries() {
        let table_id = TableId::from(233);
        let vnode = VirtualNode::ZERO;
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [234, 233].into_iter().map(|epoch| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table_id, prefix_slice_with_vnode(vnode, b"")),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(epoch)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store.clone(),
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        let expected = (vnode.to_index(), vnode.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), expected);
        assert_eq!(block_vnode_range(&sst, 1), expected);
    }

    #[tokio::test]
    async fn test_block_vnode_range_fails_open_for_shortened_meta_keys() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        builder_options.shorten_block_meta_key_threshold = Some(0);
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [255, 256].into_iter().map(|vnode| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_id,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"long-key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store,
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        assert!(
            FullKey::decode(&sst.meta.block_metas[1].smallest_key)
                .user_key
                .table_key
                .as_ref()
                .len()
                < VirtualNode::SIZE
        );
        let full_range = (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), full_range);
        assert_eq!(block_vnode_range(&sst, 1), full_range);
    }

    #[test]
    fn test_vnode_range_overlaps_bitmap_uses_right_exclusive_end() {
        let right_exclusive = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [12]);
        assert!(!vnode_range_overlaps_bitmap((10, 12), &right_exclusive));

        let inside_range = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [11]);
        assert!(vnode_range_overlaps_bitmap((10, 12), &inside_range));

        let last_vnode = Bitmap::from_indices(
            VirtualNode::COUNT_FOR_TEST,
            [VirtualNode::COUNT_FOR_TEST - 1],
        );
        assert!(vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST - 1, VirtualNode::COUNT_FOR_TEST),
            &last_vnode
        ));
        assert!(!vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST, VirtualNode::COUNT_FOR_TEST + 1),
            &last_vnode
        ));
    }
}
