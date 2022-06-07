// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// `VirtualNode` is the logical key for consistent hash. Virtual nodes stand for the intermediate
/// layer between data and physical nodes.
pub type VirtualNode = u16;
pub const VNODE_BITS: usize = 11;
pub const VIRTUAL_NODE_COUNT: usize = 1 << VNODE_BITS;
pub const VNODE_BITMAP_LEN: usize = 1 << (VNODE_BITS - 3);

// `VNodeBitmap` is a bitmap of vnodes for a state table, which indicates the vnodes that the state
// table owns.
#[derive(Clone, Default)]
pub struct VNodeBitmap {
    table_id: u32,
    bitmap: Vec<u8>,
}

impl From<risingwave_pb::common::VNodeBitmap> for VNodeBitmap {
    fn from(proto: risingwave_pb::common::VNodeBitmap) -> Self {
        Self {
            table_id: proto.table_id,
            bitmap: proto.bitmap,
        }
    }
}

impl VNodeBitmap {
    pub fn new(table_id: u32, bitmap: Vec<u8>) -> Self {
        Self { table_id, bitmap }
    }

    pub fn check_overlap(&self, bitmaps: &[risingwave_pb::common::VNodeBitmap]) -> bool {
        if bitmaps.is_empty() {
            return true;
        }
        if let Ok(pos) =
            bitmaps.binary_search_by_key(&self.table_id, |bitmap| bitmap.get_table_id())
        {
            let text = &bitmaps[pos];
            assert_eq!(self.bitmap.len(), VNODE_BITMAP_LEN);
            assert_eq!(text.bitmap.len(), VNODE_BITMAP_LEN);
            for i in 0..VNODE_BITMAP_LEN as usize {
                if (self.bitmap[i] & text.get_bitmap()[i]) != 0 {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_pb::common::VNodeBitmap as ProstBitmap;

    use super::*;

    #[test]
    fn test_check_overlap() {
        // The bitmap is for table with id 3, and owns vnodes from 64 to 128.
        let table_id = 3;
        let mut bitmap = [0; VNODE_BITMAP_LEN].to_vec();
        for byte in bitmap.iter_mut().take(16).skip(8) {
            *byte = 0b11111111;
        }
        let vnode_bitmap = VNodeBitmap::new(table_id, bitmap);

        // Test overlap.
        let bitmaps_1 = (2..4)
            .map(|table_id| {
                let mut test_bitmap = [0; VNODE_BITMAP_LEN].to_vec();
                test_bitmap[10] = 0b1;
                ProstBitmap {
                    table_id,
                    bitmap: test_bitmap,
                }
            })
            .collect_vec();
        assert!(vnode_bitmap.check_overlap(&bitmaps_1));

        // Test non-overlap with same table id.
        let bitmaps_2 = (2..4)
            .map(|table_id| {
                let mut test_bitmap = [0; VNODE_BITMAP_LEN].to_vec();
                test_bitmap[20] = 0b1;
                ProstBitmap {
                    table_id,
                    bitmap: test_bitmap,
                }
            })
            .collect_vec();
        assert!(!vnode_bitmap.check_overlap(&bitmaps_2));

        // Test non-overlap with different table ids and same vnodes.
        let bitmaps_3 = (4..6)
            .map(|table_id| {
                let mut test_bitmap = [0; VNODE_BITMAP_LEN].to_vec();
                for byte in test_bitmap.iter_mut().take(16).skip(8) {
                    *byte = 0b11111111;
                }
                ProstBitmap {
                    table_id,
                    bitmap: test_bitmap,
                }
            })
            .collect_vec();
        assert!(!vnode_bitmap.check_overlap(&bitmaps_3));

        // Test empty
        assert!(vnode_bitmap.check_overlap(&[]));
    }
}
