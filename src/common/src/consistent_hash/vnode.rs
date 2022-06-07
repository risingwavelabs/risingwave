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
