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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::VirtualNode;
use risingwave_pb::hummock::table_watermarks::PbEpochNewWatermarks;
use risingwave_pb::hummock::vnodes::{AllVnodes, VnodeRange};
use risingwave_pb::hummock::{vnodes, PbTableWatermarks, PbVnodeWatermark, PbVnodes};

use crate::HummockEpoch;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
pub enum Vnodes {
    Bitmap(Arc<Bitmap>),
    Range {
        start_vnode: VirtualNode,
        end_vnode: VirtualNode,
    },
    Single(VirtualNode),
    All,
}

impl Vnodes {
    pub fn to_protobuf(&self) -> PbVnodes {
        let vnodes = match self {
            Vnodes::Bitmap(bitmap) => vnodes::PbVnodes::VnodeBitmap(bitmap.to_protobuf()),
            Vnodes::Range {
                start_vnode,
                end_vnode,
            } => vnodes::PbVnodes::Range(VnodeRange {
                vnode_start: start_vnode.to_index() as _,
                vnode_end: end_vnode.to_index() as _,
            }),
            Vnodes::Single(vnode) => vnodes::PbVnodes::Single(vnode.to_index() as _),
            Vnodes::All => vnodes::PbVnodes::All(AllVnodes {}),
        };
        PbVnodes {
            vnodes: Some(vnodes),
        }
    }

    pub fn from_protobuf(pb: &PbVnodes) -> Vnodes {
        match pb.vnodes.as_ref().expect("should not be None") {
            vnodes::Vnodes::VnodeBitmap(bitmap) => Vnodes::Bitmap(Arc::new(Bitmap::from(bitmap))),
            vnodes::Vnodes::Range(range) => Vnodes::Range {
                start_vnode: VirtualNode::from_index(range.vnode_start as _),
                end_vnode: VirtualNode::from_index(range.vnode_end as _),
            },
            vnodes::Vnodes::Single(vnode) => Vnodes::Single(VirtualNode::from_index(*vnode as _)),
            vnodes::Vnodes::All(_) => Vnodes::All,
        }
    }

    #[auto_enums::auto_enum(Iterator)]
    pub fn vnodes(&self) -> impl Iterator<Item = VirtualNode> + '_ {
        match self {
            Vnodes::Bitmap(bitmap) => bitmap.iter_ones().map(VirtualNode::from_index),
            Vnodes::Range {
                start_vnode,
                end_vnode,
            } => (start_vnode.to_index()..=end_vnode.to_index()).map(VirtualNode::from_index),
            Vnodes::Single(vnode) => once(*vnode),
            Vnodes::All => (0..VirtualNode::COUNT).map(VirtualNode::from_index),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VnodeWatermark {
    vnodes: Vnodes,
    watermark: Bytes,
}

impl VnodeWatermark {
    pub fn new(vnodes: Vnodes, watermark: Bytes) -> Self {
        Self { vnodes, watermark }
    }

    pub fn to_protobuf(&self) -> PbVnodeWatermark {
        PbVnodeWatermark {
            watermark: self.watermark.to_vec(),
            vnodes: Some(self.vnodes.to_protobuf()),
        }
    }

    pub fn from_protobuf(pb: &PbVnodeWatermark) -> Self {
        Self {
            vnodes: match pb.vnodes.as_ref().unwrap().vnodes.as_ref().unwrap() {
                vnodes::PbVnodes::VnodeBitmap(bitmap) => {
                    Vnodes::Bitmap(Arc::new(Bitmap::from(bitmap)))
                }
                vnodes::PbVnodes::Range(range) => Vnodes::Range {
                    start_vnode: VirtualNode::from_index(range.vnode_start as _),
                    end_vnode: VirtualNode::from_index(range.vnode_end as _),
                },
                vnodes::PbVnodes::Single(vnode) => {
                    Vnodes::Single(VirtualNode::from_index(*vnode as _))
                }
                vnodes::PbVnodes::All(_) => Vnodes::All,
            },
            watermark: Bytes::from(pb.watermark.clone()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableWatermarks {
    // later epoch at the back
    watermarks: Vec<(HummockEpoch, Vec<VnodeWatermark>)>,
    direction: WatermarkDirection,
}

impl TableWatermarks {
    pub fn single_epoch(
        epoch: HummockEpoch,
        watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) -> Self {
        Self {
            direction,
            watermarks: vec![(epoch, watermarks)],
        }
    }

    pub fn to_protobuf(&self) -> PbTableWatermarks {
        PbTableWatermarks {
            epoch_watermarks: self
                .watermarks
                .iter()
                .map(|(epoch, watermarks)| PbEpochNewWatermarks {
                    watermarks: watermarks.iter().map(VnodeWatermark::to_protobuf).collect(),
                    epoch: *epoch,
                })
                .collect(),
            is_ascending: match self.direction {
                WatermarkDirection::Ascending => true,
                WatermarkDirection::Descending => false,
            },
        }
    }

    pub fn add_new_epoch_watermarks(
        &mut self,
        epoch: HummockEpoch,
        watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) {
        assert_eq!(self.direction, direction);
        if let Some((prev_epoch, _)) = self.watermarks.last() {
            assert!(*prev_epoch < epoch);
        }
        self.watermarks.push((epoch, watermarks));
    }

    pub fn from_protobuf(pb: &PbTableWatermarks) -> Self {
        Self {
            watermarks: pb
                .epoch_watermarks
                .iter()
                .map(|epoch_watermark| {
                    let epoch = epoch_watermark.epoch;
                    let watermarks = epoch_watermark
                        .watermarks
                        .iter()
                        .map(VnodeWatermark::from_protobuf)
                        .collect();
                    (epoch, watermarks)
                })
                .collect(),
            direction: if pb.is_ascending {
                WatermarkDirection::Ascending
            } else {
                WatermarkDirection::Descending
            },
        }
    }
}

pub fn merge_multiple_new_table_watermarks(
    table_watermarks_list: impl IntoIterator<Item = HashMap<u64, PbTableWatermarks>>,
) -> HashMap<u64, PbTableWatermarks> {
    let mut ret: HashMap<u64, (bool, BTreeMap<u64, PbEpochNewWatermarks>)> = HashMap::new();
    for table_watermarks in table_watermarks_list {
        for (table_id, new_watermarks) in table_watermarks {
            let table_watermarks = match ret.entry(table_id) {
                Entry::Occupied(entry) => {
                    let (is_ascending, epoch_watermarks) = entry.into_mut();
                    assert_eq!(new_watermarks.is_ascending, *is_ascending);
                    epoch_watermarks
                }
                Entry::Vacant(entry) => {
                    let (_, epoch_watermarks) =
                        entry.insert((new_watermarks.is_ascending, BTreeMap::new()));
                    epoch_watermarks
                }
            };
            for new_epoch_watermarks in new_watermarks.epoch_watermarks {
                table_watermarks
                    .entry(new_epoch_watermarks.epoch)
                    .or_insert_with(|| PbEpochNewWatermarks {
                        watermarks: vec![],
                        epoch: new_epoch_watermarks.epoch,
                    })
                    .watermarks
                    .extend(new_epoch_watermarks.watermarks);
            }
        }
    }
    ret.into_iter()
        .map(|(table_id, (is_ascending, epoch_watermarks))| {
            (
                table_id,
                PbTableWatermarks {
                    is_ascending,
                    epoch_watermarks: epoch_watermarks.into_values().collect(),
                },
            )
        })
        .collect()
}

pub fn apply_new_table_watermark(
    table_watermark: &mut PbTableWatermarks,
    newly_added_watermark: &PbTableWatermarks,
) {
    assert_eq!(
        table_watermark.is_ascending,
        newly_added_watermark.is_ascending
    );
    if let Some(prev_last_epoch_watermarks) = table_watermark.epoch_watermarks.last() && let Some(new_first_epoch_watermarks) = newly_added_watermark.epoch_watermarks.first() {
        assert!(prev_last_epoch_watermarks.epoch < new_first_epoch_watermarks.epoch);
    }
    table_watermark
        .epoch_watermarks
        .extend(newly_added_watermark.epoch_watermarks.clone());
}

pub fn clear_stale_epoch_watermark(table_watermark: &mut PbTableWatermarks, safe_epoch: u64) {
    let mut result_epoch_watermark = Vec::with_capacity(table_watermark.epoch_watermarks.len());
    let mut unset_vnode: HashSet<VirtualNode> = (0..VirtualNode::COUNT)
        .map(VirtualNode::from_index)
        .collect();
    while let Some(epoch_watermark) = table_watermark.epoch_watermarks.last() {
        if epoch_watermark.epoch >= safe_epoch {
            let epoch_watermark = table_watermark
                .epoch_watermarks
                .pop()
                .expect("have check Some");
            for watermark in &epoch_watermark.watermarks {
                for vnode in
                    Vnodes::from_protobuf(watermark.vnodes.as_ref().expect("should not be None"))
                        .vnodes()
                {
                    unset_vnode.remove(&vnode);
                }
            }
            result_epoch_watermark.push(epoch_watermark);
        } else {
            break;
        }
    }
    while !unset_vnode.is_empty() && let Some(epoch_watermark) = table_watermark.epoch_watermarks.pop() {
        let mut new_vnode_watermarks = Vec::new();
        for vnode_watermark in &epoch_watermark.watermarks {
            let mut set_vnode = Vec::new();
            for vnode in Vnodes::from_protobuf(vnode_watermark.vnodes.as_ref().expect("should not be None")).vnodes() {
                if unset_vnode.remove(&vnode) {
                    set_vnode.push(vnode);
                }
            }
            if !set_vnode.is_empty() {
                let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
                for vnode in set_vnode {
                    builder.set(vnode.to_index(), true);
                }
                let bitmap = builder.finish();
                new_vnode_watermarks.push(PbVnodeWatermark {
                    vnodes: Some(PbVnodes {
                        vnodes: Some(vnodes::Vnodes::VnodeBitmap(bitmap.to_protobuf()))
                    }),
                    watermark: vnode_watermark.watermark.clone(),
                })
            }
        }
        if !new_vnode_watermarks.is_empty() {
            result_epoch_watermark.push(PbEpochNewWatermarks {
                watermarks: new_vnode_watermarks,
                // set epoch as safe epoch
                epoch: safe_epoch,
            })
        }
    }
    *table_watermark = PbTableWatermarks {
        epoch_watermarks: result_epoch_watermark,
        is_ascending: table_watermark.is_ascending,
    }
}
